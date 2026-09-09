package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
)

// Producer wraps the Kafka producer with convenience methods.
type Producer struct {
	producer *kafka.Producer
	logger   *zap.Logger
}

// NewProducer creates a new Kafka producer.
func NewProducer(cfg config.KafkaConfig, logger *zap.Logger) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Brokers,
		"acks":               "all",
		"retries":            3,
		"retry.backoff.ms":   100,
		"linger.ms":          5,
		"batch.num.messages": 1000,
		"compression.type":   "snappy",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	// Start delivery report handler
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logger.Error("delivery failed",
						zap.Error(ev.TopicPartition.Error),
						zap.String("topic", *ev.TopicPartition.Topic))
				}
			}
		}
	}()

	return &Producer{producer: p, logger: logger}, nil
}

// Produce sends a message to the given topic with the specified key.
func (p *Producer) Produce(topic string, key string, value []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: value,
	}
	return p.producer.Produce(msg, nil)
}

// ProduceSync sends a message and waits for the delivery report.
func (p *Producer) ProduceSync(topic string, key string, value []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:       []byte(key),
		Value:     value,
		Timestamp: time.Now(),
	}

	deliveryChan := make(chan kafka.Event, 1)
	if err := p.producer.Produce(msg, deliveryChan); err != nil {
		return fmt.Errorf("produce failed: %w", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}
	return nil
}

// ProduceJSON serializes the value as JSON and produces it.
func (p *Producer) ProduceJSON(topic string, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}
	return p.Produce(topic, key, data)
}

// BatchMessage is one message in a tracked batch.
type BatchMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

// BatchResult reports per-message delivery outcomes.
type BatchResult struct {
	Accepted int
	Failed   int
	Errors   []error
}

// ProduceBatch enqueues all messages with per-message delivery channels,
// then blocks until every delivery report is received (or the timeout
// expires). The returned BatchResult reflects only messages that Kafka
// actually acknowledged, so callers can safely tell the client which
// events are durably stored.
func (p *Producer) ProduceBatch(messages []BatchMessage, timeout time.Duration) (*BatchResult, error) {
	if len(messages) == 0 {
		return &BatchResult{}, nil
	}

	deliveryChan := make(chan kafka.Event, len(messages))
	deadline := time.Now().Add(timeout)
	var enqueueErrors []error

	for _, m := range messages {
		topic := m.Topic // capture for pointer
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:       m.Key,
			Value:     m.Value,
			Timestamp: time.Now(),
		}
		if err := p.producer.Produce(msg, deliveryChan); err != nil {
			enqueueErrors = append(enqueueErrors, err)
			// Still wait for the remaining delivery reports; count this one
			// as already failed by injecting a synthetic error event below.
			deliveryChan <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Error: err},
			}
		}
	}

	result := &BatchResult{Errors: enqueueErrors}
	received := 0
	for received < len(messages) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// Timeout: any message without a report is treated as failed.
			result.Failed += len(messages) - received
			return result, fmt.Errorf("batch delivery timeout: %d/%d confirmed",
				received, len(messages))
		}

		select {
		case ev := <-deliveryChan:
			received++
			m, ok := ev.(*kafka.Message)
			if !ok {
				result.Failed++
				continue
			}
			if m.TopicPartition.Error != nil {
				result.Failed++
				result.Errors = append(result.Errors, m.TopicPartition.Error)
			} else {
				result.Accepted++
			}
		case <-time.After(remaining):
			result.Failed += len(messages) - received
			return result, fmt.Errorf("batch delivery timeout: %d/%d confirmed",
				received, len(messages))
		}
	}

	return result, nil
}

// Flush waits for all messages to be delivered.
func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

// Close flushes and closes the producer.
func (p *Producer) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
}

// Consumer wraps the Kafka consumer with convenience methods.
type Consumer struct {
	consumer *kafka.Consumer
	logger   *zap.Logger
}

// NewConsumer creates a new Kafka consumer in the given group.
func NewConsumer(cfg config.KafkaConfig, groupID string, topics []string, logger *zap.Logger) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     cfg.Brokers,
		"group.id":              groupID,
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    false,
		"session.timeout.ms":    6000,
		"heartbeat.interval.ms": 2000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, fmt.Errorf("subscribe failed: %w", err)
	}

	return &Consumer{consumer: c, logger: logger}, nil
}

// Poll reads a single message with the given timeout.
func (c *Consumer) Poll(timeoutMs int) (*kafka.Message, error) {
	ev := c.consumer.Poll(timeoutMs)
	if ev == nil {
		return nil, nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		return e, nil
	case kafka.Error:
		if e.Code() == kafka.ErrAllBrokersDown {
			return nil, fmt.Errorf("all brokers down")
		}
		c.logger.Warn("kafka consumer error", zap.Error(e))
		return nil, nil
	case kafka.PartitionEOF:
		return nil, nil
	default:
		return nil, nil
	}
}

// Commit commits the current offset.
func (c *Consumer) Commit() ([]kafka.TopicPartition, error) {
	return c.consumer.Commit()
}

// CommitMessage commits the offset for a specific message.
func (c *Consumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return c.consumer.CommitMessage(msg)
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	return c.consumer.Close()
}

// PollLoop runs a consumer loop with context cancellation. A handler error
// stops the loop without committing the failed offset. Continuing to poll and
// then committing a later offset could skip the failed event in that partition.
func (c *Consumer) PollLoop(ctx context.Context, handler func(msg *kafka.Message) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.Poll(1000)
			if err != nil {
				c.logger.Error("poll error", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			if msg == nil {
				continue
			}

			if err := handler(msg); err != nil {
				return fmt.Errorf("handler failed at %s[%d] offset %d: %w",
					*msg.TopicPartition.Topic, msg.TopicPartition.Partition,
					msg.TopicPartition.Offset, err)
			}

			// Handler succeeded — now it is safe to commit.
			if _, err := c.CommitMessage(msg); err != nil {
				c.logger.Error("commit error", zap.Error(err))
			}
		}
	}
}

// CommitOffsets commits explicitly supplied next offsets. Batch consumers use
// this to acknowledge only messages already written to durable storage.
func (c *Consumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return c.consumer.CommitOffsets(offsets)
}

// CreateTopics creates the required topics if they don't exist.
func CreateTopics(brokers string, topics []string, partitions int, replicationFactor int) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return fmt.Errorf("admin client error: %w", err)
	}
	defer adminClient.Close()

	var topicSpecs []kafka.TopicSpecification
	for _, topic := range topics {
		topicSpecs = append(topicSpecs, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, topicSpecs)
	if err != nil {
		return fmt.Errorf("create topics error: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("topic %s: %s", result.Topic, result.Error)
		}
	}

	return nil
}
