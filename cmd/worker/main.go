package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	kafkalib "github.com/log-analytics-platform/internal/kafka"
	"github.com/log-analytics-platform/internal/models"
	"github.com/log-analytics-platform/internal/tinybird"
)

var (
	eventsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_events_processed_total",
		Help: "Total events processed",
	})

	eventsDeadLettered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_events_dead_lettered_total",
		Help: "Total events sent to dead letter queue",
	})

	processingLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_processing_latency_seconds",
		Help:    "Event processing latency",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
	})

	batchSizeMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_batch_size",
		Help:    "Batch size for Tinybird appends",
		Buckets: []float64{1, 10, 50, 100, 500, 1000, 5000},
	})

	flushFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_flush_failures_total",
		Help: "Total Tinybird flush failures",
	})

	offsetsCommitted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_offsets_committed_total",
		Help: "Total offset commits (only after durable write)",
	})

	appendLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_append_latency_seconds",
		Help:    "Tinybird append round-trip latency per batch",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	})
)

func init() {
	prometheus.MustRegister(eventsProcessed, eventsDeadLettered, processingLatency,
		batchSizeMetric, flushFailures, offsetsCommitted, appendLatency)
}

// bufferedItem pairs a normalized event with the Kafka message it came from,
// so we can commit the correct offsets after a durable write.
// appendBatchSize is the number of buffered events per Tinybird append.
// Large batches amortize the synchronous Events API round-trip (wait=true),
// which is the dominant cost in the worker loop.
const appendBatchSize = 5000

// secretPatterns is compiled once: redactSecrets runs on every event, so
// compiling these per message would dominate worker CPU at high throughput.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(password|passwd|secret|token|api_key|apikey|authorization)\s*[:=]\s*\S+`),
	regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`),
	regexp.MustCompile(`\b(?:\d{4}[- ]?){3}\d{4}\b`),
}

type bufferedItem struct {
	event models.LogEvent
	msg   *kafka.Message // needed to commit offset after flush
}

// Processor handles event normalization, enrichment, and redaction.
type Processor struct {
	cfg      *config.Config
	tinybird *tinybird.Client
	producer *kafkalib.Producer
	logger   *zap.Logger

	mu     sync.Mutex
	buffer []bufferedItem
}

func NewProcessor(cfg *config.Config, logger *zap.Logger) (*Processor, error) {
	if cfg.Tinybird.AppendToken == "" {
		return nil, fmt.Errorf("TINYBIRD_APPEND_TOKEN must be configured")
	}
	tb, err := tinybird.NewClient(cfg.Tinybird, logger)
	if err != nil {
		return nil, fmt.Errorf("tinybird client: %w", err)
	}

	producer, err := kafkalib.NewProducer(cfg.Kafka, logger)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	return &Processor{
		cfg:      cfg,
		tinybird: tb,
		producer: producer,
		logger:   logger,
		buffer:   make([]bufferedItem, 0, appendBatchSize),
	}, nil
}

// normalize processes, enriches, and redacts an event.
func (p *Processor) normalize(raw []byte) (*models.LogEvent, error) {
	var event models.LogEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	event.Severity = normalizeSeverity(event.Severity)
	event.Message = redactSecrets(event.Message)
	if event.Attributes != nil {
		redactAttributes(event.Attributes)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	return &event, nil
}

// addToBuffer normalizes the message and adds it to the buffer.
// Returns true if the buffer has reached the flush threshold.
// Does NOT commit any offsets — that happens only after a durable write.
func (p *Processor) addToBuffer(msg *kafka.Message) (shouldFlush bool) {
	start := time.Now()
	defer func() {
		processingLatency.Observe(time.Since(start).Seconds())
	}()

	event, err := p.normalize(msg.Value)
	if err != nil {
		p.sendToDLQ(msg.Value, err)
		eventsDeadLettered.Inc()
		// DLQ'd messages still need their offset committed so we don't
		// re-process them forever. The caller commits after flush, and
		// we include the msg in the buffer (with no event) so its
		// offset gets tracked. We use a sentinel empty event.
		p.mu.Lock()
		p.buffer = append(p.buffer, bufferedItem{msg: msg})
		shouldFlush = len(p.buffer) >= appendBatchSize
		p.mu.Unlock()
		return shouldFlush
	}

	p.mu.Lock()
	p.buffer = append(p.buffer, bufferedItem{event: *event, msg: msg})
	shouldFlush = len(p.buffer) >= appendBatchSize
	p.mu.Unlock()

	eventsProcessed.Inc()
	return shouldFlush
}

// Flush writes buffered events to Tinybird and returns the messages
// whose offsets should be committed. If the Tinybird write fails,
// the buffer is restored and NO offsets are returned — so the consumer
// will re-deliver these messages after rebalance.
func (p *Processor) Flush() ([]*kafka.Message, error) {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return nil, nil
	}

	items := p.buffer
	p.buffer = make([]bufferedItem, 0, appendBatchSize)
	p.mu.Unlock()

	// Separate real events from DLQ/dup-only messages
	var events []models.LogEvent
	msgs := make([]*kafka.Message, 0, len(items))
	for _, item := range items {
		msgs = append(msgs, item.msg)
		if item.event.EventID != "" {
			events = append(events, item.event)
		}
	}

	batchSizeMetric.Observe(float64(len(events)))

	ctx := context.Background()

	// Tinybird acknowledges the batch before its Kafka offsets are committed.
	if len(events) > 0 {
		appendStart := time.Now()
		appendErr := p.tinybird.AppendEvents(ctx, events)
		appendLatency.Observe(time.Since(appendStart).Seconds())
		if appendErr != nil {
			p.logger.Error("batch insert failed, restoring buffer — offsets NOT committed",
				zap.Error(appendErr), zap.Int("batch_size", len(events)))
			flushFailures.Inc()

			// Restore buffer so events aren't lost
			p.mu.Lock()
			p.buffer = append(items, p.buffer...)
			p.mu.Unlock()

			return nil, fmt.Errorf("tinybird append: %w", err)
		}
	}

	p.logger.Debug("batch flushed to Tinybird",
		zap.Int("events", len(events)),
		zap.Int("msgs", len(msgs)))

	// Return the messages — the caller will commit their offsets.
	return msgs, nil
}

func (p *Processor) sendToDLQ(raw []byte, originalErr error) {
	dlqEvent := models.DeadLetterEvent{
		Raw:       string(raw),
		Error:     originalErr.Error(),
		FailedAt:  time.Now(),
		Component: "processing-worker",
	}

	data, err := json.Marshal(dlqEvent)
	if err != nil {
		p.logger.Error("DLQ marshal failed", zap.Error(err))
		return
	}

	if err := p.producer.Produce(p.cfg.Kafka.TopicDLQ, "dlq", data); err != nil {
		p.logger.Error("DLQ produce failed", zap.Error(err))
	}
}

func (p *Processor) Close() {
	// Final flush (best-effort)
	if msgs, err := p.Flush(); err == nil {
		_ = msgs
	}
	p.producer.Close()
}

// --- Consumer loop with batch-aware offset management ---

// commitOffsets commits the highest offset per partition from a set of messages.
func commitOffsets(consumer *kafkalib.Consumer, msgs []*kafka.Message, logger *zap.Logger) {
	if len(msgs) == 0 {
		return
	}

	// Find the highest offset per topic-partition
	highest := make(map[string]kafka.TopicPartition)
	for _, msg := range msgs {
		key := fmt.Sprintf("%s-%d", *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
		existing, ok := highest[key]
		if !ok || msg.TopicPartition.Offset > existing.Offset {
			highest[key] = msg.TopicPartition
		}
	}

	// Commit the highest offset per partition. Kafka expects the NEXT offset.
	offsets := make([]kafka.TopicPartition, 0, len(highest))
	for _, tp := range highest {
		tp.Offset = tp.Offset + 1
		offsets = append(offsets, tp)
	}

	if _, err := consumer.CommitOffsets(offsets); err != nil {
		logger.Error("offset commit failed", zap.Error(err))
	} else {
		offsetsCommitted.Inc()
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	proc, err := NewProcessor(cfg, logger)
	if err != nil {
		logger.Fatal("processor init failed", zap.Error(err))
	}
	defer proc.Close()

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9091", mux)
	}()

	// Create consumer
	consumer, err := kafkalib.NewConsumer(cfg.Kafka, cfg.Kafka.GroupWorkers,
		[]string{cfg.Kafka.TopicLogs}, logger)
	if err != nil {
		logger.Fatal("consumer init failed", zap.Error(err))
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down worker")
		cancel()
	}()

	logger.Info("processing worker started",
		zap.String("group", cfg.Kafka.GroupWorkers),
		zap.String("topic", cfg.Kafka.TopicLogs))

	// Custom consumer loop: offsets committed ONLY after Tinybird acknowledges the write.
	flushTimer := time.NewTimer(2 * time.Second)
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Shutdown: final flush + commit
			if msgs, err := proc.Flush(); err == nil {
				commitOffsets(consumer, msgs, logger)
			}
			return

		case <-flushTimer.C:
			// Time-based flush
			msgs, err := proc.Flush()
			if err != nil {
				logger.Error("timed flush failed", zap.Error(err))
			} else if len(msgs) > 0 {
				commitOffsets(consumer, msgs, logger)
			}
			flushTimer.Reset(2 * time.Second)

		default:
			msg, err := consumer.Poll(100)
			if err != nil {
				logger.Error("poll error", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			if msg == nil {
				continue
			}

			shouldFlush := proc.addToBuffer(msg)

			if shouldFlush {
				// Size-based flush
				msgs, err := proc.Flush()
				if err != nil {
					logger.Error("size flush failed, message will be re-delivered", zap.Error(err))
					// Don't commit — message stays uncommitted and will be
					// re-delivered after rebalance.
				} else if len(msgs) > 0 {
					commitOffsets(consumer, msgs, logger)
				}
				flushTimer.Reset(2 * time.Second)
			}
		}
	}
}

// normalizeSeverity maps severity strings to standard levels.
func normalizeSeverity(sev string) string {
	upper := strings.ToUpper(strings.TrimSpace(sev))
	switch {
	case upper == "FATAL" || upper == "CRITICAL" || upper == "EMERGENCY" || upper == "EMERG":
		return "FATAL"
	case upper == "ERROR" || upper == "ERR":
		return "ERROR"
	case upper == "WARNING" || upper == "WARN":
		return "WARNING"
	case upper == "INFO" || upper == "INFORMATION":
		return "INFO"
	case upper == "DEBUG" || upper == "TRACE":
		return "DEBUG"
	default:
		if upper == "" {
			return "INFO"
		}
		return upper
	}
}

func redactSecrets(msg string) string {
	for _, pattern := range secretPatterns {
		msg = pattern.ReplaceAllString(msg, "[REDACTED]")
	}
	return msg
}

func redactAttributes(attrs map[string]interface{}) {
	sensitiveKeys := []string{"password", "secret", "token", "api_key", "apikey", "authorization", "ssn"}
	for key := range attrs {
		lower := strings.ToLower(key)
		for _, sensitive := range sensitiveKeys {
			if strings.Contains(lower, sensitive) {
				attrs[key] = "[REDACTED]"
				break
			}
		}
	}
}
