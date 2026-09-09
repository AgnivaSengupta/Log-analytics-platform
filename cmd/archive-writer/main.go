package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	kafkalib "github.com/log-analytics-platform/internal/kafka"
	"github.com/log-analytics-platform/internal/storage"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	archivedEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "archive_events_total",
		Help: "Total events archived to S3",
	})

	archiveLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "archive_batch_latency_seconds",
		Help:    "Archive batch write latency",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
	})
)

func init() {
	prometheus.MustRegister(archivedEvents, archiveLatency)
}

// ArchiveWriter reads raw Kafka messages and writes them verbatim to S3/MinIO.
// It preserves the original bytes exactly as received to support replay with
// future parser versions.
type ArchiveWriter struct {
	cfg    *config.Config
	s3     *storage.S3Client
	logger *zap.Logger
	buffer map[string][]bufferedArchiveItem // grouped by service
	mu     sync.Mutex
}

// bufferedArchiveItem keeps a Kafka offset alongside its raw payload. The
// offset is committed only after the corresponding S3 batch is durable.
type bufferedArchiveItem struct {
	item storage.RawArchiveItem
	msg  *kafka.Message
}

func NewArchiveWriter(cfg *config.Config, logger *zap.Logger) (*ArchiveWriter, error) {
	s3Client, err := storage.NewS3Client(cfg.S3, logger)
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}

	return &ArchiveWriter{
		cfg:    cfg,
		s3:     s3Client,
		logger: logger,
		buffer: make(map[string][]bufferedArchiveItem),
	}, nil
}

// extractMetadata pulls the routing fields from the raw JSON without
// fully decoding the event. This is cheaper than a full unmarshal and
// ensures the archive never mutates the original payload.
func extractMetadata(raw []byte) (service string, ts time.Time, err error) {
	// Decode only the fields we need for partitioning
	var envelope struct {
		Service   string    `json:"service"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", time.Time{}, err
	}
	return envelope.Service, envelope.Timestamp, nil
}

// addToBuffer preserves the raw Kafka payload without committing its offset.
func (a *ArchiveWriter) addToBuffer(msg *kafka.Message) bool {
	service, ts, err := extractMetadata(msg.Value)
	if err != nil {
		a.logger.Warn("metadata extraction failed in archive writer", zap.Error(err))
		// Still archive the raw bytes under an "unknown" partition
		service = "_unknown"
		ts = time.Now()
	}

	// Copy the raw bytes — the Kafka library may reuse msg.Value's backing array
	raw := make([]byte, len(msg.Value))
	copy(raw, msg.Value)

	a.mu.Lock()
	a.buffer[service] = append(a.buffer[service], bufferedArchiveItem{
		item: storage.RawArchiveItem{Service: service, Timestamp: ts, Raw: raw},
		msg:  msg,
	})
	shouldFlush := len(a.buffer[service]) >= 500
	a.mu.Unlock()

	return shouldFlush
}

// flushService writes buffered raw events for a service to S3 and returns the
// offsets that may now be committed. On failure it restores the whole batch.
func (a *ArchiveWriter) flushService(service string) ([]*kafka.Message, error) {
	a.mu.Lock()
	items := a.buffer[service]
	if len(items) == 0 {
		a.mu.Unlock()
		return nil, nil
	}
	a.buffer[service] = make([]bufferedArchiveItem, 0, 500)
	a.mu.Unlock()

	rawItems := make([]storage.RawArchiveItem, 0, len(items))
	msgs := make([]*kafka.Message, 0, len(items))
	for _, buffered := range items {
		rawItems = append(rawItems, buffered.item)
		msgs = append(msgs, buffered.msg)
	}

	start := time.Now()
	ctx := context.Background()

	if err := a.s3.ArchiveRawBatch(ctx, rawItems[0].Timestamp, service, rawItems); err != nil {
		a.logger.Error("archive failed", zap.Error(err), zap.String("service", service))
		// Put items back for retry
		a.mu.Lock()
		a.buffer[service] = append(items, a.buffer[service]...)
		a.mu.Unlock()
		return nil, err
	}

	archivedEvents.Add(float64(len(items)))
	archiveLatency.Observe(time.Since(start).Seconds())
	a.logger.Debug("archived raw batch", zap.String("service", service), zap.Int("count", len(items)))

	return msgs, nil
}

// FlushAll writes every buffered service. It returns offsets only when every
// batch succeeds; otherwise nothing is committed and duplicates are replayed.
func (a *ArchiveWriter) FlushAll() ([]*kafka.Message, error) {
	a.mu.Lock()
	services := make([]string, 0, len(a.buffer))
	for svc := range a.buffer {
		services = append(services, svc)
	}
	a.mu.Unlock()

	var committed []*kafka.Message
	for _, svc := range services {
		msgs, err := a.flushService(svc)
		if err != nil {
			return nil, fmt.Errorf("flush %s: %w", svc, err)
		}
		committed = append(committed, msgs...)
	}
	return committed, nil
}

func commitOffsets(consumer *kafkalib.Consumer, msgs []*kafka.Message, logger *zap.Logger) {
	highest := make(map[string]kafka.TopicPartition)
	for _, msg := range msgs {
		key := fmt.Sprintf("%s-%d", *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
		if current, ok := highest[key]; !ok || msg.TopicPartition.Offset > current.Offset {
			highest[key] = msg.TopicPartition
		}
	}
	offsets := make([]kafka.TopicPartition, 0, len(highest))
	for _, tp := range highest {
		tp.Offset++
		offsets = append(offsets, tp)
	}
	if len(offsets) > 0 {
		if _, err := consumer.CommitOffsets(offsets); err != nil {
			logger.Error("archive offset commit failed", zap.Error(err))
		}
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	writer, err := NewArchiveWriter(cfg, logger)
	if err != nil {
		logger.Fatal("archive writer init failed", zap.Error(err))
	}

	// Managed R2 buckets are created and access-scoped outside this service.
	// Keep optional creation only for an explicitly configured local S3 backend.
	if cfg.S3.EnsureBucket {
		if err := writer.s3.EnsureBucket(context.Background()); err != nil {
			logger.Warn("bucket creation", zap.Error(err))
		}
	}

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9092", mux)
	}()

	// Create consumer (separate group from processing workers)
	consumer, err := kafkalib.NewConsumer(cfg.Kafka, cfg.Kafka.GroupArchive,
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
		logger.Info("shutting down archive writer")
		cancel()
	}()

	logger.Info("archive writer started",
		zap.String("group", cfg.Kafka.GroupArchive),
		zap.String("bucket", cfg.S3.Bucket))

	flushTimer := time.NewTimer(10 * time.Second)
	defer flushTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			if msgs, err := writer.FlushAll(); err != nil {
				logger.Error("final archive flush failed", zap.Error(err))
			} else {
				commitOffsets(consumer, msgs, logger)
			}
			return
		case <-flushTimer.C:
			if msgs, err := writer.FlushAll(); err != nil {
				logger.Error("archive flush failed; offsets remain uncommitted", zap.Error(err))
			} else {
				commitOffsets(consumer, msgs, logger)
			}
			flushTimer.Reset(10 * time.Second)
		default:
			msg, err := consumer.Poll(100)
			if err != nil {
				logger.Error("archive consumer poll failed", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			if msg == nil {
				continue
			}
			if writer.addToBuffer(msg) {
				if msgs, err := writer.FlushAll(); err != nil {
					logger.Error("size archive flush failed; offsets remain uncommitted", zap.Error(err))
				} else {
					commitOffsets(consumer, msgs, logger)
				}
				flushTimer.Reset(10 * time.Second)
			}
		}
	}
}
