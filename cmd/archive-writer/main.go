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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	kafkalib "github.com/log-analytics-platform/internal/kafka"
	"github.com/log-analytics-platform/internal/storage"
)

const (
	maxPartitionBatch = 500
	maxBuffered       = 5000
	uploadWorkers     = 4
	flushInterval     = 10 * time.Second
	putTimeout        = 30 * time.Second
)

var (
	archivedEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "archive_events_total",
		Help: "Total events archived to Cloudflare R2",
	})
	archiveLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "archive_batch_latency_seconds",
		Help:    "Archive batch write latency",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
	})
	archiveFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "archive_flush_failures_total",
		Help: "Failed R2 put attempts (retried)",
	})
	bufferedEvents = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "archive_buffered_events",
		Help: "Raw events buffered and not yet handed to an upload worker",
	})
	uploadsInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "archive_uploads_inflight",
		Help: "R2 uploads currently in flight",
	})
	offsetsCommitted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "archive_offsets_committed_total",
		Help: "Successful offset commits (only after R2 put)",
	})
)

func init() {
	prometheus.MustRegister(
		archivedEvents,
		archiveLatency,
		archiveFailures,
		bufferedEvents,
		uploadsInflight,
		offsetsCommitted,
	)
}

type partKey struct {
	hour    string // UTC hour: 2006-01-02T15
	service string
}

type bufferedArchiveItem struct {
	item storage.RawArchiveItem
	tp   kafka.TopicPartition
}

type uploadJob struct {
	key   partKey
	items []bufferedArchiveItem
}

type uploadResult struct {
	offsets []kafka.TopicPartition
	err     error
	key     partKey
	count   int
}

type ArchiveWriter struct {
	cfg    *config.Config
	s3     *storage.S3Client
	logger *zap.Logger
	buffer map[partKey][]bufferedArchiveItem
	total  int
	mu     sync.Mutex
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
		buffer: make(map[partKey][]bufferedArchiveItem),
	}, nil
}

func extractMetadata(raw []byte) (service string, ts time.Time, err error) {
	var envelope struct {
		Service   string    `json:"service"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", time.Time{}, err
	}
	return envelope.Service, envelope.Timestamp, nil
}

func copyTP(tp kafka.TopicPartition) kafka.TopicPartition {
	out := kafka.TopicPartition{
		Partition: tp.Partition,
		Offset:    tp.Offset,
	}
	if tp.Topic != nil {
		t := *tp.Topic
		out.Topic = &t
	}
	return out
}

func makeKey(service string, ts time.Time) partKey {
	if service == "" {
		service = "_unknown"
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	hour := ts.UTC().Truncate(time.Hour).Format("2006-01-02T15")
	return partKey{hour: hour, service: service}
}

func (a *ArchiveWriter) addToBuffer(msg *kafka.Message) (flushKey partKey, shouldFlush bool) {
	service, ts, err := extractMetadata(msg.Value)
	if err != nil {
		a.logger.Warn("metadata extraction failed in archive writer", zap.Error(err))
		service = "_unknown"
		ts = time.Now()
	}
	raw := make([]byte, len(msg.Value))
	copy(raw, msg.Value)
	key := makeKey(service, ts)

	item := bufferedArchiveItem{
		item: storage.RawArchiveItem{
			Service:   key.service,
			Timestamp: ts.UTC(),
			Raw:       raw,
		},
		tp: copyTP(msg.TopicPartition),
	}

	a.mu.Lock()
	a.buffer[key] = append(a.buffer[key], item)
	a.total++
	n := len(a.buffer[key])
	total := a.total
	a.mu.Unlock()
	bufferedEvents.Set(float64(total))

	if n >= maxPartitionBatch {
		return key, true
	}
	return key, false
}

func (a *ArchiveWriter) take(key partKey) []bufferedArchiveItem {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := a.buffer[key]
	if len(items) == 0 {
		return nil
	}
	delete(a.buffer, key)
	a.total -= len(items)
	bufferedEvents.Set(float64(a.total))
	return items
}

func (a *ArchiveWriter) takeLargest() (partKey, []bufferedArchiveItem) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var best partKey
	bestN := 0
	for k, v := range a.buffer {
		if len(v) > bestN {
			best = k
			bestN = len(v)
		}
	}
	if bestN == 0 {
		return partKey{}, nil
	}
	items := a.buffer[best]
	delete(a.buffer, best)
	a.total -= len(items)
	bufferedEvents.Set(float64(a.total))
	return best, items
}

func (a *ArchiveWriter) takeAll() map[partKey][]bufferedArchiveItem {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.buffer
	a.buffer = make(map[partKey][]bufferedArchiveItem)
	a.total = 0
	bufferedEvents.Set(0)
	return out
}

func (a *ArchiveWriter) bufferedTotal() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total
}

func (a *ArchiveWriter) upload(ctx context.Context, job uploadJob) uploadResult {
	if len(job.items) == 0 {
		return uploadResult{key: job.key}
	}

	rawItems := make([]storage.RawArchiveItem, 0, len(job.items))
	offsets := make([]kafka.TopicPartition, 0, len(job.items))
	for _, it := range job.items {
		rawItems = append(rawItems, it.item)
		offsets = append(offsets, it.tp)
	}

	backoff := 200 * time.Millisecond
	const maxBackoff = 10 * time.Second
	for attempt := 1; ; attempt++ {
		putCtx, cancel := context.WithTimeout(context.Background(), putTimeout)
		start := time.Now()
		err := a.s3.ArchiveRawBatch(putCtx, rawItems[0].Timestamp.UTC(), job.key.service, rawItems)
		cancel()
		if err == nil {
			archivedEvents.Add(float64(len(job.items)))
			archiveLatency.Observe(time.Since(start).Seconds())
			a.logger.Debug("archived raw batch",
				zap.String("service", job.key.service),
				zap.String("hour", job.key.hour),
				zap.Int("count", len(job.items)))
			return uploadResult{offsets: offsets, key: job.key, count: len(job.items)}
		}

		archiveFailures.Inc()
		a.logger.Error("archive failed, backing off",
			zap.Error(err),
			zap.String("service", job.key.service),
			zap.String("hour", job.key.hour),
			zap.Int("attempt", attempt),
			zap.Int("count", len(job.items)),
			zap.Duration("backoff", backoff))

		select {
		case <-ctx.Done():
			return uploadResult{err: ctx.Err(), key: job.key, count: len(job.items)}
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

type partitionKey struct {
	topic     string
	partition int32
}

func commitOffsets(consumer *kafkalib.Consumer, tps []kafka.TopicPartition, logger *zap.Logger) {
	if len(tps) == 0 {
		return
	}
	highest := make(map[partitionKey]kafka.TopicPartition)
	for _, tp := range tps {
		if tp.Topic == nil {
			continue
		}
		k := partitionKey{topic: *tp.Topic, partition: tp.Partition}
		if cur, ok := highest[k]; !ok || tp.Offset > cur.Offset {
			highest[k] = tp
		}
	}
	offsets := make([]kafka.TopicPartition, 0, len(highest))
	for _, tp := range highest {
		tp.Offset++
		offsets = append(offsets, tp)
	}
	if len(offsets) == 0 {
		return
	}
	if _, err := consumer.CommitOffsets(offsets); err != nil {
		logger.Error("archive offset commit failed", zap.Error(err))
		return
	}
	offsetsCommitted.Inc()
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	writer, err := NewArchiveWriter(cfg, logger)
	if err != nil {
		logger.Fatal("archive writer init failed", zap.Error(err))
	}

	if cfg.S3.EnsureBucket {
		if err := writer.s3.EnsureBucket(context.Background()); err != nil {
			logger.Warn("bucket creation", zap.Error(err))
		}
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":9092", mux)
	}()

	consumer, err := kafkalib.NewConsumer(cfg.Kafka, cfg.Kafka.GroupArchive, []string{cfg.Kafka.TopicLogs}, logger)
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

	jobs := make(chan uploadJob)
	results := make(chan uploadResult, uploadWorkers)
	var wg sync.WaitGroup
	for i := 0; i < uploadWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- writer.upload(ctx, job)
			}
		}()
	}

	inFlight := 0

	handleResult := func(res uploadResult) {
		inFlight--
		uploadsInflight.Dec()
		if res.err != nil {
			logger.Error("archive batch abandoned; offsets remain uncommitted",
				zap.Error(res.err),
				zap.String("service", res.key.service),
				zap.String("hour", res.key.hour),
				zap.Int("count", res.count))
			return
		}
		commitOffsets(consumer, res.offsets, logger)
	}

	sendJob := func(key partKey, items []bufferedArchiveItem) bool {
		if len(items) == 0 {
			return true
		}
		job := uploadJob{key: key, items: items}
		for {
			select {
			case jobs <- job:
				inFlight++
				uploadsInflight.Inc()
				return true
			case res := <-results:
				handleResult(res)
			case <-ctx.Done():
				return false
			}
		}
	}

	enqueueKey := func(key partKey) bool {
		return sendJob(key, writer.take(key))
	}

	enqueueAll := func() bool {
		all := writer.takeAll()
		for key, items := range all {
			if !sendJob(key, items) {
				return false
			}
		}
		return true
	}

	drainOverCap := func() bool {
		for writer.bufferedTotal() >= maxBuffered {
			key, items := writer.takeLargest()
			if len(items) == 0 {
				return true
			}
			if !sendJob(key, items) {
				return false
			}
		}
		return true
	}

	logger.Info("archive writer started",
		zap.String("group", cfg.Kafka.GroupArchive),
		zap.String("bucket", cfg.S3.Bucket),
		zap.Int("upload_workers", uploadWorkers),
		zap.Int("max_buffered", maxBuffered))

	flushTimer := time.NewTimer(flushInterval)
	defer flushTimer.Stop()

running:
	for {
		if writer.bufferedTotal() >= maxBuffered {
			select {
			case <-ctx.Done():
				break running
			case res := <-results:
				handleResult(res)
			case <-flushTimer.C:
				if !enqueueAll() {
					break running
				}
				flushTimer.Reset(flushInterval)
			}
			continue
		}

		select {
		case <-ctx.Done():
			break running
		case res := <-results:
			handleResult(res)
		case <-flushTimer.C:
			if !enqueueAll() {
				break running
			}
			flushTimer.Reset(flushInterval)
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
			key, shouldFlush := writer.addToBuffer(msg)
			if shouldFlush {
				if !enqueueKey(key) {
					break running
				}
			}
			if !drainOverCap() {
				break running
			}
		}
	}

	_ = enqueueAll()
	close(jobs)
	for inFlight > 0 {
		handleResult(<-results)
	}
	wg.Wait()
}