package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
		Help: "Events normalized (not yet acknowledged by Tinybird)",
	})
	eventsAppended = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_events_appended_total",
		Help: "Events acknowledged by Tinybird",
	})
	eventsDeadLettered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_events_dead_lettered_total",
		Help: "Events sent to the dead-letter topic",
	})
	processingLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_processing_latency_seconds",
		Help:    "Normalize + redact latency per event",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
	})
	batchSizeMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_batch_size",
		Help:    "Events per sealed Tinybird batch",
		Buckets: []float64{1, 10, 50, 100, 500, 1000, 2000, 5000},
	})
	flushFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_flush_failures_total",
		Help: "Failed Tinybird append attempts (retried if transient)",
	})
	offsetsCommitted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_offsets_committed_total",
		Help: "Successful offset commits (only after durable write or DLQ)",
	})
	appendLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_append_latency_seconds",
		Help:    "Tinybird append round-trip per attempt",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	})
	appendInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "worker_append_inflight",
		Help: "Tinybird appends currently in flight",
	})
	batchesPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "worker_batches_pending",
		Help: "Sealed batches waiting for an append worker",
	})
	uncommittedBatches = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "worker_uncommitted_batches",
		Help: "Sealed batches not yet in the committed offset prefix",
	})
)

func init() {
	prometheus.MustRegister(
		eventsProcessed,
		eventsAppended,
		eventsDeadLettered,
		processingLatency,
		batchSizeMetric,
		flushFailures,
		offsetsCommitted,
		appendLatency,
		appendInflight,
		batchesPending,
		uncommittedBatches,
	)
}

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(password|passwd|secret|token|api_key|apikey|authorization)\s*[:=]\s*\S+`),
	regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`),
	regexp.MustCompile(`\b(?:\d{4}[- ]?){3}\d{4}\b`),
}

// sealedBatch is produced in poll order. seq is dense (0, 1, 2, ...) so the
// committer can advance only the contiguous acknowledged prefix.
type sealedBatch struct {
	seq     uint64
	events  []models.LogEvent
	offsets []kafka.TopicPartition
}

type completedBatch struct {
	seq     uint64
	offsets []kafka.TopicPartition
	err     error
}

type batcher struct {
	maxSize int
	events  []models.LogEvent
	offsets []kafka.TopicPartition
}

func newBatcher(maxSize int) *batcher {
	return &batcher{
		maxSize: maxSize,
		events:  make([]models.LogEvent, 0, maxSize),
		offsets: make([]kafka.TopicPartition, 0, maxSize),
	}
}

func (b *batcher) add(event models.LogEvent, tp kafka.TopicPartition) {
	b.events = append(b.events, event)
	b.offsets = append(b.offsets, tp)
}

func (b *batcher) addCommitOnly(tp kafka.TopicPartition) {
	b.offsets = append(b.offsets, tp)
}

func (b *batcher) empty() bool { return len(b.offsets) == 0 }
func (b *batcher) full() bool  { return len(b.offsets) >= b.maxSize }

func (b *batcher) seal(seq uint64) *sealedBatch {
	sb := &sealedBatch{seq: seq, events: b.events, offsets: b.offsets}
	b.events = make([]models.LogEvent, 0, b.maxSize)
	b.offsets = make([]kafka.TopicPartition, 0, b.maxSize)
	return sb
}

type Processor struct {
	cfg      *config.Config
	tinybird *tinybird.Client
	producer *kafkalib.Producer
	logger   *zap.Logger
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
	}, nil
}

func (p *Processor) Close() {
	p.producer.Close()
}

func (p *Processor) normalize(raw []byte) (*models.LogEvent, error) {
	var event models.LogEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	event.Severity = models.NormalizeSeverity(event.Severity)
	event.Message = redactSecrets(event.Message)
	if event.Attributes != nil {
		redactAttributes(event.Attributes)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	return &event, nil
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

func (p *Processor) bufferOne(ctx context.Context, b *batcher, msg *kafka.Message) error {
	start := time.Now()
	raw := make([]byte, len(msg.Value))
	copy(raw, msg.Value)
	tp := copyTP(msg.TopicPartition)

	event, err := p.normalize(raw)
	processingLatency.Observe(time.Since(start).Seconds())
	if err != nil {
		if dlqErr := p.sendToDLQ(ctx, raw, err); dlqErr != nil {
			return dlqErr
		}
		eventsDeadLettered.Inc()
		b.addCommitOnly(tp)
		return nil
	}
	b.add(*event, tp)
	eventsProcessed.Inc()
	return nil
}

func (p *Processor) sendToDLQ(ctx context.Context, raw []byte, originalErr error) error {
	dlqEvent := models.DeadLetterEvent{
		Raw:       string(raw),
		Error:     originalErr.Error(),
		FailedAt:  time.Now(),
		Component: "processing-worker",
	}
	data, err := json.Marshal(dlqEvent)
	if err != nil {
		return fmt.Errorf("DLQ marshal: %w", err)
	}

	backoff := 200 * time.Millisecond
	const maxBackoff = 10 * time.Second
	for {
		if err := p.producer.ProduceSync(p.cfg.Kafka.TopicDLQ, "dlq", data); err == nil {
			return nil
		} else {
			p.logger.Error("DLQ produce failed, retrying", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("DLQ produce aborted: %w", ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	msg := strings.ToLower(err.Error())
	permanent := []string{
		"quarantined",
		"unexpected payload",
		"append token",
		"returned 400",
		"returned 401",
		"returned 403",
		"returned 404",
		"returned 422",
	}
	for _, s := range permanent {
		if strings.Contains(msg, s) {
			return false
		}
	}
	if strings.Contains(msg, "committed") && strings.Contains(msg, " of ") && strings.Contains(msg, " rows") {
		return false
	}
	return true
}

func (p *Processor) appendWithRetry(shutdownCtx context.Context, batch *sealedBatch) error {
	if len(batch.events) == 0 {
		return nil
	}
	backoff := 200 * time.Millisecond
	const maxBackoff = 10 * time.Second
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		err := p.tinybird.AppendEvents(attemptCtx, batch.events)
		appendLatency.Observe(time.Since(start).Seconds())
		cancel()
		if err == nil {
			if attempt > 1 {
				p.logger.Info("batch append recovered",
					zap.Uint64("seq", batch.seq),
					zap.Int("attempts", attempt),
					zap.Int("events", len(batch.events)))
			}
			return nil
		}
		if !isRetryable(err) {
			return err
		}
		flushFailures.Inc()
		p.logger.Warn("batch append failed, backing off",
			zap.Uint64("seq", batch.seq),
			zap.Int("attempt", attempt),
			zap.Int("events", len(batch.events)),
			zap.Duration("backoff", backoff),
			zap.Error(err))
		select {
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func (p *Processor) persistBatch(shutdownCtx context.Context, batch *sealedBatch) error {
	err := p.appendWithRetry(shutdownCtx, batch)
	if err == nil {
		eventsAppended.Add(float64(len(batch.events)))
		return nil
	}
	if shutdownCtx.Err() != nil {
		return err
	}
	p.logger.Error("permanent append failure; DLQ then advance offsets",
		zap.Uint64("seq", batch.seq),
		zap.Int("events", len(batch.events)),
		zap.Error(err))
	for i := range batch.events {
		raw, mErr := json.Marshal(batch.events[i])
		if mErr != nil {
			raw = []byte(`{"error":"marshal failed"}`)
		}
		if dlqErr := p.sendToDLQ(shutdownCtx, raw, err); dlqErr != nil {
			return dlqErr
		}
		eventsDeadLettered.Inc()
	}
	return nil
}

func (p *Processor) appendWorker(ctx context.Context, batches <-chan *sealedBatch, done chan<- completedBatch, wg *sync.WaitGroup) {
	defer wg.Done()
	for batch := range batches {
		batchesPending.Dec()
		appendInflight.Inc()
		err := p.persistBatch(ctx, batch)
		appendInflight.Dec()
		done <- completedBatch{seq: batch.seq, offsets: batch.offsets, err: err}
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
	if _, err := consumer.CommitOffsets(offsets); err != nil {
		logger.Error("offset commit failed", zap.Error(err))
		return
	}
	offsetsCommitted.Inc()
}

func runCommitter(consumer *kafkalib.Consumer, done <-chan completedBatch, admit chan struct{}, logger *zap.Logger) {
	pending := make(map[uint64][]kafka.TopicPartition)
	var next uint64
	for cb := range done {
		if cb.err != nil {
			logger.Error("batch was not durably appended; holding offsets for replay",
				zap.Uint64("seq", cb.seq), zap.Error(cb.err))
			continue
		}
		pending[cb.seq] = cb.offsets
		var prefix []kafka.TopicPartition
		released := 0
		for {
			offsets, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			prefix = append(prefix, offsets...)
			next++
			released++
		}
		if len(prefix) > 0 {
			commitOffsets(consumer, prefix, logger)
		}
		for i := 0; i < released; i++ {
			select {
			case <-admit:
				uncommittedBatches.Dec()
			default:
			}
		}
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	batchSize := cfg.Worker.BatchSize
	if batchSize <= 0 {
		batchSize = 2000
	}
	flushMs := cfg.Worker.FlushIntervalMs
	if flushMs <= 0 {
		flushMs = 250
	}
	concurrency := cfg.Worker.AppendConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	if concurrency > 64 {
		concurrency = 64
	}
	flushInterval := time.Duration(flushMs) * time.Millisecond

	proc, err := NewProcessor(cfg, logger)
	if err != nil {
		logger.Fatal("processor init failed", zap.Error(err))
	}
	defer proc.Close()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":9091", mux)
	}()

	consumer, err := kafkalib.NewConsumer(cfg.Kafka, cfg.Kafka.GroupWorkers, []string{cfg.Kafka.TopicLogs}, logger)
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

	admit := make(chan struct{}, concurrency)
	batches := make(chan *sealedBatch, concurrency)
	done := make(chan completedBatch, concurrency)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go proc.appendWorker(ctx, batches, done, &wg)
	}
	commitDone := make(chan struct{})
	go func() {
		runCommitter(consumer, done, admit, logger)
		close(commitDone)
	}()

	logger.Info("processing worker started",
		zap.String("group", cfg.Kafka.GroupWorkers),
		zap.String("topic", cfg.Kafka.TopicLogs),
		zap.Int("batch_size", batchSize),
		zap.Duration("flush_interval", flushInterval),
		zap.Int("append_concurrency", concurrency))

	b := newBatcher(batchSize)
	var seq uint64
	var leftover []*sealedBatch

	seal := func() bool {
		if b.empty() {
			return true
		}
		sb := b.seal(seq)
		seq++
		batchSizeMetric.Observe(float64(len(sb.events)))
		select {
		case admit <- struct{}{}:
			uncommittedBatches.Inc()
			batchesPending.Inc()
			batches <- sb
			return true
		case <-ctx.Done():
			leftover = append(leftover, sb)
			return false
		}
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

running:
	for {
		select {
		case <-ctx.Done():
			break running
		case <-ticker.C:
			if !seal() {
				break running
			}
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
			if err := proc.bufferOne(ctx, b, msg); err != nil {
				logger.Error("buffer/DLQ failed; pausing poll", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			if b.full() {
				if !seal() {
					break running
				}
			}
		}
	}

	if !b.empty() {
		leftover = append(leftover, b.seal(seq))
	}
	close(batches)
	wg.Wait()
	close(done)
	<-commitDone

	for _, sb := range leftover {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := proc.persistBatch(flushCtx, sb)
		flushCancel()
		if err != nil {
			logger.Error("shutdown flush failed; offsets remain uncommitted",
				zap.Uint64("seq", sb.seq), zap.Error(err))
			continue
		}
		commitOffsets(consumer, sb.offsets, logger)
	}
}

func normalizeSeverity(sev string) string {
	upper := strings.ToUpper(strings.TrimSpace(sev))
	switch upper {
	case "FATAL", "CRITICAL", "EMERGENCY", "EMERG":
		return "FATAL"
	case "ERROR", "ERR":
		return "ERROR"
	case "WARNING", "WARN":
		return "WARNING"
	case "INFO", "INFORMATION":
		return "INFO"
	case "DEBUG", "TRACE":
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