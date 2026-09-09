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
		Buckets: []float64{1, 10, 50, 100, 500, 1000, 2000, 5000},
	})

	flushFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_flush_failures_total",
		Help: "Total failed Tinybird append attempts (each is retried)",
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

	appendInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "worker_append_inflight",
		Help: "Tinybird appends currently in flight",
	})

	batchesPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "worker_batches_pending",
		Help: "Sealed batches waiting for an append slot",
	})
)

func init() {
	prometheus.MustRegister(eventsProcessed, eventsDeadLettered, processingLatency,
		batchSizeMetric, flushFailures, offsetsCommitted, appendLatency,
		appendInflight, batchesPending)
}

// Pipeline design
//
// The worker is a three-stage pipeline so that the slow step (HTTPS append to
// Tinybird, wait=true) never blocks the fast steps (Kafka poll + normalize):
//
//	poll/normalize (1 goroutine) -> sealed batches (bounded chan)
//		-> append workers (N goroutines) -> completions (chan)
//		-> committer (1 goroutine)
//
//   - The poll loop seals a batch every batchSize events or flushInterval,
//     whichever comes first, and keeps polling while appends are in flight.
//   - N append workers POST batches concurrently with backoff retries.
//   - The committer commits offsets only for the contiguous prefix of
//     acknowledged batches, so at-least-once ordering is preserved even when
//     appends finish out of order.
//   - The bounded batch channel is the backpressure valve: when Tinybird is
//     slow, sealing blocks, polling pauses, and Kafka (not worker RAM) holds
//     the backlog.

// secretPatterns is compiled once: redactSecrets runs on every event, so
// compiling these per message would dominate worker CPU at high throughput.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(password|passwd|secret|token|api_key|apikey|authorization)\s*[:=]\s*\S+`),
	regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`),
	regexp.MustCompile(`\b(?:\d{4}[- ]?){3}\d{4}\b`),
}

// sealedBatch is a batch sealed by the poll loop, in poll order. seq numbers
// are dense (0, 1, 2, ...) so the committer can detect a contiguous prefix.
type sealedBatch struct {
	seq    uint64
	events []models.LogEvent
	msgs   []*kafka.Message
}

// completedBatch reports a durably appended batch back to the committer. Only
// TopicPartitions are kept so out-of-order completions don't pin full payloads.
// err is non-nil only when shutting down mid-retry; the committer must not
// advance offsets past a failed batch.
type completedBatch struct {
	seq     uint64
	offsets []kafka.TopicPartition
	err     error
}

// batcher accumulates normalized events until sealed by size or time. It is
// owned by the poll goroutine only — no locking needed.
type batcher struct {
	maxSize int
	events  []models.LogEvent
	msgs    []*kafka.Message
}

func newBatcher(maxSize int) *batcher {
	return &batcher{
		maxSize: maxSize,
		events:  make([]models.LogEvent, 0, maxSize),
		msgs:    make([]*kafka.Message, 0, maxSize),
	}
}

func (b *batcher) add(event models.LogEvent, msg *kafka.Message) {
	b.events = append(b.events, event)
	b.msgs = append(b.msgs, msg)
}

// addCommitOnly tracks a DLQ'd message: no event to append, but its offset
// must still advance so it isn't re-processed forever.
func (b *batcher) addCommitOnly(msg *kafka.Message) {
	b.msgs = append(b.msgs, msg)
}

func (b *batcher) len() int      { return len(b.msgs) }
func (b *batcher) empty() bool   { return len(b.msgs) == 0 }
func (b *batcher) full() bool    { return len(b.msgs) >= b.maxSize }
func (b *batcher) seal(seq uint64) *sealedBatch {
	sb := &sealedBatch{seq: seq, events: b.events, msgs: b.msgs}
	b.events = make([]models.LogEvent, 0, b.maxSize)
	b.msgs = make([]*kafka.Message, 0, b.maxSize)
	return sb
}

// Processor handles event normalization, enrichment, redaction, and appends.
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

// bufferOne normalizes a message into the open batch. It reports whether the
// batch reached the size threshold and should be sealed. Offsets are never
// committed here — only the committer advances them, after a durable write.
func (p *Processor) bufferOne(b *batcher, msg *kafka.Message) (shouldSeal bool) {
	start := time.Now()
	event, err := p.normalize(msg.Value)
	processingLatency.Observe(time.Since(start).Seconds())

	if err != nil {
		p.sendToDLQ(msg.Value, err)
		eventsDeadLettered.Inc()
		b.addCommitOnly(msg)
		return b.full()
	}

	b.add(*event, msg)
	eventsProcessed.Inc()
	return b.full()
}

// appendWithRetry appends one sealed batch, retrying with exponential backoff
// until Tinybird acknowledges it. It returns success only on acknowledgement:
// dropping an un-acked batch would lose data and committing past it would skip
// data, so the pipeline stalls (with Kafka buffering) instead. The only error
// return is on shutdown, when the caller must hold offsets for redelivery.
func (p *Processor) appendWithRetry(shutdownCtx context.Context, batch *sealedBatch) error {
	if len(batch.events) == 0 {
		return nil // DLQ/commit-only batch: nothing to append
	}

	batchSizeMetric.Observe(float64(len(batch.events)))

	backoff := 200 * time.Millisecond
	const maxBackoff = 10 * time.Second
	for attempt := 1; ; attempt++ {
		// Detached per-attempt context: an in-flight append is allowed to
		// finish during shutdown instead of being cancelled mid-write.
		attemptCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		appendErr := p.tinybird.AppendEvents(attemptCtx, batch.events)
		appendLatency.Observe(time.Since(start).Seconds())
		cancel()

		if appendErr == nil {
			if attempt > 1 {
				p.logger.Info("batch append recovered",
					zap.Uint64("seq", batch.seq),
					zap.Int("attempts", attempt),
					zap.Int("events", len(batch.events)))
			}
			return nil
		}

		flushFailures.Inc()
		p.logger.Warn("batch append failed, backing off",
			zap.Uint64("seq", batch.seq),
			zap.Int("events", len(batch.events)),
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Error(appendErr))

		select {
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// appendWorker drains sealed batches and reports completions. Multiple workers
// run concurrently; each in-flight append holds one gauge slot.
func (p *Processor) appendWorker(shutdownCtx context.Context, batches <-chan *sealedBatch, done chan<- completedBatch, wg *sync.WaitGroup) {
	defer wg.Done()
	for batch := range batches {
		batchesPending.Dec()
		appendInflight.Inc()
		err := p.appendWithRetry(shutdownCtx, batch)
		appendInflight.Dec()

		offsets := make([]kafka.TopicPartition, 0, len(batch.msgs))
		for _, m := range batch.msgs {
			offsets = append(offsets, m.TopicPartition)
		}
		done <- completedBatch{seq: batch.seq, offsets: offsets, err: err}
	}
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
	p.producer.Close()
}

// partitionKey identifies a topic-partition for offset tracking.
type partitionKey struct {
	topic     string
	partition int32
}

// commitOffsets commits the highest offset per partition from a set of
// TopicPartitions. Kafka expects the NEXT offset to consume.
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
	} else {
		offsetsCommitted.Inc()
	}
}

// runCommitter commits offsets only for the contiguous prefix of acknowledged
// batches. Batches seal in poll order, so per-partition offsets rise
// monotonically across batches: committing a seq prefix can never skip an
// un-acked message, even when appends finish out of order. A failed batch
// (shutdown only) halts all further commits; uncommitted messages are
// re-delivered on restart.
func runCommitter(consumer *kafkalib.Consumer, done <-chan completedBatch, logger *zap.Logger) {
	pending := make(map[uint64][]kafka.TopicPartition)
	var next uint64
	failed := false

	for cb := range done {
		if cb.err != nil {
			logger.Error("batch was not durably appended; holding offsets for redelivery",
				zap.Uint64("seq", cb.seq), zap.Error(cb.err))
			failed = true
		}
		if failed {
			continue
		}
		pending[cb.seq] = cb.offsets

		var prefix []kafka.TopicPartition
		for {
			offsets, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			prefix = append(prefix, offsets...)
			next++
		}
		if len(prefix) > 0 {
			commitOffsets(consumer, prefix, logger)
		}
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	// Pipeline tuning, all overridable via WORKER_* env vars.
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
	if concurrency > 32 {
		concurrency = 32
	}
	flushInterval := time.Duration(flushMs) * time.Millisecond

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

	// Bounded channels are the backpressure valve: seal blocks when the
	// pipeline is full, pausing polls while Kafka holds the backlog.
	batches := make(chan *sealedBatch, concurrency*2)
	done := make(chan completedBatch, concurrency*2)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go proc.appendWorker(ctx, batches, done, &wg)
	}
	commitDone := make(chan struct{})
	go func() {
		runCommitter(consumer, done, logger)
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

	// seal hands the open batch to the append pipeline. The send blocks when
	// the pipeline is full, which pauses polling — safe at any time because
	// append workers drain until batches is closed.
	seal := func() {
		if b.empty() {
			return
		}
		sb := b.seal(seq)
		seq++
		batchesPending.Inc()
		batches <- sb
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

running:
	for {
		select {
		case <-ctx.Done():
			break running

		case <-ticker.C:
			// Time-based seal: bounds end-to-end latency at low traffic.
			seal()

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

			if proc.bufferOne(b, msg) {
				// Size-based seal.
				seal()
			}
		}
	}

	// Shutdown: seal the tail, drain appends, then commit everything acked.
	seal()
	close(batches)
	wg.Wait()
	close(done)
	<-commitDone
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
