package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	kafkalib "github.com/log-analytics-platform/internal/kafka"
	"github.com/log-analytics-platform/internal/models"
)

var (
	eventsIngested = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_events_ingested_total",
		Help: "Total events ingested",
	}, []string{"service", "status"})

	eventsRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_events_rejected_total",
		Help: "Total events rejected",
	})

	ingestLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_ingest_latency_seconds",
		Help:    "Ingest request latency",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})

	currentRate = atomic.Int64{}
)

func init() {
	prometheus.MustRegister(eventsIngested, eventsRejected, ingestLatency)
}

type Gateway struct {
	cfg      *config.Config
	producer *kafkalib.Producer
	logger   *zap.Logger
	quota    int64
}

func NewGateway(cfg *config.Config, logger *zap.Logger) (*Gateway, error) {
	producer, err := kafkalib.NewProducer(cfg.Kafka, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Gateway{
		cfg:      cfg,
		producer: producer,
		logger:   logger,
		quota:    int64(cfg.Gateway.IngestQuotaPerSec),
	}, nil
}

func (g *Gateway) handleIngest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		ingestLatency.Observe(time.Since(start).Seconds())
	}()

	var req models.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	// Rate limiting
	rate := currentRate.Load()
	if rate > g.quota {
		eventsRejected.Add(float64(len(req.Events)))
		w.Header().Set("Retry-After", "1")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":       "quota exceeded",
			"retry_after": 1,
		})
		return
	}

	// Validate and prepare messages — but do NOT count them as accepted yet.
	var batch []kafkalib.BatchMessage
	validationFailures := 0
	services := make([]string, 0, len(req.Events)) // parallel to batch for metrics

	for i := range req.Events {
		ev := &req.Events[i]

		// Assign event ID if missing
		if ev.EventID == "" {
			ev.EventID = uuid.New().String()
		}

		// Assign timestamp if missing
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now()
		}

		// Validate required fields
		if ev.Service == "" || ev.Message == "" {
			validationFailures++
			eventsIngested.WithLabelValues("unknown", "rejected").Inc()
			continue
		}

		// Partition by service+source for per-key ordering
		partitionKey := ev.Service
		if ev.Source != "" {
			partitionKey = ev.Service + ":" + ev.Source
		}

		data, err := json.Marshal(ev)
		if err != nil {
			g.logger.Error("marshal error", zap.Error(err))
			validationFailures++
			continue
		}

		batch = append(batch, kafkalib.BatchMessage{
			Topic: g.cfg.Kafka.TopicLogs,
			Key:   []byte(partitionKey),
			Value: data,
		})
		services = append(services, ev.Service)
	}

	// Enqueue all valid messages and BLOCK until Kafka confirms every delivery.
	// Only messages that Kafka actually persists are reported as accepted.
	accepted := 0
	kafkaFailures := 0
	if len(batch) > 0 {
		result, err := g.producer.ProduceBatch(batch, 10*time.Second)
		if err != nil {
			g.logger.Error("batch produce error", zap.Error(err))
		}

		accepted = result.Accepted
		kafkaFailures = result.Failed

		// Emit per-service metrics based on actual delivery outcomes.
		// We attribute failures to the services round-robin since the
		// BatchResult doesn't carry per-index identity; in practice the
		// total counts are what matter for monitoring.
		for i := 0; i < accepted && i < len(services); i++ {
			eventsIngested.WithLabelValues(services[i], "accepted").Inc()
		}
		for i := accepted; i < len(services); i++ {
			eventsIngested.WithLabelValues(services[i], "kafka_failed").Inc()
		}

		currentRate.Add(int64(accepted))
	}

	totalFailed := validationFailures + kafkaFailures
	if kafkaFailures > 0 {
		eventsRejected.Add(float64(kafkaFailures))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.IngestResponse{
		Accepted: accepted,
		Failed:   totalFailed,
	})
}

func (g *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (g *Gateway) Start() error {
	router := mux.NewRouter()
	router.HandleFunc("/v1/ingest", g.handleIngest).Methods("POST")
	router.HandleFunc("/health", g.handleHealth).Methods("GET")
	router.Handle("/metrics", promhttp.Handler())

	handler := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	}).Handler(router)

	addr := fmt.Sprintf(":%d", g.cfg.Gateway.Port)
	g.logger.Info("gateway starting", zap.String("addr", addr))

	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Reset rate counter periodically
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			currentRate.Store(0)
		}
	}()

	return server.ListenAndServe()
}

func (g *Gateway) Close() {
	g.producer.Close()
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	// Ensure Kafka topics exist
	if err := kafkalib.CreateTopics(cfg.Kafka.Brokers,
		[]string{cfg.Kafka.TopicLogs, cfg.Kafka.TopicDLQ}, 12, 1); err != nil {
		logger.Warn("topic creation", zap.Error(err))
	}

	gw, err := NewGateway(cfg, logger)
	if err != nil {
		logger.Fatal("gateway init failed", zap.Error(err))
	}
	defer gw.Close()

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down gateway")
		cancel()
		_ = ctx
	}()

	if err := gw.Start(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("gateway failed", zap.Error(err))
	}
}