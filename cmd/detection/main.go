package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/detection"
	kafkalib "github.com/log-analytics-platform/internal/kafka"
	"github.com/log-analytics-platform/internal/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	detectionEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "detection_events_processed_total",
		Help: "Total events processed by detection",
	})

	alertsFired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "detection_alerts_fired_total",
		Help: "Total alerts fired",
	})

	detectionLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "detection_processing_latency_seconds",
		Help:    "Detection processing latency",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10),
	})
)

func init() {
	prometheus.MustRegister(detectionEvents, alertsFired, detectionLatency)
}

// DetectionService consumes from Kafka and evaluates detection rules.
type DetectionService struct {
	cfg      *config.Config
	engine   *detection.Engine
	producer *kafkalib.Producer
	logger   *zap.Logger
}

func NewDetectionService(cfg *config.Config, logger *zap.Logger) (*DetectionService, error) {
	producer, err := kafkalib.NewProducer(cfg.Kafka, logger)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	engine := detection.NewEngine(
		cfg.Detection.WindowMinutes,
		cfg.Detection.ErrorRateThreshold,
		cfg.Detection.MinEvents,
	)

	ds := &DetectionService{
		cfg:      cfg,
		engine:   engine,
		producer: producer,
		logger:   logger,
	}

	// Register alert callback
	engine.OnAlert(ds.handleAlert)

	return ds, nil
}

// Process handles a single event from Kafka.
func (ds *DetectionService) Process(msg *kafka.Message) error {
	start := time.Now()
	defer func() {
		detectionLatency.Observe(time.Since(start).Seconds())
	}()

	var event models.LogEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		ds.logger.Warn("unmarshal error in detection", zap.Error(err))
		return nil
	}

	ds.engine.ProcessEvent(event)
	detectionEvents.Inc()

	return nil
}

// handleAlert processes a fired alert.
func (ds *DetectionService) handleAlert(alert models.Alert) {
	alertsFired.Inc()
	ds.logger.Warn("alert fired",
		zap.String("service", alert.Service),
		zap.String("rule", alert.RuleID),
		zap.Float64("value", alert.Value),
		zap.String("message", alert.Message))

	// Publish alert to a dedicated Kafka topic for the alert service
	data, err := json.Marshal(alert)
	if err != nil {
		ds.logger.Error("alert marshal failed", zap.Error(err))
		return
	}

	if err := ds.producer.Produce("alerts", alert.Fingerprint, data); err != nil {
		ds.logger.Error("alert produce failed", zap.Error(err))
	}
}

func (ds *DetectionService) Close() {
	ds.producer.Close()
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	// Ensure alerts topic exists
	if err := kafkalib.CreateTopics(cfg.Kafka.Brokers, []string{"alerts"}, 6, 1); err != nil {
		logger.Warn("alerts topic creation", zap.Error(err))
	}

	ds, err := NewDetectionService(cfg, logger)
	if err != nil {
		logger.Fatal("detection service init failed", zap.Error(err))
	}
	defer ds.Close()

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "healthy",
				"windows": ds.engine.GetWindows(),
			})
		})
		http.ListenAndServe(":9093", mux)
	}()

	// Create consumer (separate group)
	consumer, err := kafkalib.NewConsumer(cfg.Kafka, cfg.Kafka.GroupDetection,
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
		logger.Info("shutting down detection service")
		cancel()
	}()

	logger.Info("detection service started",
		zap.String("group", cfg.Kafka.GroupDetection),
		zap.Int("window_minutes", cfg.Detection.WindowMinutes),
		zap.Float64("error_rate_threshold", cfg.Detection.ErrorRateThreshold))

	if err := consumer.PollLoop(ctx, ds.Process); err != nil && err != context.Canceled {
		logger.Fatal("consumer loop failed", zap.Error(err))
	}
}
