package main

import (
	"bytes"
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
	"github.com/log-analytics-platform/internal/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	alertsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "alert_service_received_total",
		Help: "Total alerts received",
	})

	alertsSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alert_service_sent_total",
		Help: "Total alerts sent by channel",
	}, []string{"channel", "status"})

	alertDedup = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "alert_service_dedup_total",
		Help: "Total alerts deduplicated",
	})
)

func init() {
	prometheus.MustRegister(alertsReceived, alertsSent, alertDedup)
}

// AlertService consumes alerts from Kafka and sends notifications.
type AlertService struct {
	cfg      *config.Config
	logger   *zap.Logger
	mu       sync.RWMutex           // protects dedup
	dedup    map[string]time.Time   // fingerprint -> last alert time
	cooldown time.Duration
}

func NewAlertService(cfg *config.Config, logger *zap.Logger) *AlertService {
	return &AlertService{
		cfg:      cfg,
		logger:   logger,
		dedup:    make(map[string]time.Time),
		cooldown: 15 * time.Minute,
	}
}

// Process handles a single alert message.
func (as *AlertService) Process(msg *kafka.Message) error {
	var alert models.Alert
	if err := json.Unmarshal(msg.Value, &alert); err != nil {
		as.logger.Warn("alert unmarshal error", zap.Error(err))
		return nil
	}

	alertsReceived.Inc()

	// Deduplicate: check under a read lock. We do NOT record the fingerprint
	// yet — that happens only after successful delivery, so a failed webhook
	// does not suppress retries for the cooldown period.
	as.mu.RLock()
	if lastSent, exists := as.dedup[alert.Fingerprint]; exists {
		if time.Since(lastSent) < as.cooldown {
			as.mu.RUnlock()
			alertDedup.Inc()
			as.logger.Debug("alert deduplicated",
				zap.String("fingerprint", alert.Fingerprint),
				zap.Time("last_sent", lastSent))
			return nil
		}
	}
	as.mu.RUnlock()

	// Send notification
	if err := as.sendNotification(alert); err != nil {
		as.logger.Error("notification failed — returning error so offset is NOT committed",
			zap.Error(err),
			zap.String("fingerprint", alert.Fingerprint))
		alertsSent.WithLabelValues("webhook", "failed").Inc()
		// Return the error so the shared consumer loop does NOT commit.
		// The alert will be re-delivered after rebalance or restart.
		return fmt.Errorf("webhook delivery: %w", err)
	}

	// Delivery succeeded — NOW record the fingerprint so subsequent
	// duplicates within the cooldown window are suppressed.
	as.mu.Lock()
	as.dedup[alert.Fingerprint] = time.Now()
	as.mu.Unlock()

	alertsSent.WithLabelValues("webhook", "success").Inc()
	return nil
}

// sendNotification dispatches the alert to configured channels.
func (as *AlertService) sendNotification(alert models.Alert) error {
	if as.cfg.Alert.WebhookURL == "" || as.cfg.Alert.WebhookURL == "PLACEHOLDER_ALERT_WEBHOOK_URL" {
		as.logger.Info("alert (webhook not configured, logging only)",
			zap.String("service", alert.Service),
			zap.String("rule", alert.RuleID),
			zap.String("message", alert.Message),
			zap.Float64("value", alert.Value))
		return nil
	}

	payload := map[string]interface{}{
		"alert_id":     alert.ID,
		"service":      alert.Service,
		"rule":         alert.RuleID,
		"severity":     alert.Severity,
		"message":      alert.Message,
		"value":        alert.Value,
		"fingerprint":  alert.Fingerprint,
		"triggered_at": alert.TriggeredAt,
		"status":       alert.Status,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", as.cfg.Alert.WebhookURL,
		bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	as.logger.Info("alert notification sent",
		zap.String("service", alert.Service),
		zap.String("rule", alert.RuleID))

	return nil
}

// cleanupDedup removes expired dedup entries under the write lock.
func (as *AlertService) cleanupDedup() {
	as.mu.Lock()
	defer as.mu.Unlock()
	for key, ts := range as.dedup {
		if time.Since(ts) > as.cooldown*2 {
			delete(as.dedup, key)
		}
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	svc := NewAlertService(cfg, logger)

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9094", mux)
	}()

	// Periodic cleanup
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			svc.cleanupDedup()
		}
	}()

	// Create consumer
	consumer, err := kafkalib.NewConsumer(cfg.Kafka, "alert-service",
		[]string{"alerts"}, logger)
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
		logger.Info("shutting down alert service")
		cancel()
	}()

	logger.Info("alert service started")

	if err := consumer.PollLoop(ctx, svc.Process); err != nil && err != context.Canceled {
		logger.Fatal("consumer loop failed", zap.Error(err))
	}
}
