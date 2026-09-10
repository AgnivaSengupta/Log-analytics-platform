package detection

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/log-analytics-platform/internal/models"
)

// WindowState tracks event counts within a sliding window.
type WindowState struct {
	TotalEvents int64
	ErrorEvents int64
	StartTime   time.Time
	Service     string
	LastAlertAt time.Time
}

// Engine is the real-time detection engine.
type Engine struct {
	mu             sync.RWMutex
	windows        map[string]*WindowState // key: service
	fingerprints   map[string]*FingerprintEntry
	alertThreshold float64
	windowDuration time.Duration
	minEvents      int
	alertCallbacks []func(alert models.Alert)
}

// FingerprintEntry tracks recurring error patterns.
type FingerprintEntry struct {
	Fingerprint string
	Service     string
	Template    string
	Count       int64
	FirstSeen   time.Time
	LastSeen    time.Time
	AlertedAt   time.Time
}

// NewEngine creates a new detection engine.
func NewEngine(windowMinutes int, errorRateThreshold float64, minEvents int) *Engine {
	e := &Engine{
		windows:        make(map[string]*WindowState),
		fingerprints:   make(map[string]*FingerprintEntry),
		alertThreshold: errorRateThreshold,
		windowDuration: time.Duration(windowMinutes) * time.Minute,
		minEvents:      minEvents,
	}

	// Start window cleanup goroutine
	go e.cleanupLoop()

	return e
}

// OnAlert registers a callback for when alerts fire.
func (e *Engine) OnAlert(callback func(alert models.Alert)) {
	e.alertCallbacks = append(e.alertCallbacks, callback)
}

// ProcessEvent processes an incoming event for detection.
func (e *Engine) ProcessEvent(event models.LogEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := event.Service
	window, exists := e.windows[key]
	if !exists || time.Since(window.StartTime) > e.windowDuration {
		window = &WindowState{
			StartTime: time.Now(),
			Service:   event.Service,
		}
		e.windows[key] = window
	}

	window.TotalEvents++
	if event.Severity == "ERROR" || event.Severity == "FATAL" || event.Severity == "CRITICAL" {
		window.ErrorEvents++

		// Track fingerprint for recurring errors
		e.trackFingerprint(event)
	}

	// Check alert conditions
	e.checkAlerts(window, event)
}

// checkAlerts evaluates whether alert conditions are met.
func (e *Engine) checkAlerts(window *WindowState, event models.LogEvent) {
	if window.TotalEvents < int64(e.minEvents) {
		return
	}
	errorRate := float64(window.ErrorEvents) / float64(window.TotalEvents)
	if errorRate >= e.alertThreshold && time.Since(window.LastAlertAt) > 15*time.Minute {
		alert := models.Alert{
			ID:          fmt.Sprintf("alert-%s-%d", window.Service, time.Now().UnixNano()),
			RuleID:      "error-rate-threshold",
			Service:     window.Service,
			Fingerprint: fmt.Sprintf("error_rate_%s", window.Service),
			Severity:    "WARNING",
			Value:       errorRate,
			Message:     fmt.Sprintf("Error rate %.2f%% exceeds threshold %.2f%% for service %s", errorRate*100, e.alertThreshold*100, window.Service),
			TriggeredAt: time.Now(),
			Status:      "firing",
		}
		window.LastAlertAt = time.Now()
		e.fireAlert(alert)
	}
}

// trackFingerprint creates a normalized fingerprint for error messages.
func (e *Engine) trackFingerprint(event models.LogEvent) {
	template := normalizeMessage(event.Message)
	fpKey := fmt.Sprintf("%s:%s", event.Service, fingerprint(template))

	entry, exists := e.fingerprints[fpKey]
	if !exists {
		entry = &FingerprintEntry{
			Fingerprint: fpKey,
			Service:     event.Service,
			Template:    template,
			FirstSeen:   time.Now(),
		}
		e.fingerprints[fpKey] = entry
	}

	entry.Count++
	entry.LastSeen = time.Now()

	// Alert if this fingerprint has occurred more than 100 times in the window
	// and hasn't been alerted recently
	if entry.Count > 100 && time.Since(entry.AlertedAt) > 15*time.Minute {
		alert := models.Alert{
			ID:          fmt.Sprintf("alert-fp-%s-%d", fpKey[:16], time.Now().UnixNano()),
			RuleID:      "recurring-error",
			Service:     event.Service,
			Fingerprint: fpKey,
			Severity:    "ERROR",
			Value:       float64(entry.Count),
			Message:     fmt.Sprintf("Recurring error pattern detected %d times: %s", entry.Count, entry.Template),
			TriggeredAt: time.Now(),
			Status:      "firing",
		}
		entry.AlertedAt = time.Now()
		e.fireAlert(alert)
	}
}

// fireAlert sends the alert to all registered callbacks.
func (e *Engine) fireAlert(alert models.Alert) {
	for _, cb := range e.alertCallbacks {
		cb(alert)
	}
}

// cleanupLoop periodically resets windows that have expired.
func (e *Engine) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		e.mu.Lock()
		for key, window := range e.windows {
			if time.Since(window.StartTime) > e.windowDuration*2 {
				delete(e.windows, key)
			}
		}

		// Cleanup old fingerprints
		for key, fp := range e.fingerprints {
			if time.Since(fp.LastSeen) > e.windowDuration*4 {
				delete(e.fingerprints, key)
			}
		}
		e.mu.Unlock()
	}
}

// GetWindows returns the current window states (for metrics/diagnostics).
func (e *Engine) GetWindows() map[string]WindowState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string]WindowState)
	for k, v := range e.windows {
		result[k] = *v
	}
	return result
}

// Fingerprint patterns, compiled once: normalizeMessage runs on every error
// event, so per-call compilation would dominate detection CPU during spikes.
var (
	uuidPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	ipPattern   = regexp.MustCompile(`\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`)
	hexPattern  = regexp.MustCompile(`[0-9a-f]{8,}`)
	numPattern  = regexp.MustCompile(`\b\d+\b`)
)

// normalizeMessage replaces variable parts of error messages with placeholders.
func normalizeMessage(msg string) string {
	msg = uuidPattern.ReplaceAllString(msg, "<UUID>")
	msg = ipPattern.ReplaceAllString(msg, "<IP>")
	msg = hexPattern.ReplaceAllString(msg, "<HEX>")
	msg = numPattern.ReplaceAllString(msg, "<N>")
	return msg
}

// fingerprint generates an MD5 fingerprint of a normalized message.
func fingerprint(template string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(template)))
}
