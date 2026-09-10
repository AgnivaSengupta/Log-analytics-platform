package models

import (
	"encoding/json"
	"strings"
	"time"
)

// LogEvent is the canonical event contract across the platform.
type LogEvent struct {
	EventID    string                 `json:"event_id"`
	Timestamp  time.Time              `json:"timestamp"`
	Service    string                 `json:"service"`
	Severity   string                 `json:"severity"`
	Message    string                 `json:"message"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	TraceID    string                 `json:"trace_id,omitempty"`
	Source     string                 `json:"source,omitempty"`
	Region     string                 `json:"region,omitempty"`
	Version    string                 `json:"version,omitempty"`
}


// NormalizeSeverity maps producer-specific severity strings onto the
// platform's canonical levels. Unknown non-empty values are kept as-is
// (uppercased); empty input becomes INFO.
func NormalizeSeverity(sev string) string {
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

// RawEvent represents the original event before normalization.
type RawEvent struct {
	EventID   string          `json:"event_id"`
	Timestamp time.Time       `json:"timestamp"`
	Raw       json.RawMessage `json:"raw"`
	Service   string          `json:"service"`
	Source    string          `json:"source"`
}

// IngestRequest is what producers POST to the gateway.
type IngestRequest struct {
	Events []LogEvent `json:"events"`
}

// IngestResponse is the gateway's acknowledgement.
type IngestResponse struct {
	Accepted int    `json:"accepted"`
	Failed   int    `json:"failed"`
	Message  string `json:"message,omitempty"`
}

// QueryRequest represents a search/analytics query.
type QueryRequest struct {
	Service   string    `json:"service"`
	Severity  string    `json:"severity,omitempty"`
	Search    string    `json:"search,omitempty"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Limit     int       `json:"limit"`
	Offset    int       `json:"offset"`
	AggType   string    `json:"agg_type,omitempty"` // count, timeline, top_errors
}

// QueryResponse wraps results from the query coordinator.
type QueryResponse struct {
	Results    []LogEvent  `json:"results,omitempty"`
	Aggregates interface{} `json:"aggregates,omitempty"`
	Total      int64       `json:"total"`
	Source     string      `json:"source"` // "hot", "cold", "merged"
	Cursor     string      `json:"cursor,omitempty"`
}

// AlertRule defines a detection rule.
type AlertRule struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	Service         string  `json:"service"`
	Condition       string  `json:"condition"` // error_rate, event_volume, latency_p99
	Threshold       float64 `json:"threshold"`
	WindowMinutes   int     `json:"window_minutes"`
	MinEvents       int     `json:"min_events"`
	CooldownMinutes int     `json:"cooldown_minutes"`
	Enabled         bool    `json:"enabled"`
}

// Alert represents a triggered alert.
type Alert struct {
	ID          string    `json:"id"`
	RuleID      string    `json:"rule_id"`
	Service     string    `json:"service"`
	Fingerprint string    `json:"fingerprint"`
	Severity    string    `json:"severity"`
	Value       float64   `json:"value"`
	Message     string    `json:"message"`
	TriggeredAt time.Time `json:"triggered_at"`
	Status      string    `json:"status"` // firing, resolved
}

// DeadLetterEvent wraps an event that failed processing.
type DeadLetterEvent struct {
	EventID   string    `json:"event_id"`
	Raw       string    `json:"raw"`
	Error     string    `json:"error"`
	FailedAt  time.Time `json:"failed_at"`
	Component string    `json:"component"`
}
