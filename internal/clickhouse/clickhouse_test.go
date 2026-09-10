package clickhouse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
	"go.uber.org/zap"
)

func TestClickHouseTableRef(t *testing.T) {
	tests := []struct {
		db       string
		tbl      string
		expected string
	}{
		{"", "logs", "logs"},
		{"default", "logs", "logs"},
		{"custom_db", "logs", "custom_db.logs"},
	}

	for _, tc := range tests {
		c := &Client{Database: tc.db, Table: tc.tbl}
		if got := c.TableRef(); got != tc.expected {
			t.Errorf("TableRef() for db=%q, tbl=%q expected %q, got %q", tc.db, tc.tbl, tc.expected, got)
		}
	}
}

func TestAppendEvents(t *testing.T) {
	var receivedBody []byte
	var receivedAuthUser, receivedAuthKey string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthUser = r.Header.Get("X-ClickHouse-User")
		receivedAuthKey = r.Header.Get("X-ClickHouse-Key")
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-ClickHouse-Summary", `{"written_rows": "2"}`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := NewClient(config.ClickHouseConfig{
		URL:      srv.URL,
		User:     "logs_append",
		Password: "test_password",
		Database: "default",
		Table:    "logs",
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	events := []models.LogEvent{
		{
			EventID:    "evt-1",
			Timestamp:  time.Now(),
			Service:    "auth-service",
			Severity:   "INFO",
			Message:    "user logged in",
			Attributes: map[string]interface{}{"user_id": "123"},
		},
		{
			EventID:   "evt-2",
			Timestamp: time.Now(),
			Service:   "auth-service",
			Severity:  "ERROR",
			Message:   "login failed",
		},
	}

	err = client.AppendEvents(context.Background(), events)
	if err != nil {
		t.Fatalf("AppendEvents failed: %v", err)
	}

	if receivedAuthUser != "logs_append" {
		t.Errorf("expected user 'logs_append', got %q", receivedAuthUser)
	}
	if receivedAuthKey != "test_password" {
		t.Errorf("expected key 'test_password', got %q", receivedAuthKey)
	}
	if len(receivedBody) == 0 {
		t.Errorf("expected non-empty gzipped body")
	}
}

func TestQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := sqlAPIResponse{
			Data: []map[string]json.RawMessage{
				{
					"total": json.RawMessage(`100`),
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client, err := NewClient(config.ClickHouseConfig{
		URL:      srv.URL,
		User:     "logs_read",
		Password: "test_password",
		Database: "default",
		Table:    "logs",
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	data, err := client.Query(context.Background(), "SELECT count() as total FROM logs")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(data))
	}

	if string(data[0]["total"]) != "100" {
		t.Errorf("expected total=100, got %s", string(data[0]["total"]))
	}
}
