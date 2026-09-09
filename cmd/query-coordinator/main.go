package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
	"github.com/log-analytics-platform/internal/storage"
	"github.com/log-analytics-platform/internal/tinybird"
)

var (
	queryLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "query_coordinator_latency_seconds",
		Help:    "Query latency by source",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"source"})

	queryCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "query_coordinator_queries_total",
		Help: "Total queries by source",
	}, []string{"source"})
)

func init() {
	prometheus.MustRegister(queryLatency, queryCount)
}

// QueryCoordinator routes hot queries to Tinybird and old data to object storage.
type QueryCoordinator struct {
	cfg      *config.Config
	tinybird *tinybird.Client
	s3       *storage.S3Client
	logger   *zap.Logger
}

func NewQueryCoordinator(cfg *config.Config, logger *zap.Logger) (*QueryCoordinator, error) {
	if cfg.Tinybird.ReadToken == "" {
		return nil, fmt.Errorf("TINYBIRD_READ_TOKEN must be configured")
	}
	tb, err := tinybird.NewClient(cfg.Tinybird, logger)
	if err != nil {
		return nil, fmt.Errorf("tinybird: %w", err)
	}

	s3Client, err := storage.NewS3Client(cfg.S3, logger)
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}

	return &QueryCoordinator{
		cfg:      cfg,
		tinybird: tb,
		s3:       s3Client,
		logger:   logger,
	}, nil
}

func sqlLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func (qc *QueryCoordinator) hotWhere(req models.QueryRequest) string {
	parts := []string{
		"timestamp >= " + sqlLiteral(req.StartTime.UTC().Format("2006-01-02 15:04:05.000")),
		"timestamp <= " + sqlLiteral(req.EndTime.UTC().Format("2006-01-02 15:04:05.000")),
	}
	if req.Service != "" {
		parts = append(parts, "service = "+sqlLiteral(req.Service))
	}
	if req.Severity != "" {
		parts = append(parts, "severity = "+sqlLiteral(req.Severity))
	}
	if req.Search != "" {
		parts = append(parts, "positionCaseInsensitive(message, "+sqlLiteral(req.Search)+") > 0")
	}
	return " WHERE " + strings.Join(parts, " AND ")
}

func (qc *QueryCoordinator) hotQuery(ctx context.Context, req models.QueryRequest) ([]models.LogEvent, int64, error) {
	where := qc.hotWhere(req)
	countRows, err := qc.tinybird.Query(ctx, "SELECT count() AS total FROM "+qc.tinybird.Datasource()+where)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if len(countRows) > 0 {
		_ = json.Unmarshal(countRows[0]["total"], &total)
	}
	query := fmt.Sprintf("SELECT event_id, timestamp, service, severity, message, attributes, trace_id, source, region, version FROM %s%s ORDER BY timestamp DESC LIMIT %d OFFSET %d", qc.tinybird.Datasource(), where, req.Limit, req.Offset)
	rows, err := qc.tinybird.Query(ctx, query)
	if err != nil {
		return nil, 0, err
	}
	results := make([]models.LogEvent, 0, len(rows))
	for _, row := range rows {
		var ev models.LogEvent
		_ = json.Unmarshal(row["event_id"], &ev.EventID)
		_ = json.Unmarshal(row["service"], &ev.Service)
		_ = json.Unmarshal(row["severity"], &ev.Severity)
		_ = json.Unmarshal(row["message"], &ev.Message)
		_ = json.Unmarshal(row["trace_id"], &ev.TraceID)
		_ = json.Unmarshal(row["source"], &ev.Source)
		_ = json.Unmarshal(row["region"], &ev.Region)
		_ = json.Unmarshal(row["version"], &ev.Version)
		if rawAttributes := row["attributes"]; rawAttributes != nil {
			var attrs string
			if json.Unmarshal(rawAttributes, &attrs) == nil {
				_ = json.Unmarshal([]byte(attrs), &ev.Attributes)
			} else {
				_ = json.Unmarshal(rawAttributes, &ev.Attributes)
			}
		}
		var rawTimestamp string
		if err := json.Unmarshal(row["timestamp"], &rawTimestamp); err != nil {
			return nil, 0, fmt.Errorf("decode timestamp: %w", err)
		}
		var parseErr error
		ev.Timestamp, parseErr = time.Parse("2006-01-02 15:04:05.000", rawTimestamp)
		if parseErr != nil {
			ev.Timestamp, parseErr = time.Parse(time.RFC3339Nano, rawTimestamp)
		}
		if parseErr != nil {
			return nil, 0, fmt.Errorf("parse timestamp: %w", parseErr)
		}
		results = append(results, ev)
	}
	return results, total, nil
}

func (qc *QueryCoordinator) hotTimeline(ctx context.Context, service string, start, end time.Time, seconds int) ([]map[string]interface{}, error) {
	req := models.QueryRequest{Service: service, StartTime: start, EndTime: end}
	query := fmt.Sprintf("SELECT toStartOfInterval(timestamp, INTERVAL %d SECOND) AS bucket, count() AS count, countIf(severity = 'ERROR') AS error_count FROM %s%s GROUP BY bucket ORDER BY bucket", seconds, qc.tinybird.Datasource(), qc.hotWhere(req))
	rows, err := qc.tinybird.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		entry := map[string]interface{}{}
		for key, raw := range row {
			var value interface{}
			_ = json.Unmarshal(raw, &value)
			entry[key] = value
		}
		result = append(result, entry)
	}
	return result, nil
}

func (qc *QueryCoordinator) hotAggregateCount(ctx context.Context, start, end time.Time) (map[string]map[string]int64, error) {
	rows, err := qc.tinybird.Query(ctx, "SELECT service, severity, count() AS count FROM "+qc.tinybird.Datasource()+qc.hotWhere(models.QueryRequest{StartTime: start, EndTime: end})+" GROUP BY service, severity")
	if err != nil {
		return nil, err
	}
	result := make(map[string]map[string]int64)
	for _, row := range rows {
		var service, severity string
		var count int64
		_ = json.Unmarshal(row["service"], &service)
		_ = json.Unmarshal(row["severity"], &severity)
		_ = json.Unmarshal(row["count"], &count)
		if result[service] == nil {
			result[service] = map[string]int64{}
		}
		result[service][severity] = count
	}
	return result, nil
}

// handleSearch handles search queries, routing to hot or cold storage.
// Results from both tiers are merge-sorted by timestamp (descending)
// and paginated globally so that the API behaves consistently regardless
// of which storage backends are involved.
func (qc *QueryCoordinator) handleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req models.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 10000 {
		req.Limit = 10000
	}
	if req.EndTime.IsZero() {
		req.EndTime = time.Now()
	}
	if req.StartTime.IsZero() {
		req.StartTime = req.EndTime.Add(-24 * time.Hour)
	}

	ctx := r.Context()
	hotCutoff := time.Now().Add(-time.Duration(qc.cfg.Query.HotRetentionDays) * 24 * time.Hour)

	filter := storage.QueryFilter{
		Service:  req.Service,
		Severity: req.Severity,
		Search:   req.Search,
	}

	var results []models.LogEvent
	var total int64
	var source string

	switch {
	case req.EndTime.Before(hotCutoff):
		// Query cold storage only — scan all matching events, sort, then paginate
		coldResults, err := qc.s3.QueryEvents(ctx, filter, req.StartTime, req.EndTime)
		if err != nil {
			qc.logger.Error("cold query failed", zap.Error(err))
			http.Error(w, `{"error":"cold storage query failed"}`, http.StatusInternalServerError)
			return
		}
		// Sort descending by timestamp for consistent ordering
		sort.Slice(coldResults, func(i, j int) bool {
			return coldResults[i].Timestamp.After(coldResults[j].Timestamp)
		})
		total = int64(len(coldResults))
		results = paginate(coldResults, req.Offset, req.Limit)
		source = "cold"
		queryCount.WithLabelValues("cold").Inc()

	case req.StartTime.After(hotCutoff):
		// Query hot storage only
		hotResults, hotTotal, err := qc.hotQuery(ctx, req)
		if err != nil {
			qc.logger.Error("hot query failed", zap.Error(err))
			http.Error(w, `{"error":"hot storage query failed"}`, http.StatusInternalServerError)
			return
		}
		results = hotResults
		total = hotTotal
		source = "hot"
		queryCount.WithLabelValues("hot").Inc()

	default:
		// Query both tiers. Fetch all matching events from cold storage,
		// then merge with hot results, sort globally, and paginate.
		fetchLimit := req.Offset + req.Limit

		hotResults, hotTotal, err := qc.hotQuery(ctx, models.QueryRequest{
			Service:   req.Service,
			Severity:  req.Severity,
			Search:    req.Search,
			StartTime: hotCutoff,
			EndTime:   req.EndTime,
			Limit:     fetchLimit,
			Offset:    0,
		})
		if err != nil {
			qc.logger.Error("hot query failed", zap.Error(err))
		}

		coldResults, err := qc.s3.QueryEvents(ctx, filter, req.StartTime, hotCutoff)
		if err != nil {
			qc.logger.Error("cold query failed", zap.Error(err))
		}

		// Merge both result sets
		merged := make([]models.LogEvent, 0, len(hotResults)+len(coldResults))
		merged = append(merged, hotResults...)
		merged = append(merged, coldResults...)

		// Global sort: newest first
		sort.Slice(merged, func(i, j int) bool {
			return merged[i].Timestamp.After(merged[j].Timestamp)
		})

		total = hotTotal + int64(len(coldResults))
		results = paginate(merged, req.Offset, req.Limit)
		source = "merged"
		queryCount.WithLabelValues("merged").Inc()
	}

	queryLatency.WithLabelValues(source).Observe(time.Since(start).Seconds())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.QueryResponse{
		Results: results,
		Total:   total,
		Source:  source,
	})
}

// paginate applies offset/limit to a slice.
func paginate(events []models.LogEvent, offset, limit int) []models.LogEvent {
	if offset >= len(events) {
		return nil
	}
	end := offset + limit
	if end > len(events) {
		end = len(events)
	}
	return events[offset:end]
}

// handleTimeline returns time-bucketed event counts.
func (qc *QueryCoordinator) handleTimeline(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	hoursStr := r.URL.Query().Get("hours")
	hours := 24
	if h := hoursStr; h != "" {
		fmt.Sscanf(h, "%d", &hours)
	}

	startTime := time.Now().Add(-time.Duration(hours) * time.Hour)
	endTime := time.Now()

	bucketSeconds := 300 // 5 minutes
	if hours > 168 {     // > 7 days
		bucketSeconds = 3600 // 1 hour
	}

	ctx := r.Context()
	timeline, err := qc.hotTimeline(ctx, service, startTime, endTime, bucketSeconds)
	if err != nil {
		qc.logger.Error("timeline query failed", zap.Error(err))
		http.Error(w, `{"error":"timeline query failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timeline": timeline,
	})
}

// handleAggregates returns aggregated metrics.
func (qc *QueryCoordinator) handleAggregates(w http.ResponseWriter, r *http.Request) {
	hoursStr := r.URL.Query().Get("hours")
	hours := 24
	if h := hoursStr; h != "" {
		fmt.Sscanf(h, "%d", &hours)
	}

	startTime := time.Now().Add(-time.Duration(hours) * time.Hour)
	endTime := time.Now()

	ctx := r.Context()
	counts, err := qc.hotAggregateCount(ctx, startTime, endTime)
	if err != nil {
		qc.logger.Error("aggregate query failed", zap.Error(err))
		http.Error(w, `{"error":"aggregate query failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"aggregates": counts,
	})
}

// handleHealth returns service health.
func (qc *QueryCoordinator) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (qc *QueryCoordinator) Close() {
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	qc, err := NewQueryCoordinator(cfg, logger)
	if err != nil {
		logger.Fatal("query coordinator init failed", zap.Error(err))
	}
	defer qc.Close()

	router := mux.NewRouter()
	router.HandleFunc("/v1/search", qc.handleSearch).Methods("POST")
	router.HandleFunc("/v1/timeline", qc.handleTimeline).Methods("GET")
	router.HandleFunc("/v1/aggregates", qc.handleAggregates).Methods("GET")
	router.HandleFunc("/health", qc.handleHealth).Methods("GET")
	router.Handle("/metrics", promhttp.Handler())

	handler := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders: []string{"Content-Type", "Authorization"},
	}).Handler(router)

	addr := fmt.Sprintf(":%d", cfg.Query.Port)
	logger.Info("query coordinator starting", zap.String("addr", addr))

	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down query coordinator")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("server failed", zap.Error(err))
	}
}
