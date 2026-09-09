package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	gatewayURL    = flag.String("gateway", "http://localhost:8080", "Gateway URL")
	targetRate    = flag.Int("rate", 10000, "Target events per second")
	duration      = flag.Duration("duration", 30*time.Second, "Test duration")
	batchSize     = flag.Int("batch", 100, "Batch size per request")
	errorRate     = flag.Float64("error-rate", 0.002, "Error rate (0.0-1.0)")
	numWorkers    = flag.Int("workers", 10, "Number of concurrent workers")
	stepMode      = flag.Bool("step", false, "Step mode: 10K → 25K → 50K → 100K")
	reportPath    = flag.String("report", "", "Write JSON report to this file (default: print to stdout)")
)

var services = []string{
	"payment", "auth", "inventory", "shipping", "notification",
	"user-service", "order-service", "search", "recommendation", "analytics",
}

var severities = []string{"INFO", "INFO", "INFO", "INFO", "DEBUG", "WARNING", "ERROR"}

var messages = []string{
	"request processed successfully",
	"user authenticated",
	"payment authorization timed out",
	"database connection pool exhausted",
	"cache miss for key",
	"inventory item updated",
	"order placed successfully",
	"shipping label generated",
	"notification sent to user",
	"search index updated",
	"rate limit exceeded for client",
	"TLS handshake failed",
	"upstream service unavailable",
	"circuit breaker opened",
	"retry attempt %d for operation",
	"memory usage above threshold",
	"disk write latency spike detected",
	"configuration reloaded",
	"health check passed",
	"garbage collection completed",
}

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

type IngestRequest struct {
	Events []LogEvent `json:"events"`
}

type IngestResponse struct {
	Accepted int    `json:"accepted"`
	Failed   int    `json:"failed"`
	Message  string `json:"message,omitempty"`
}

type Metrics struct {
	TotalSent     atomic.Int64
	TotalAccepted atomic.Int64
	TotalRejected atomic.Int64
	TotalFailed   atomic.Int64
	Latencies     sync.Map
}

func generateEvent(rng *rand.Rand, currentErrorRate float64) LogEvent {
	service := services[rng.Intn(len(services))]
	severity := severities[rng.Intn(len(severities))]

	// Override severity based on error rate
	if rng.Float64() < currentErrorRate {
		severity = "ERROR"
	}

	msg := messages[rng.Intn(len(messages))]

	event := LogEvent{
		EventID:   uuid.New().String(),
		Timestamp: time.Now(),
		Service:   service,
		Severity:  severity,
		Message:   msg,
		Attributes: map[string]interface{}{
			"http.status_code": pickStatusCode(rng, severity),
			"http.method":      pickMethod(rng),
			"request.duration_ms": rng.Intn(5000),
		},
		Source:  fmt.Sprintf("load-gen-%d", rng.Intn(10)),
		Region:  "us-east-1",
		Version: "1.0.0",
	}

	if severity == "ERROR" {
		event.TraceID = uuid.New().String()[:16]
	}

	return event
}

func pickStatusCode(rng *rand.Rand, severity string) int {
	if severity == "ERROR" {
		codes := []int{500, 502, 503, 504, 429}
		return codes[rng.Intn(len(codes))]
	}
	if severity == "WARNING" {
		return 408
	}
	codes := []int{200, 200, 200, 201, 204, 301}
	return codes[rng.Intn(len(codes))]
}

func pickMethod(rng *rand.Rand) string {
	methods := []string{"GET", "GET", "GET", "POST", "PUT", "DELETE"}
	return methods[rng.Intn(len(methods))]
}

func runWorker(id int, rate int, batchSz int, targetErrRate float64, stop <-chan struct{}, metrics *Metrics) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	client := &http.Client{Timeout: 10 * time.Second}

	// Calculate interval between batches
	eventsPerWorker := float64(rate) / float64(*numWorkers)
	batchesPerSec := eventsPerWorker / float64(batchSz)
	interval := time.Duration(float64(time.Second) / batchesPerSec)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			events := make([]LogEvent, batchSz)
			for i := 0; i < batchSz; i++ {
				events[i] = generateEvent(rng, targetErrRate)
			}

			req := IngestRequest{Events: events}
			data, _ := json.Marshal(req)

			start := time.Now()
			resp, err := client.Post(*gatewayURL+"/v1/ingest", "application/json", bytes.NewReader(data))
			latency := time.Since(start)

			if err != nil {
				metrics.TotalFailed.Add(int64(batchSz))
				continue
			}

			var ingestResp IngestResponse
			json.NewDecoder(resp.Body).Decode(&ingestResp)
			resp.Body.Close()

			metrics.TotalSent.Add(int64(batchSz))
			metrics.TotalAccepted.Add(int64(ingestResp.Accepted))
			metrics.TotalRejected.Add(int64(ingestResp.Failed))

			// Store latency
			bucket := fmt.Sprintf("p%d", id)
			if existing, ok := metrics.Latencies.Load(bucket); ok {
				latencies := existing.([]time.Duration)
				latencies = append(latencies, latency)
				if len(latencies) > 1000 {
					latencies = latencies[len(latencies)-1000:]
				}
				metrics.Latencies.Store(bucket, latencies)
			} else {
				metrics.Latencies.Store(bucket, []time.Duration{latency})
			}
		}
	}
}

type LatencyStats struct {
	Samples int     `json:"samples"`
	MinMs   float64 `json:"min_ms"`
	AvgMs   float64 `json:"avg_ms"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
}

type StepResult struct {
	Name          string       `json:"name"`
	TargetRate    int          `json:"target_rate"`
	DurationSec   float64      `json:"duration_sec"`
	Sent          int64        `json:"sent"`
	Accepted      int64        `json:"accepted"`
	Rejected      int64        `json:"rejected"`
	Failed        int64        `json:"failed"`
	ActualRate    float64      `json:"actual_rate"`
	AcceptedRate  float64      `json:"accepted_rate"`
	AcceptancePct float64      `json:"acceptance_pct"`
	Latency       LatencyStats `json:"latency"`
}

type Report struct {
	Gateway   string       `json:"gateway"`
	Timestamp time.Time    `json:"timestamp"`
	Steps     []StepResult `json:"steps"`
}

func latencyStats(metrics *Metrics) LatencyStats {
	var all []float64
	metrics.Latencies.Range(func(_, v interface{}) bool {
		if latencies, ok := v.([]time.Duration); ok {
			for _, d := range latencies {
				all = append(all, float64(d.Microseconds())/1000.0)
			}
		}
		return true
	})
	stats := LatencyStats{Samples: len(all)}
	if len(all) == 0 {
		return stats
	}
	sort.Float64s(all)
	var sum float64
	for _, ms := range all {
		sum += ms
	}
	stats.MinMs = all[0]
	stats.MaxMs = all[len(all)-1]
	stats.AvgMs = sum / float64(len(all))
	stats.P50Ms = all[int(float64(len(all))*0.50)]
	stats.P95Ms = all[int(float64(len(all))*0.95)]
	stats.P99Ms = all[int(float64(len(all))*0.99)]
	return stats
}

func stepResult(name string, rate int, metrics *Metrics, startTime time.Time) StepResult {
	elapsed := time.Since(startTime).Seconds()
	sent := metrics.TotalSent.Load()
	accepted := metrics.TotalAccepted.Load()
	res := StepResult{
		Name:         name,
		TargetRate:   rate,
		DurationSec:  elapsed,
		Sent:         sent,
		Accepted:     accepted,
		Rejected:     metrics.TotalRejected.Load(),
		Failed:       metrics.TotalFailed.Load(),
		ActualRate:   float64(sent) / elapsed,
		AcceptedRate: float64(accepted) / elapsed,
		Latency:      latencyStats(metrics),
	}
	if sent > 0 {
		res.AcceptancePct = float64(accepted) / float64(sent) * 100
	}
	return res
}

func writeReport(report Report) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Printf("failed to encode report: %v\n", err)
		return
	}
	if *reportPath != "" {
		if err := os.WriteFile(*reportPath, data, 0644); err != nil {
			fmt.Printf("failed to write report to %s: %v\n", *reportPath, err)
			return
		}
		fmt.Printf("JSON report written to %s\n", *reportPath)
		return
	}
	fmt.Printf("\n--- JSON report ---\n%s\n", string(data))
}

func printMetrics(metrics *Metrics, startTime time.Time, rate int) {
	elapsed := time.Since(startTime).Seconds()
	sent := metrics.TotalSent.Load()
	accepted := metrics.TotalAccepted.Load()
	rejected := metrics.TotalRejected.Load()
	failed := metrics.TotalFailed.Load()

	fmt.Printf("\n═══════════════════════════════════════════════\n")
	fmt.Printf("  LOAD TEST RESULTS (target: %d events/sec)\n", rate)
	fmt.Printf("═══════════════════════════════════════════════\n")
	fmt.Printf("  Duration:           %.1f seconds\n", elapsed)
	fmt.Printf("  Total Sent:         %d\n", sent)
	fmt.Printf("  Total Accepted:     %d\n", accepted)
	fmt.Printf("  Total Rejected:     %d\n", rejected)
	fmt.Printf("  Total Failed:       %d\n", failed)
	fmt.Printf("  Actual Rate:        %.0f events/sec\n", float64(sent)/elapsed)
	fmt.Printf("  Accepted Rate:      %.0f events/sec\n", float64(accepted)/elapsed)
	if sent > 0 {
		fmt.Printf("  Acceptance Rate:    %.2f%%\n", float64(accepted)/float64(sent)*100)
	}
	lat := latencyStats(metrics)
	fmt.Printf("  Latency (ms):       p50=%.1f p95=%.1f p99=%.1f avg=%.1f (n=%d)\n", lat.P50Ms, lat.P95Ms, lat.P99Ms, lat.AvgMs, lat.Samples)
	fmt.Printf("═══════════════════════════════════════════════\n\n")
}

func runTest(rate int, dur time.Duration, errRate float64, metrics *Metrics) time.Time {
	stop := make(chan struct{})
	start := time.Now()

	fmt.Printf("Starting load test: %d events/sec for %v (error rate: %.2f%%)\n",
		rate, dur, errRate*100)

	for i := 0; i < *numWorkers; i++ {
		go runWorker(i, rate, *batchSize, errRate, stop, metrics)
	}

	// Progress reporter
	progressTicker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-progressTicker.C:
				sent := metrics.TotalSent.Load()
				elapsed := time.Since(start).Seconds()
				fmt.Printf("  [%.0fs] sent: %d (%.0f/sec)\n",
					elapsed, sent, float64(sent)/elapsed)
			}
		}
	}()

	time.Sleep(dur)
	close(stop)
	progressTicker.Stop()
	time.Sleep(2 * time.Second) // Wait for in-flight requests
	return start
}

func main() {
	flag.Parse()

	fmt.Println("╔═══════════════════════════════════════════════╗")
	fmt.Println("║   Distributed Log Analytics - Load Generator  ║")
	fmt.Println("╚═══════════════════════════════════════════════╝")
	fmt.Printf("Gateway: %s\n", *gatewayURL)

	if *stepMode {
		steps := []struct {
			name string
			rate int
			dur  time.Duration
		}{
			{"10K", 10000, 30 * time.Second},
			{"25K", 25000, 30 * time.Second},
			{"50K", 50000, 60 * time.Second},
			{"100K", 100000, 60 * time.Second},
		}

		report := Report{Gateway: *gatewayURL, Timestamp: time.Now()}
		for _, step := range steps {
			metrics := &Metrics{}
			startTime := runTest(step.rate, step.dur, *errorRate, metrics)
			printMetrics(metrics, startTime, step.rate)
			report.Steps = append(report.Steps, stepResult(step.name, step.rate, metrics, startTime))
			time.Sleep(5 * time.Second) // Cool down between steps
		}
		writeReport(report)
	} else {
		metrics := &Metrics{}
		startTime := runTest(*targetRate, *duration, *errorRate, metrics)
		printMetrics(metrics, startTime, *targetRate)
		writeReport(Report{
			Gateway:   *gatewayURL,
			Timestamp: startTime,
			Steps:     []StepResult{stepResult("single", *targetRate, metrics, startTime)},
		})
	}
}
