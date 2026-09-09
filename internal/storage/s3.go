package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
)

// S3Client talks to Cloudflare R2 over its S3-compatible API. The type and
// S3_* setting names refer to the API dialect, not to AWS: the endpoint is
// always the account-specific R2 endpoint.
type S3Client struct {
	client *s3.S3
	bucket string
	logger *zap.Logger
}

// NewS3Client creates a client for the Cloudflare R2 archive bucket. The
// endpoint must be the account-specific R2 S3 endpoint
// (https://<account-id>.r2.cloudflarestorage.com) and the region should stay
// "auto". The bucket and its API token are provisioned in the Cloudflare
// dashboard (see MANAGED_SERVICES_SETUP.md), not by this service.
func NewS3Client(cfg config.S3Config, logger *zap.Logger) (*S3Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("S3_ENDPOINT must be configured (https://<account-id>.r2.cloudflarestorage.com)")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET must be configured")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("S3_ACCESS_KEY and S3_SECRET_KEY must be configured with an R2 API token")
	}

	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(cfg.Region),
		Endpoint:         aws.String(cfg.Endpoint),
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create R2 session: %w", err)
	}

	return &S3Client{
		client: s3.New(sess),
		bucket: cfg.Bucket,
		logger: logger,
	}, nil
}

// EnsureBucket creates the bucket if it doesn't exist. R2 buckets are
// normally created in the Cloudflare dashboard, so production deployments
// leave S3_ENSURE_BUCKET=false and never call this.
func (s *S3Client) EnsureBucket(ctx context.Context) error {
	_, err := s.client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
		!strings.Contains(err.Error(), "BucketAlreadyExists") {
		return fmt.Errorf("create bucket failed: %w", err)
	}
	s.logger.Info("R2 bucket ready", zap.String("bucket", s.bucket))
	return nil
}

// ArchiveEvents writes a batch of events to object storage in a partitioned path.
func (s *S3Client) ArchiveEvents(ctx context.Context, events []models.LogEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Partition by date/hour for efficient retrieval
	firstEvent := events[0]
	partitionPath := fmt.Sprintf("raw/%s/%s/%s/",
		firstEvent.Timestamp.Format("2006/01/02"),
		firstEvent.Timestamp.Format("15"),
		firstEvent.Service,
	)

	// Create JSONL payload
	var buf bytes.Buffer
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	key := partitionPath + fmt.Sprintf("%s-%d.jsonl",
		firstEvent.EventID[:8], time.Now().UnixNano())

	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/x-ndjson"),
	})
	if err != nil {
		return fmt.Errorf("put object failed: %w", err)
	}

	s.logger.Debug("archived events to R2",
		zap.String("key", key),
		zap.Int("count", len(events)))

	return nil
}

// RawArchiveItem is one original, unmodified Kafka payload with the
// minimum metadata required for partitioning in object storage.
type RawArchiveItem struct {
	Timestamp time.Time
	Service   string
	Raw       []byte // original bytes — written verbatim
}

// ArchiveRawBatch writes a batch of raw, unmodified Kafka payloads to
// object storage as newline-delimited JSON. The original bytes are
// preserved exactly as received so the archive supports faithful replay
// with a future parser version.
func (s *S3Client) ArchiveRawBatch(ctx context.Context, ts time.Time, service string, items []RawArchiveItem) error {
	if len(items) == 0 {
		return nil
	}

	partitionPath := fmt.Sprintf("raw/%s/%s/%s/",
		ts.Format("2006/01/02"),
		ts.Format("15"),
		service,
	)

	var buf bytes.Buffer
	for _, item := range items {
		buf.Write(item.Raw)
		buf.WriteByte('\n')
	}

	key := partitionPath + fmt.Sprintf("batch-%d.jsonl", time.Now().UnixNano())

	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/x-ndjson"),
	})
	if err != nil {
		return fmt.Errorf("put raw batch failed: %w", err)
	}

	s.logger.Debug("archived raw batch to R2",
		zap.String("key", key),
		zap.Int("count", len(items)))

	return nil
}

// ArchiveRaw writes a single raw event to storage.
func (s *S3Client) ArchiveRaw(ctx context.Context, eventID string, service string, ts time.Time, data []byte) error {
	key := fmt.Sprintf("raw/%s/%s/%s/%s.json",
		ts.Format("2006/01/02"),
		ts.Format("15"),
		service,
		eventID,
	)

	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// QueryFilter specifies all filters for a cold-storage query.
type QueryFilter struct {
	Service  string
	Severity string
	Search   string
}

// QueryEvents reads events from object storage applying all provided filters.
// Scans ALL matching objects (up to maxScanItems) to ensure correct global
// ordering and pagination. Returns all matching events; caller is responsible
// for sorting and pagination.
const maxScanItems = 10000 // safety limit to prevent OOM on large datasets

func (s *S3Client) QueryEvents(ctx context.Context, filter QueryFilter, startTime, endTime time.Time) ([]models.LogEvent, error) {
	// Scan every daily partition in the requested interval. Restricting the
	// prefix to startTime's day silently omits all subsequent dates.
	prefixes := []string{"raw/"}
	if !startTime.IsZero() && !endTime.IsZero() {
		prefixes = prefixes[:0]
		day := time.Date(startTime.UTC().Year(), startTime.UTC().Month(), startTime.UTC().Day(), 0, 0, 0, 0, time.UTC)
		lastDay := time.Date(endTime.UTC().Year(), endTime.UTC().Month(), endTime.UTC().Day(), 0, 0, 0, 0, time.UTC)
		for !day.After(lastDay) {
			prefixes = append(prefixes, fmt.Sprintf("raw/%s/", day.Format("2006/01/02")))
			day = day.AddDate(0, 0, 1)
		}
	}

	var allEvents []models.LogEvent
	searchLower := strings.ToLower(filter.Search)
	scannedItems := 0

	scanLimitReached := false
	for _, prefix := range prefixes {
		if scanLimitReached {
			break
		}
		input := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)}
		err := s.client.ListObjectsV2PagesWithContext(ctx, input,
			func(page *s3.ListObjectsV2Output, lastPage bool) bool {
				for _, obj := range page.Contents {
					if scannedItems >= maxScanItems {
						s.logger.Warn("cold query scan limit reached",
							zap.Int("limit", maxScanItems),
							zap.Int("events_found", len(allEvents)))
						scanLimitReached = true
						return false
					}

					events, err := s.readObject(ctx, *obj.Key)
					if err != nil {
						s.logger.Warn("failed to read object", zap.String("key", *obj.Key), zap.Error(err))
						continue
					}
					scannedItems += len(events)

					for _, ev := range events {
						// Time filter
						if ev.Timestamp.Before(startTime) || ev.Timestamp.After(endTime) {
							continue
						}
						// Service filter
						if filter.Service != "" && ev.Service != filter.Service {
							continue
						}
						// Severity filter
						if filter.Severity != "" && !strings.EqualFold(ev.Severity, filter.Severity) {
							continue
						}
						// Free-text search filter (case-insensitive match on message)
						if searchLower != "" && !strings.Contains(strings.ToLower(ev.Message), searchLower) {
							continue
						}

						allEvents = append(allEvents, ev)
					}
				}
				return true
			})
		if err != nil {
			return nil, fmt.Errorf("list objects for %s failed: %w", prefix, err)
		}
	}
	if scanLimitReached {
		return nil, fmt.Errorf("cold query exceeds %d-event scan limit; narrow the time range or filters", maxScanItems)
	}

	s.logger.Debug("cold query scan complete",
		zap.Int("objects_scanned", scannedItems),
		zap.Int("events_matched", len(allEvents)))

	return allEvents, nil
}

func (s *S3Client) readObject(ctx context.Context, key string) ([]models.LogEvent, error) {
	output, err := s.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	var events []models.LogEvent
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var ev models.LogEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		events = append(events, ev)
	}

	return events, nil
}

// ListPartitions returns available date partitions for a service.
func (s *S3Client) ListPartitions(ctx context.Context, service string) ([]string, error) {
	prefix := "raw/"
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	var partitions []string
	output, err := s.client.ListObjectsV2WithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	for _, cp := range output.CommonPrefixes {
		partitions = append(partitions, path.Base(*cp.Prefix))
	}

	return partitions, nil
}
