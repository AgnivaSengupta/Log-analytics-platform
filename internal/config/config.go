package config

import (
	"os"
	"strconv"
)

type Config struct {
	Kafka     KafkaConfig
	ClickHouse ClickHouseConfig
	S3        S3Config
	Gateway   GatewayConfig
	Query     QueryConfig
	Detection DetectionConfig
	Alert     AlertConfig
	Worker    WorkerConfig
}

type KafkaConfig struct {
	Brokers        string
	TopicLogs      string
	TopicDLQ       string
	GroupWorkers   string
	GroupArchive   string
	GroupDetection string
}

type ClickHouseConfig struct {
	URL      string
	User     string
	Password string
	Database string
	Table    string
}

type S3Config struct {
	Endpoint     string
	Region       string
	Bucket       string
	AccessKey    string
	SecretKey    string
	EnsureBucket bool
}

type GatewayConfig struct {
	Port              int
	IngestQuotaPerSec int
}

type QueryConfig struct {
	Port             int
	HotRetentionDays int
	MaxScanBytes     int64
}

type DetectionConfig struct {
	WindowMinutes      int
	ErrorRateThreshold float64
	MinEvents          int
}

type AlertConfig struct {
	Port       int
	WebhookURL string
}

// WorkerConfig tunes the processing-worker append pipeline: batches seal every
// BatchSize events or FlushIntervalMs (whichever first), and up to
// AppendConcurrency ClickHouse inserts run in flight per worker process.
type WorkerConfig struct {
	BatchSize         int
	FlushIntervalMs   int
	AppendConcurrency int
}

func Load() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:        getEnv("KAFKA_BROKERS", "localhost:9092"),
			TopicLogs:      getEnv("KAFKA_TOPIC_LOGS", "logs"),
			TopicDLQ:       getEnv("KAFKA_TOPIC_DLQ", "logs-dlq"),
			GroupWorkers:   getEnv("KAFKA_GROUP_WORKERS", "processing-workers"),
			GroupArchive:   getEnv("KAFKA_GROUP_ARCHIVE", "archive-writers"),
			GroupDetection: getEnv("KAFKA_GROUP_DETECTION", "detection-service"),
		},
		ClickHouse: ClickHouseConfig{
			URL:      getEnv("CLICKHOUSE_URL", "http://localhost:8123"),
			User:     getEnv("CLICKHOUSE_USER", "default"),
			Password: getEnv("CLICKHOUSE_PASSWORD", ""),
			Database: getEnv("CLICKHOUSE_DATABASE", "default"),
			Table:    getEnv("CLICKHOUSE_TABLE", "logs"),
		},
		S3: S3Config{
			Endpoint:     getEnv("S3_ENDPOINT", ""),
			Region:       getEnv("S3_REGION", "auto"),
			Bucket:       getEnv("S3_BUCKET", "log-archive"),
			AccessKey:    getEnv("S3_ACCESS_KEY", ""),
			SecretKey:    getEnv("S3_SECRET_KEY", ""),
			EnsureBucket: getEnvBool("S3_ENSURE_BUCKET", false),
		},
		Gateway: GatewayConfig{
			Port:              getEnvInt("GATEWAY_PORT", 8080),
			IngestQuotaPerSec: getEnvInt("GATEWAY_INGEST_QUOTA_PER_SEC", 100000),
		},
		Query: QueryConfig{
			Port:             getEnvInt("QUERY_COORDINATOR_PORT", 8081),
			HotRetentionDays: getEnvInt("QUERY_HOT_RETENTION_DAYS", 30),
			MaxScanBytes:     int64(getEnvInt("QUERY_MAX_SCAN_BYTES", 1073741824)),
		},
		Detection: DetectionConfig{
			WindowMinutes:      getEnvInt("DETECTION_WINDOW_MINUTES", 5),
			ErrorRateThreshold: getEnvFloat("DETECTION_ERROR_RATE_THRESHOLD", 0.05),
			MinEvents:          getEnvInt("DETECTION_MIN_EVENTS", 100),
		},
		Alert: AlertConfig{
			Port:       getEnvInt("ALERT_SERVICE_PORT", 8082),
			WebhookURL: getEnv("ALERT_WEBHOOK_URL", ""),
		},
		Worker: WorkerConfig{
			BatchSize:         getEnvInt("WORKER_BATCH_SIZE", 2000),
			FlushIntervalMs:   getEnvInt("WORKER_FLUSH_INTERVAL_MS", 250),
			AppendConcurrency: getEnvInt("WORKER_APPEND_CONCURRENCY", 4),
		},
	}
}

func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}
