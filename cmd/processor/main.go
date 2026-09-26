// Command processor receives events from Kafka, validates them against
// declarative JSON Schema contracts, and persists valid events to a Redis
// Stream for low-latency pickup by a downstream Sender service.
//
// Offsets are committed only after a message has been fully handled
// (persisted or dead-lettered), so a crash mid-message causes re-delivery
// on restart rather than silent loss.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"streaming-event-processor/internal/persister"
	"streaming-event-processor/internal/registry"
	"streaming-event-processor/internal/validator"
)

type config struct {
	kafkaBootstrapServers string
	kafkaTopic            string
	kafkaGroupID          string
	redisURL              string
	schemasDir            string
}

func configFromEnv() config {
	return config{
		kafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		kafkaTopic:            getEnv("KAFKA_TOPIC", "events"),
		kafkaGroupID:          getEnv("KAFKA_GROUP_ID", "event-processor-group"),
		redisURL:              getEnv("REDIS_URL", "redis://localhost:6379/0"),
		schemasDir:            getEnv("SCHEMAS_DIR", "schemas"),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := configFromEnv()

	reg, err := registry.New(cfg.schemasDir)
	if err != nil {
		log.Fatalf("loading schema registry: %v", err)
	}
	log.Printf("Event processor starting: topic=%s group=%s known_event_types=%v",
		cfg.kafkaTopic, cfg.kafkaGroupID, reg.KnownEventTypes())

	pst, err := persister.New(cfg.redisURL)
	if err != nil {
		log.Fatalf("connecting to redis: %v", err)
	}
	defer pst.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.kafkaBootstrapServers},
		Topic:          cfg.kafkaTopic,
		GroupID:        cfg.kafkaGroupID,
		CommitInterval: 0, // manual commits, see FetchMessage/CommitMessages below
		ErrorLogger:    kafka.LoggerFunc(log.Printf),
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("Event processor started")
	run(ctx, reader, pst, reg)
	log.Println("Event processor stopped")
}

func run(ctx context.Context, reader *kafka.Reader, pst *persister.RedisPersister, reg *registry.SchemaRegistry) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		handleMessage(ctx, msg.Value, pst, reg)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func handleMessage(ctx context.Context, rawBytes []byte, pst *persister.RedisPersister, reg *registry.SchemaRegistry) {
	var raw map[string]any
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		dead := map[string]any{"raw_bytes": string(rawBytes)}
		if dlqErr := pst.DeadLetter(ctx, dead, "invalid_json"); dlqErr != nil {
			log.Printf("dead-letter error: %v", dlqErr)
		}
		return
	}

	outcome := validator.ValidateRawEvent(raw, reg)
	if !outcome.Valid || outcome.Envelope == nil {
		reason := outcome.Reason
		if reason == "" {
			reason = "unknown_validation_failure"
		}
		if dlqErr := pst.DeadLetter(ctx, raw, reason); dlqErr != nil {
			log.Printf("dead-letter error: %v", dlqErr)
		}
		return
	}

	if _, err := pst.Persist(ctx, *outcome.Envelope); err != nil {
		log.Printf("persist error: %v", err)
	}
}
