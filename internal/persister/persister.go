// Package persister writes validated events to a Redis Stream the
// downstream Sender reads from. Ready events land on ReadyStream; anything
// that fails validation or processing lands on DLQStream instead, so one
// bad event never blocks the pipeline or gets silently dropped.
package persister

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"streaming-event-processor/internal/envelope"
)

const (
	ReadyStream     = "events:ready"
	DLQStream       = "events:dlq"
	dedupeKeyPrefix = "events:seen:"
	dedupeTTL       = 24 * time.Hour // bounds the dedupe window, not correctness-critical
)

// RedisPersister persists validated events with an idempotency guard
// (SETNX on event_id) that prevents duplicate persistence on redelivery.
type RedisPersister struct {
	client *redis.Client
}

func New(redisURL string) (*RedisPersister, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis url: %w", err)
	}
	return &RedisPersister{client: redis.NewClient(opts)}, nil
}

func (p *RedisPersister) Close() error {
	return p.client.Close()
}

// Persist persists a validated event. Returns false if it was a duplicate
// (already persisted).
func (p *RedisPersister) Persist(ctx context.Context, env envelope.EventEnvelope) (bool, error) {
	dedupeKey := dedupeKeyPrefix + env.EventID
	isNew, err := p.client.SetNX(ctx, dedupeKey, "1", dedupeTTL).Result()
	if err != nil {
		return false, fmt.Errorf("dedupe check: %w", err)
	}
	if !isNew {
		log.Printf("Duplicate event_id=%s, skipping persist", env.EventID)
		return false, nil
	}

	fields, err := env.ToRedisFields()
	if err != nil {
		return false, fmt.Errorf("encoding envelope: %w", err)
	}

	if err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: ReadyStream, Values: fields}).Err(); err != nil {
		return false, fmt.Errorf("xadd %s: %w", ReadyStream, err)
	}

	log.Printf("Persisted event_id=%s event_type=%s tenant_id=%s to %s",
		env.EventID, env.EventType, env.TenantID, ReadyStream)
	return true, nil
}

// DeadLetter records a raw event that failed validation or processing.
func (p *RedisPersister) DeadLetter(ctx context.Context, raw any, reason string) error {
	rawJSON, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("encoding raw event: %w", err)
	}

	values := map[string]any{"reason": reason, "raw": string(rawJSON)}
	if err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: DLQStream, Values: values}).Err(); err != nil {
		return fmt.Errorf("xadd %s: %w", DLQStream, err)
	}

	log.Printf("Dead-lettered event: %s", reason)
	return nil
}
