// Command sample_producer simulates multiple producers publishing events to
// Kafka, mixing in a fraction of invalid events to exercise the DLQ path.
//
// Run against the docker-compose stack:
//
//	go run ./cmd/sample_producer
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	bootstrapServers = "localhost:9092"
	topic            = "events"
	count            = 30
	invalidRatio     = 0.15
)

var (
	tenants    = []string{"client-a", "client-b", "client-c"}
	severities = []string{"info", "warning", "critical"}
)

func pick(options []string) string {
	return options[rand.Intn(len(options))]
}

func makeMonitoringAlert() map[string]any {
	tenant := pick(tenants)
	return map[string]any{
		"event_id":         uuid.NewString(),
		"event_type":       "monitoring.alert",
		"tenant_id":        tenant,
		"target_client_id": tenant,
		"occurred_at":      time.Now().UTC().Format(time.RFC3339),
		"payload": map[string]any{
			"severity": pick(severities),
			"message":  "Sample alert from producer simulation",
		},
	}
}

func makeTransactionAuthorized() map[string]any {
	tenant := pick(tenants)
	return map[string]any{
		"event_id":         uuid.NewString(),
		"event_type":       "transaction.authorized",
		"tenant_id":        tenant,
		"target_client_id": tenant,
		"occurred_at":      time.Now().UTC().Format(time.RFC3339),
		"payload": map[string]any{
			"transaction_id": uuid.NewString(),
			"amount":         roundToCents(rand.Float64()*499 + 1),
			"currency":       "USD",
		},
	}
}

// makeInvalidEvent is missing a required payload field and uses an
// out-of-enum value, to exercise the DLQ path.
func makeInvalidEvent() map[string]any {
	return map[string]any{
		"event_id":         uuid.NewString(),
		"event_type":       "monitoring.alert",
		"tenant_id":        "client-a",
		"target_client_id": "client-a",
		"occurred_at":      time.Now().UTC().Format(time.RFC3339),
		"payload": map[string]any{
			"severity": "catastrophic",
		},
	}
}

func roundToCents(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(bootstrapServers),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx := context.Background()

	for i := 0; i < count; i++ {
		roll := rand.Float64()

		var event map[string]any
		switch {
		case roll < invalidRatio:
			event = makeInvalidEvent()
		case roll < invalidRatio+0.5:
			event = makeMonitoringAlert()
		default:
			event = makeTransactionAuthorized()
		}

		body, err := json.Marshal(event)
		if err != nil {
			log.Fatalf("encoding event: %v", err)
		}

		if err := writer.WriteMessages(ctx, kafka.Message{Value: body}); err != nil {
			log.Fatalf("sending event: %v", err)
		}

		fmt.Printf("Sent %s event_id=%s\n", event["event_type"], event["event_id"])
		time.Sleep(200 * time.Millisecond)
	}
}
