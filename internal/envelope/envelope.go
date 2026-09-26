// Package envelope defines the fixed shape every event must have, regardless
// of event_type. Payload's internal shape is type-specific and checked
// separately against the declarative JSON Schema registered for its
// event_type (see internal/registry and internal/validator).
package envelope

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EventEnvelope is the parsed, validated envelope of an event.
type EventEnvelope struct {
	EventID        string
	EventType      string
	TenantID       string
	TargetClientID string
	OccurredAt     time.Time
	Payload        map[string]any
}

// rawEnvelope mirrors the wire shape, using pointers/omitted-ness to tell
// "field absent" apart from "field present but zero value" — the same
// distinction pydantic's default_factory relies on for EventID/OccurredAt.
type rawEnvelope struct {
	EventID        *string        `json:"event_id"`
	EventType      *string        `json:"event_type"`
	TenantID       *string        `json:"tenant_id"`
	TargetClientID *string        `json:"target_client_id"`
	OccurredAt     *string        `json:"occurred_at"`
	Payload        map[string]any `json:"payload"`
}

// FromRaw parses raw into an EventEnvelope. EventType, TenantID,
// TargetClientID and Payload are required; EventID defaults to a fresh UUID
// and OccurredAt to the current time when absent from raw.
func FromRaw(raw map[string]any) (EventEnvelope, error) {
	buf, err := json.Marshal(raw)
	if err != nil {
		return EventEnvelope{}, fmt.Errorf("re-encoding raw event: %w", err)
	}

	var re rawEnvelope
	if err := json.Unmarshal(buf, &re); err != nil {
		return EventEnvelope{}, fmt.Errorf("decoding envelope: %w", err)
	}

	switch {
	case re.EventType == nil:
		return EventEnvelope{}, fmt.Errorf("field required: event_type")
	case re.TenantID == nil:
		return EventEnvelope{}, fmt.Errorf("field required: tenant_id")
	case re.TargetClientID == nil:
		return EventEnvelope{}, fmt.Errorf("field required: target_client_id")
	case re.Payload == nil:
		return EventEnvelope{}, fmt.Errorf("field required: payload")
	}

	eventID := uuid.NewString()
	if re.EventID != nil {
		eventID = *re.EventID
	}

	occurredAt := time.Now().UTC()
	if re.OccurredAt != nil {
		parsed, err := time.Parse(time.RFC3339, *re.OccurredAt)
		if err != nil {
			return EventEnvelope{}, fmt.Errorf("field occurred_at: invalid date-time: %w", err)
		}
		occurredAt = parsed
	}

	return EventEnvelope{
		EventID:        eventID,
		EventType:      *re.EventType,
		TenantID:       *re.TenantID,
		TargetClientID: *re.TargetClientID,
		OccurredAt:     occurredAt,
		Payload:        re.Payload,
	}, nil
}

// ToRedisFields flattens the envelope to str->str fields suitable for XADD.
func (e EventEnvelope) ToRedisFields() (map[string]string, error) {
	payloadJSON, err := json.Marshal(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("encoding payload: %w", err)
	}

	return map[string]string{
		"event_id":         e.EventID,
		"event_type":       e.EventType,
		"tenant_id":        e.TenantID,
		"target_client_id": e.TargetClientID,
		"occurred_at":      e.OccurredAt.Format(time.RFC3339),
		"payload":          string(payloadJSON),
	}, nil
}
