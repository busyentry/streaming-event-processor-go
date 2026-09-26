// Package validator implements the two-stage validation used before an
// event is persisted:
//  1. Envelope shape (required fields, types).
//  2. Full payload against the declarative JSON Schema contract registered
//     for this event_type.
package validator

import (
	"fmt"

	"streaming-event-processor/internal/envelope"
	"streaming-event-processor/internal/registry"
)

// ValidationOutcome is the result of validating one raw event.
type ValidationOutcome struct {
	Valid    bool
	Envelope *envelope.EventEnvelope
	Reason   string
}

// ValidateRawEvent runs the two-stage validation described above.
func ValidateRawEvent(raw map[string]any, reg *registry.SchemaRegistry) ValidationOutcome {
	env, err := envelope.FromRaw(raw)
	if err != nil {
		return ValidationOutcome{Valid: false, Reason: fmt.Sprintf("envelope_invalid: %s", err)}
	}

	payloadValidator := reg.GetValidator(env.EventType)
	if payloadValidator == nil {
		return ValidationOutcome{Valid: false, Reason: fmt.Sprintf("unknown_event_type: %s", env.EventType)}
	}

	if err := payloadValidator.Validate(raw); err != nil {
		return ValidationOutcome{Valid: false, Reason: fmt.Sprintf("schema_violation: %s", err)}
	}

	return ValidationOutcome{Valid: true, Envelope: &env}
}
