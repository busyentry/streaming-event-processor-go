// Package registry loads declarative JSON Schema contracts and dispatches
// validation by event_type. Adding support for a new event type is a matter
// of dropping a new .json file into the schemas directory -- no code
// changes required.
package registry

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// SchemaRegistry maps event_type -> compiled JSON Schema.
type SchemaRegistry struct {
	validators map[string]*jsonschema.Schema
}

// New loads every *.json file in schemasDir and compiles it as a draft-07
// JSON Schema, keyed by the schema's "title" field (falling back to the
// filename stem).
func New(schemasDir string) (*SchemaRegistry, error) {
	info, err := os.Stat(schemasDir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("schemas directory not found: %s", schemasDir)
	}

	matches, err := filepath.Glob(filepath.Join(schemasDir, "*.json"))
	if err != nil {
		return nil, fmt.Errorf("listing schemas: %w", err)
	}
	sort.Strings(matches)

	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft7

	validators := make(map[string]*jsonschema.Schema, len(matches))
	for _, path := range matches {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}

		url := "mem://" + filepath.Base(path)
		if err := compiler.AddResource(url, strings.NewReader(string(raw))); err != nil {
			return nil, fmt.Errorf("invalid schema in %s: %w", filepath.Base(path), err)
		}

		schema, err := compiler.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("invalid schema in %s: %w", filepath.Base(path), err)
		}

		eventType, _ := doc["title"].(string)
		if eventType == "" {
			eventType = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		}

		validators[eventType] = schema
		log.Printf("Loaded schema for event_type=%s from %s", eventType, filepath.Base(path))
	}

	if len(validators) == 0 {
		log.Printf("No schemas loaded from %s", schemasDir)
	}

	return &SchemaRegistry{validators: validators}, nil
}

// KnownEventTypes returns the sorted list of event types this registry knows.
func (r *SchemaRegistry) KnownEventTypes() []string {
	types := make([]string, 0, len(r.validators))
	for t := range r.validators {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// GetValidator returns the compiled schema for eventType, or nil if unknown.
func (r *SchemaRegistry) GetValidator(eventType string) *jsonschema.Schema {
	return r.validators[eventType]
}
