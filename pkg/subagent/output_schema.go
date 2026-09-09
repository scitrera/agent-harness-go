package subagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	MaxOutputSchemaBytes = 64 << 10
	maxOutputSchemaDepth = 32
)

// ValidationError is a deterministic, compact error returned to the child for
// its single correction attempt.
type ValidationError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// OutputContract is a compiled, fail-closed subset of JSON Schema suitable for
// delegated result contracts. Unsupported assertion keywords are rejected at
// admission instead of being silently ignored.
type OutputContract struct {
	raw    json.RawMessage
	schema map[string]any
	digest string
}

func CompileOutputSchema(raw json.RawMessage) (*OutputContract, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > MaxOutputSchemaBytes {
		return nil, errors.New("subagent: output schema exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var schema map[string]any
	if err := decoder.Decode(&schema); err != nil {
		return nil, fmt.Errorf("subagent: decode output schema: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, errors.New("subagent: output schema contains trailing JSON")
	} else if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("subagent: invalid trailing output schema content: %w", err)
	}
	if schema == nil {
		return nil, errors.New("subagent: output schema must be an object")
	}
	if err := validateSchemaShape(schema, 0); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return &OutputContract{raw: canonical, schema: schema, digest: "sha256:" + hex.EncodeToString(sum[:])}, nil
}

func (c *OutputContract) Raw() json.RawMessage {
	if c == nil {
		return nil
	}
	return append(json.RawMessage(nil), c.raw...)
}

func (c *OutputContract) Digest() string {
	if c == nil {
		return ""
	}
	return c.digest
}

func (c *OutputContract) Validate(raw json.RawMessage) []ValidationError {
	if c == nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return []ValidationError{{Path: "$", Message: "invalid JSON: " + err.Error()}}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return []ValidationError{{Path: "$", Message: "result contains trailing JSON"}}
	} else if !errors.Is(err, io.EOF) {
		return []ValidationError{{Path: "$", Message: "invalid trailing content: " + err.Error()}}
	}
	var out []ValidationError
	validateSchemaValue(c.schema, value, "$", &out)
	return out
}

// StructuredDigest identifies the exact validated JSON bytes returned by a
// child. Callers should trim surrounding whitespace before computing it.
func StructuredDigest(raw json.RawMessage) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

var allowedSchemaKeywords = map[string]bool{
	"$schema": true, "$id": true, "title": true, "description": true, "default": true, "examples": true,
	"type": true, "properties": true, "required": true, "additionalProperties": true,
	"items": true, "enum": true, "const": true,
	"minItems": true, "maxItems": true, "minLength": true, "maxLength": true,
	"minimum": true, "maximum": true, "pattern": true,
}

func validateSchemaShape(schema map[string]any, depth int) error {
	if depth > maxOutputSchemaDepth {
		return errors.New("subagent: output schema exceeds nesting limit")
	}
	for keyword := range schema {
		if !allowedSchemaKeywords[keyword] {
			return fmt.Errorf("subagent: unsupported output schema keyword %q", keyword)
		}
	}
	if rawType, ok := schema["type"]; ok {
		if _, ok := schemaTypes(rawType); !ok {
			return errors.New("subagent: output schema type must be a supported string or string array")
		}
	}
	if raw, ok := schema["properties"]; ok {
		properties, ok := raw.(map[string]any)
		if !ok {
			return errors.New("subagent: output schema properties must be an object")
		}
		for name, child := range properties {
			childSchema, ok := child.(map[string]any)
			if !ok {
				return fmt.Errorf("subagent: output schema property %q must be an object", name)
			}
			if err := validateSchemaShape(childSchema, depth+1); err != nil {
				return err
			}
		}
	}
	if raw, ok := schema["required"]; ok {
		if _, ok := stringArray(raw); !ok {
			return errors.New("subagent: output schema required must be a string array")
		}
	}
	if raw, ok := schema["additionalProperties"]; ok {
		if _, ok := raw.(bool); !ok {
			return errors.New("subagent: output schema additionalProperties must be boolean")
		}
	}
	if raw, ok := schema["items"]; ok {
		child, ok := raw.(map[string]any)
		if !ok {
			return errors.New("subagent: output schema items must be an object")
		}
		if err := validateSchemaShape(child, depth+1); err != nil {
			return err
		}
	}
	if raw, ok := schema["enum"]; ok {
		if _, ok := raw.([]any); !ok {
			return errors.New("subagent: output schema enum must be an array")
		}
	}
	for _, keyword := range []string{"minItems", "maxItems", "minLength", "maxLength"} {
		if raw, ok := schema[keyword]; ok {
			if value, ok := number(raw); !ok || value < 0 || math.Trunc(value) != value {
				return fmt.Errorf("subagent: output schema %s must be a non-negative integer", keyword)
			}
		}
	}
	for _, keyword := range []string{"minimum", "maximum"} {
		if raw, ok := schema[keyword]; ok {
			if _, ok := number(raw); !ok {
				return fmt.Errorf("subagent: output schema %s must be a number", keyword)
			}
		}
	}
	if raw, ok := schema["pattern"]; ok {
		pattern, ok := raw.(string)
		if !ok {
			return errors.New("subagent: output schema pattern must be a string")
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("subagent: invalid output schema pattern: %w", err)
		}
	}
	return nil
}

func validateSchemaValue(schema map[string]any, value any, path string, out *[]ValidationError) {
	if rawType, ok := schema["type"]; ok {
		types, _ := schemaTypes(rawType)
		matches := false
		for _, expected := range types {
			if matchesType(expected, value) {
				matches = true
				break
			}
		}
		if !matches {
			*out = append(*out, ValidationError{Path: path, Message: "expected type " + strings.Join(types, " or ")})
			return
		}
	}
	if enum, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enum {
			if jsonEqual(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			*out = append(*out, ValidationError{Path: path, Message: "value is not in enum"})
		}
	}
	if expected, ok := schema["const"]; ok && !jsonEqual(expected, value) {
		*out = append(*out, ValidationError{Path: path, Message: "value does not match const"})
	}

	if object, ok := value.(map[string]any); ok {
		properties, _ := schema["properties"].(map[string]any)
		if required, ok := stringArray(schema["required"]); ok {
			for _, name := range required {
				if _, exists := object[name]; !exists {
					*out = append(*out, ValidationError{Path: childPath(path, name), Message: "required property is missing"})
				}
			}
		}
		for name, item := range object {
			if child, ok := properties[name].(map[string]any); ok {
				validateSchemaValue(child, item, childPath(path, name), out)
			} else if additional, specified := schema["additionalProperties"].(bool); specified && !additional {
				*out = append(*out, ValidationError{Path: childPath(path, name), Message: "additional property is not allowed"})
			}
		}
	}
	if array, ok := value.([]any); ok {
		validateBound(schema, "minItems", float64(len(array)), path, "array has too few items", true, out)
		validateBound(schema, "maxItems", float64(len(array)), path, "array has too many items", false, out)
		if items, ok := schema["items"].(map[string]any); ok {
			for index, item := range array {
				validateSchemaValue(items, item, fmt.Sprintf("%s[%d]", path, index), out)
			}
		}
	}
	if text, ok := value.(string); ok {
		length := float64(utf8.RuneCountInString(text))
		validateBound(schema, "minLength", length, path, "string is too short", true, out)
		validateBound(schema, "maxLength", length, path, "string is too long", false, out)
		if raw, ok := schema["pattern"].(string); ok {
			pattern := regexp.MustCompile(raw)
			if !pattern.MatchString(text) {
				*out = append(*out, ValidationError{Path: path, Message: "string does not match pattern"})
			}
		}
	}
	if numeric, ok := number(value); ok {
		validateBound(schema, "minimum", numeric, path, "number is below minimum", true, out)
		validateBound(schema, "maximum", numeric, path, "number is above maximum", false, out)
	}
}

func validateBound(schema map[string]any, keyword string, actual float64, path, message string, minimum bool, out *[]ValidationError) {
	raw, ok := schema[keyword]
	if !ok {
		return
	}
	limit, ok := number(raw)
	if !ok {
		return
	}
	if (minimum && actual < limit) || (!minimum && actual > limit) {
		*out = append(*out, ValidationError{Path: path, Message: message})
	}
}

func schemaTypes(raw any) ([]string, bool) {
	valid := map[string]bool{"object": true, "array": true, "string": true, "number": true, "integer": true, "boolean": true, "null": true}
	if single, ok := raw.(string); ok {
		return []string{single}, valid[single]
	}
	values, ok := stringArray(raw)
	if !ok || len(values) == 0 {
		return nil, false
	}
	for _, value := range values {
		if !valid[value] {
			return nil, false
		}
	}
	return values, true
}

func stringArray(raw any) ([]string, bool) {
	values, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		out = append(out, text)
	}
	return out, true
}

func matchesType(expected string, value any) bool {
	switch expected {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := number(value)
		return ok
	case "integer":
		n, ok := number(value)
		return ok && math.Trunc(n) == n
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return false
	}
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func jsonEqual(left, right any) bool {
	a, _ := json.Marshal(left)
	b, _ := json.Marshal(right)
	return bytes.Equal(a, b)
}

func childPath(parent, name string) string {
	if strings.IndexFunc(name, func(r rune) bool {
		letterOrDigit := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		return r != '_' && r != '-' && !letterOrDigit
	}) < 0 {
		return parent + "." + name
	}
	encoded, _ := json.Marshal(name)
	return parent + "[" + string(encoded) + "]"
}
