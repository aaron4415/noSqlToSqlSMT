package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

type FieldMappings struct {
	Topics []struct {
		Name                     string   `yaml:"name"`
		IncludeFields            []string `yaml:"include_fields"`
		array_obj_include_fields []string `yaml:"array_obj_include_fields"`
	} `yaml:"topics"`
}

var fieldMappings FieldMappings

type SchemaField struct {
	Field    string `json:"field"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

type DiffResult struct {
	ArrayName     string
	IsObjectArray bool
	RemovedItems  []interface{}
	AddedItems    []interface{}
}

func TransformTopicName(input string) string {
	const streamPrefix = "mongo.data-hub-stream."
	const sourcePrefix = "mongo.data-hub-source."
	const maxLen = 30
	var trimmed string

	// Step 1: Replace prefix with "MDHS_"
	if strings.HasPrefix(input, streamPrefix) {
		trimmed = strings.TrimPrefix(input, streamPrefix)
	} else if strings.HasPrefix(input, sourcePrefix) {
		trimmed = strings.TrimPrefix(input, sourcePrefix)
	}
	normalized := strings.NewReplacer(".", "_", "-", "_").Replace(trimmed)

	// Step 2: Split into parts
	parts := strings.Split(normalized, "_")

	// Remove empty strings caused by consecutive separators
	cleanParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}

	// Step 3: Capitalize all parts
	for i := range cleanParts {
		cleanParts[i] = strings.ToUpper(cleanParts[i])
	}

	// Step 4: Assemble full name and check length
	fullName := "MDHS_" + strings.Join(cleanParts, "_")
	if len(fullName) <= maxLen {
		return fullName
	}

	// Step 5: If too long, shorten each part to 3 characters
	shortenedParts := make([]string, 0, len(cleanParts))
	for _, part := range cleanParts {
		if len(part) > 3 {
			shortenedParts = append(shortenedParts, part[:3])
		} else {
			shortenedParts = append(shortenedParts, part)
		}
	}

	shortenedName := "MDHS_" + strings.Join(shortenedParts, "_")
	if len(shortenedName) > maxLen {
		// Optional: truncate result to maxLen if still too long
		return shortenedName[:maxLen]
	}

	return shortenedName
}

func buildSchema(newMsg map[string]interface{}) []SchemaField {
	fields := []SchemaField{}
	for key, value := range newMsg {
		field := SchemaField{
			Field:    key,
			Type:     inferType(value),
			Optional: true,
		}
		fields = append(fields, field)
	}
	return fields
}

func BuildKafkaMessage(topicName string, id string, payload map[string]interface{}, allowed []string) (*kafka.Message, error) {
	flattenedPayload := FlattenMap(payload, "", "_")
	cleanedPayload := make(map[string]interface{}, len(allowed))
	for _, key := range allowed {
		if val, exists := flattenedPayload[key]; exists && val != nil {
			cleanedPayload[key] = val
		}
	}

	schemaSection := map[string]interface{}{
		"type":   "struct",
		"fields": buildSchema(cleanedPayload),
	}

	CleanSchema(schemaSection, allowed)

	envelope := map[string]interface{}{
		"schema":  schemaSection,
		"payload": cleanedPayload,
	}

	valueBytes, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("error marshalling value: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(id),
		Value: valueBytes,
	}

	return &msg, nil
}

func inferType(value interface{}) string {
	switch v := value.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64:
		// Check if the float64 is really an integer (i.e. no fractional part)
		if v == float64(int64(v)) {
			// If the value fits in int64, mark it as such.
			return "int64"
		}
		return "double"
	case float32:
		// Even though most JSON numbers become float64, include this case for completeness.
		if v == float32(int64(v)) {
			return "int64"
		}
		return "double"
	case int, int32, int64:
		return "int64"
	default:
		// Fallback: use reflection to get the type name
		return fmt.Sprintf("%T", v)
	}
}

func FlattenMap(input map[string]interface{}, parentKey string, delimiter string) map[string]interface{} {
	output := make(map[string]interface{})
	for key, value := range input {
		newKey := key
		if parentKey != "" {
			newKey = parentKey + delimiter + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested maps
			nested := FlattenMap(v, newKey, delimiter)
			for k, val := range nested {
				output[k] = val
			}
		case []interface{}:
			// Handle arrays if needed (optional)
			// This example skips arrays to focus on object flattening
		default:
			output[newKey] = value
		}
	}
	return output
}

func Contains(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

func parseJSONField(data string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(data), &result)
	return result, err
}

func ComputeArrayDiffs(before, after interface{}, opts bool) ([]DiffResult, error) {
	var diffs []DiffResult
	visited := make(map[string]struct{})

	var walk func(path string, a, b interface{})
	walk = func(path string, a, b interface{}) {
		mapA, okA := a.(map[string]interface{})
		mapB, okB := b.(map[string]interface{})

		if okA || okB {
			allKeys := map[string]struct{}{}
			if okA {
				for k := range mapA {
					allKeys[k] = struct{}{}
				}
			}
			if okB {
				for k := range mapB {
					allKeys[k] = struct{}{}
				}
			}

			for key := range allKeys {
				nextPath := key
				if path != "" {
					nextPath = path + "_" + key
				}
				walk(nextPath, mapA[key], mapB[key])
			}
			return
		}

		arrA, isArrA := a.([]interface{})
		arrB, isArrB := b.([]interface{})

		if isArrA || isArrB {
			if _, seen := visited[path]; seen {
				return
			}
			visited[path] = struct{}{}

			isObjectArray := false
			if isArrA && len(arrA) > 0 {
				_, isObjectArray = arrA[0].(map[string]interface{})
			} else if isArrB && len(arrB) > 0 {
				_, isObjectArray = arrB[0].(map[string]interface{})
			}

			var removed, added []interface{}
			if isArrA && !isArrB {
				removed = arrA
			} else if !isArrA && isArrB && opts {
				added = arrB
			} else if isArrA && isArrB {
				if isObjectArray {
					removed = objectDifference(arrA, arrB)
					if opts {
						added = objectDifference(arrB, arrA)
					}
				} else {
					removed = primitiveDifference(arrA, arrB)
					if opts {
						added = primitiveDifference(arrB, arrA)
					}
				}
			}

			if len(removed) > 0 || len(added) > 0 {
				diffs = append(diffs, DiffResult{
					ArrayName:     path,
					IsObjectArray: isObjectArray,
					RemovedItems:  removed,
					AddedItems:    added,
				})
			}
		}
	}

	walk("", before, after)
	return diffs, nil
}

func primitiveDifference(a, b []interface{}) []interface{} {
	var diff []interface{}
	for _, itemA := range a {
		found := false
		for _, itemB := range b {
			if reflect.DeepEqual(itemA, itemB) {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, itemA)
		}
	}
	return diff
}

func CleanSchema(schema map[string]interface{}, includeFields []string) {
	rawFields, ok := schema["fields"].([]SchemaField)
	if !ok {
		return
	}

	filtered := make([]SchemaField, 0, len(rawFields))
	for _, field := range rawFields {
		// Keep only whitelisted fields
		if !Contains(includeFields, field.Field) {
			continue
		}
		// Skip array types
		if field.Type == "array" {
			continue
		}
		filtered = append(filtered, field)
	}

	// Assign filtered slice back
	schema["fields"] = filtered
}

func GetBeforeAfter(payload map[string]interface{}) (before, after map[string]interface{}, err error) {
	// Get and decode __before if available and not null
	if beforeRaw, ok := payload["__before"]; ok && beforeRaw != nil {
		if beforeStr, ok := beforeRaw.(string); ok {
			before, err = parseJSONField(beforeStr)
			if err != nil {
				return nil, nil, fmt.Errorf("error parsing before: %v", err)
			}
		}
	}

	// Get and decode __after if available and not null
	if afterRaw, ok := payload["__after"]; ok && afterRaw != nil {
		if afterStr, ok := afterRaw.(string); ok {
			after, err = parseJSONField(afterStr)
			if err != nil {
				return nil, nil, fmt.Errorf("error parsing after: %v", err)
			}
		}
	}

	// If both are nil, return error
	if before == nil && after == nil {
		return nil, nil, fmt.Errorf("both before and after are null")
	}

	before = flattenWrapper(before, "__before")
	after = flattenWrapper(after, "__after")
	return before, after, nil
}

func flattenWrapper(m map[string]interface{}, wrapperKey string) map[string]interface{} {
	if m == nil {
		return nil
	}
	if inner, ok := m[wrapperKey].(map[string]interface{}); ok {
		for k, v := range inner {
			m[k] = v
		}
		delete(m, wrapperKey)
	}
	return m
}

func ToHashID(item interface{}) string {
	itemStr := fmt.Sprintf("%v", item)
	hash := sha256.Sum256([]byte(itemStr))
	return hex.EncodeToString(hash[:8])
}

func CreateTombstone(compositeID string) kafka.Message {
	return kafka.Message{
		Key:   []byte(compositeID),
		Value: nil,
	}
}

func objectDifference(a, b []interface{}) []interface{} {
	var result []interface{}
	for _, itemA := range a {
		found := false
		for _, itemB := range b {
			if reflect.DeepEqual(itemA, itemB) {
				found = true
				break
			}
		}
		if !found {
			result = append(result, itemA)
		}
	}
	return result
}

func LoadFieldMappings() error {
	yamlFile, err := os.ReadFile("field_mappings.yaml")
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(yamlFile, &fieldMappings)
	if err != nil {
		return err
	}
	return nil
}

func GetIncludeFields(topicName string) ([]string, map[string][]string) {
	var topLevel []string
	nestedInclude := make(map[string][]string)

	for _, topic := range fieldMappings.Topics {
		if topic.Name != topicName {
			continue
		}
		for _, f := range topic.IncludeFields {
			if !strings.Contains(f, ".") {
				// top-level field
				topLevel = append(topLevel, f)
			} else {
				// nested field in dot-notation: "route.id"
				parts := strings.SplitN(f, ".", 2)
				arrayName, fieldName := parts[0], parts[1]
				nestedInclude[arrayName] = append(nestedInclude[arrayName], fieldName)
			}
		}
		return topLevel, nestedInclude
	}

	return topLevel, nestedInclude
}
