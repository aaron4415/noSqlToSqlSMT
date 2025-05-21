package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"kafka-go/service/producer"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

type FieldMappings struct {
	Topics []struct {
		Name          string   `yaml:"name"`
		IncludeFields []string `yaml:"include_fields"`
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

func BuildKafkaMessage(topicName string, id string, payload map[string]interface{}) (*kafka.Message, error) {
	flattenedPayload := FlattenMap(payload, "", "_")
	keyBytes, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("error marshalling key: %w", err)
	}

	schemaFields := buildSchema(flattenedPayload)
	valueWrapper := map[string]interface{}{
		"schema": map[string]interface{}{
			"type":   "struct",
			"fields": schemaFields,
		},
		"payload": payload,
	}

	valueBytes, err := json.Marshal(valueWrapper)
	if err != nil {
		return nil, fmt.Errorf("error marshalling value: %w", err)
	}

	msg := kafka.Message{
		Key:   keyBytes,
		Value: valueBytes,
	}

	log.Printf("✔️ Prepared Kafka message for topic %s:\nKey: %s\nValue: %.300s...\n", topicName, keyBytes, valueBytes)
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

func CleanPayload(data map[string]interface{}, includeFields []string) {
	hierarchy := buildHierarchyTree(includeFields)
	cleanRecursive(data, hierarchy, "")
}

// Build nested map structure from dot-separated paths
func buildHierarchyTree(fields []string) map[string]interface{} {
	root := make(map[string]interface{})
	for _, f := range fields {
		parts := strings.Split(f, ".")
		node := root
		for _, p := range parts {
			child, exists := node[p]
			if !exists {
				child = make(map[string]interface{})
				node[p] = child
			}
			node = child.(map[string]interface{})
		}
	}
	return root
}

func cleanRecursive(data map[string]interface{}, hierarchy map[string]interface{}, currentPath string) {
	if data == nil {
		return
	}

	for key := range data {
		fullPath := joinPath(currentPath, key)

		// Check if this field or any nested field is included
		if _, exists := findSubtree(hierarchy, key); !exists {
			delete(data, key)
		} else {
			// Process nested structures with updated hierarchy
			processNested(data[key], hierarchy[key].(map[string]interface{}), fullPath)
		}
	}
}

func processNested(value interface{}, subtree map[string]interface{}, path string) {
	switch v := value.(type) {
	case map[string]interface{}:
		cleanRecursive(v, subtree, path)
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				cleanRecursive(m, subtree, path)
			}
		}
	}
}

// Helper functions
func joinPath(base, part string) string {
	if base == "" {
		return part
	}
	return base + "." + part
}

func findSubtree(hierarchy map[string]interface{}, key string) (map[string]interface{}, bool) {
	if subtree, ok := hierarchy[key].(map[string]interface{}); ok {
		return subtree, true
	}
	return nil, false
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
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
	var walk func(path string, a, b interface{})
	walk = func(path string, a, b interface{}) {
		// 1) Maps as before...
		mapA, okA := a.(map[string]interface{})
		mapB, okB := b.(map[string]interface{})
		if okA && okB {
			for key := range mapA {
				next := key
				if path != "" {
					next = path + "_" + key
				}
				walk(next, mapA[key], mapB[key])
			}
			return
		}

		// 2) Handle array vs nil (or non‐array) => record full removal or addition
		arrA, isArrA := a.([]interface{})
		arrB, isArrB := b.([]interface{})
		// case: array existed before but gone after => everything removed
		if isArrA && !isArrB {
			diffs = append(diffs, DiffResult{
				ArrayName:     path,
				IsObjectArray: len(arrA) > 0 && arrA[0] != nil && reflect.TypeOf(arrA[0]).Kind() == reflect.Map,
				RemovedItems:  arrA,
				AddedItems:    nil,
			})
			return
		}
		// case: array appears new after
		if !isArrA && isArrB && opts {
			diffs = append(diffs, DiffResult{
				ArrayName:     path,
				IsObjectArray: len(arrB) > 0 && arrB[0] != nil && reflect.TypeOf(arrB[0]).Kind() == reflect.Map,
				RemovedItems:  nil,
				AddedItems:    arrB,
			})
			return
		}

		// 3) Your existing "both non‐nil arrays" logic
		if isArrA && isArrB {
			isObjectArray := false
			if len(arrA) > 0 {
				_, isObjectArray = arrA[0].(map[string]interface{})
			}
			var removed, added []interface{}
			if isObjectArray {
				removed = objectDifference(arrA, arrB)
			} else {
				removed = primitiveDifference(arrA, arrB)
				if opts {
					added = primitiveDifference(arrB, arrA)
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
			return
		}

		// 4) Otherwise: primitives or mixed types, ignore
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
	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return
	}

	filtered := make([]interface{}, 0)
	for _, f := range fields {
		field, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		if fieldName, exists := field["field"].(string); exists && contains(includeFields, fieldName) {
			filtered = append(filtered, field)
		}
	}
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
	log.Printf("Simple before: %v", before)
	log.Printf("Simple after: %v", after)
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

func ProcessArrayDiffs(diffResults []DiffResult, parentID string, prod *producer.Producer, ctx context.Context, outputTopic string) {
	for _, diff := range diffResults {
		topic := TransformTopicName(fmt.Sprintf("%s_%v", outputTopic, diff.ArrayName))
		var messages []kafka.Message

		if diff.IsObjectArray {
			for _, item := range diff.RemovedItems {
				if obj, ok := item.(map[string]interface{}); ok {
					if id, exists := obj["id"]; exists {
						compositeID := fmt.Sprintf("%s_%v", parentID, id)
						messages = append(messages, CreateTombstone(compositeID))
					}
				}
			}
		} else {
			for _, item := range diff.AddedItems {
				compositeID := fmt.Sprintf("%s_%v", parentID, ToHashID(item))
				msgPayload := map[string]interface{}{
					"_id":       compositeID,
					"parent_id": parentID,
					"value":     item,
				}

				msg, err := BuildKafkaMessage(topic, compositeID, msgPayload)
				if err != nil {
					log.Printf("Error creating message: %v", err)
					continue
				}
				messages = append(messages, *msg)
			}
			for _, item := range diff.RemovedItems {
				compositeID := fmt.Sprintf("%s_%v", parentID, ToHashID(item))
				messages = append(messages, CreateTombstone(compositeID))
			}
		}

		if len(messages) > 0 {
			if err := prod.CreateAndWriteTopic(ctx, topic, messages); err != nil {
				log.Printf("Failed to produce to %s: %v", topic, err)
			} else {
				log.Printf("Successfully produced %d messages to topic %s", len(messages), topic)
			}
		}
	}
}

func ToHashID(item interface{}) string {
	itemStr := fmt.Sprintf("%v", item)
	hash := sha256.Sum256([]byte(itemStr))
	return hex.EncodeToString(hash[:8])
}

func CreateTombstone(compositeID string) kafka.Message {
	keyBytes, err := json.Marshal(compositeID)
	if err != nil {
		log.Printf("error marshalling key: %v", err)
	}
	return kafka.Message{
		Key:   keyBytes,
		Value: nil,
	}
}

func objectDifference(a, b []interface{}) []interface{} {
	var diff []interface{}
	for _, itemA := range a {
		mapA, okA := itemA.(map[string]interface{})
		if !okA {
			continue
		}
		idA, okA := mapA["id"]
		if !okA {
			continue
		}

		found := false
		for _, itemB := range b {
			mapB, okB := itemB.(map[string]interface{})
			if !okB {
				continue
			}
			idB, okB := mapB["id"]
			if !okB {
				continue
			}
			if reflect.DeepEqual(idA, idB) {
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

func GetIncludeFields(topicName string) []string {
	for _, topic := range fieldMappings.Topics {
		if topic.Name == topicName {
			return topic.IncludeFields
		}
	}
	return nil
}
