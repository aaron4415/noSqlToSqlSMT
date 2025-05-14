package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"kafka-go/service/consumer"
	"kafka-go/service/producer"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"

	"gopkg.in/yaml.v3"
)

type MessageKey struct {
	ID                         string `json:"id"`
	DBZPhysicalTableIdentifier string `json:"__dbz__physicalTableIdentifier"`
}

type DiffResult struct {
	ArrayName     string
	IsObjectArray bool
	RemovedItems  []interface{}
	AddedItems    []interface{}
}

type SchemaField struct {
	Field    string `json:"field"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

type FieldMappings struct {
	Topics []struct {
		Name          string   `yaml:"name"`
		IncludeFields []string `yaml:"include_fields"`
	} `yaml:"topics"`
}

var fieldMappings FieldMappings

func init() {
	// Load YAML config during initialization
	yamlFile, err := os.ReadFile("field_mappings.yaml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, &fieldMappings)
	if err != nil {
		panic(err)
	}
}

func main() {
	log.Printf("SMT Start running")
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	brokers := strings.Split(os.Getenv("BROKERS"), ",")
	inputTopics := strings.Split(os.Getenv("INPUT_TOPICS"), ",")

	if len(inputTopics) == 0 || inputTopics[0] == "" {
		log.Fatal("INPUT_TOPICS environment variable is not set or is empty")
	}

	log.Printf("Watching %d topic(s):", len(inputTopics))
	for i, topic := range inputTopics {
		trimmedTopic := strings.TrimSpace(topic)
		if trimmedTopic != "" {
			log.Printf("  [%d] %s", i+1, trimmedTopic)
		}
	}

	ctx := context.Background()
	prod := producer.NewProducer(brokers)

	for _, inputTopic := range inputTopics {
		cons := consumer.NewConsumer(brokers, inputTopic)

		go func(c *consumer.Consumer, outTopic string) {
			defer c.Close()
			if err := c.Consume(ctx, func(msg kafka.Message) {
				if msg.Value == nil {
					log.Printf("null message")
					return
				} else {
					processMessage(msg.Value, prod, ctx, outTopic)
				}
			}); err != nil {
				log.Printf("Consumer error: %v", err)
			}
		}(cons, inputTopic)
	}

	select {}
}

func processMessage(value []byte, prod *producer.Producer, ctx context.Context, outputTopic string) {
	// Parse the incoming JSON into a document.
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(value), &doc); err != nil {
		log.Printf("❌ Error parsing JSON: %v", err)
		return
	}

	// Extract the payload (actual data) from the document.
	payload, ok := doc["payload"].(map[string]interface{})
	if !ok {
		log.Printf("No 'payload' field found in document or payload is not an object")
		return
	}

	op, ok := payload["__op"].(string)
	if !ok {
		log.Printf("No 'op' field in payload")
		return
	}

	// Get the parent's identifier from the payload (assuming it's a string).
	parentID, ok := payload["id"].(string)
	if !ok {
		log.Printf("No 'id' field found in payload")
		parentID = payload["_id"].(string)
	}

	switch op {
	case "c": // Insert
		handleInsert(doc, payload, prod, ctx, parentID, outputTopic)
	case "u": // Update
		handleUpdate(doc, payload, prod, ctx, parentID, outputTopic)
	case "d": // Delete
		handleDelete(payload, prod, ctx, parentID, outputTopic)
	case "r": // Delete
		log.Printf("No need to handle Read operation")
	default:
		log.Printf("Unknown operation: %s", op)
	}
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

func buildKafkaMessage(topicName string, id string, payload map[string]interface{}) (*kafka.Message, error) {
	flattenedPayload := flattenMap(payload, "", "_")
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

func handleInsert(doc map[string]interface{}, payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	includeFields := getIncludeFields(outputTopic)
	if includeFields == nil {
		log.Printf("No field mapping found for topic: %s", outputTopic)
		return
	}
	cleanPayload(payload)
	filterFields(payload, includeFields)
	processedPayload, err := processPayload(payload, prod, ctx, parentID, outputTopic)
	if err != nil {
		log.Printf("Error processing payload: %v", err)
		return
	}

	doc["payload"] = processedPayload
	if schema, ok := doc["schema"].(map[string]interface{}); ok {
		cleanSchema(schema, includeFields)
	} else {
		log.Printf("schema is not a map or is nil")
	}

	produceBaseMessage(doc, prod, ctx, parentID, outputTopic)
}

func handleUpdate(doc map[string]interface{}, payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	includeFields := getIncludeFields(outputTopic)
	before, after, err := getBeforeAfter(payload)
	if err != nil {
		log.Printf("Error getting before/after: %v", err)
		return
	} else {
		filterFields(before, includeFields)
		filterFields(after, includeFields)
	}
	filterFields(payload, includeFields)
	// Process main payload
	processedPayload, err := processPayload(payload, prod, ctx, parentID, outputTopic)
	if err != nil {
		log.Printf("Error processing payload: %v", err)
		return
	}

	diffResults := compareArrays(before, after)
	processArrayDiffs(diffResults, parentID, prod, ctx, outputTopic)

	cleanPayload(processedPayload)
	doc["payload"] = processedPayload
	if schema, ok := doc["schema"].(map[string]interface{}); ok {
		cleanSchema(schema, includeFields)
	} else {
		log.Printf("schema is not a map or is nil")
	}
	produceBaseMessage(doc, prod, ctx, parentID, outputTopic)
}

func handleDelete(payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	if err := prod.ProduceBatch(ctx, outputTopic, []kafka.Message{createTombstone(parentID)}); err != nil {
		log.Fatalf("Error producing base message: %v", err)
	} else {
		log.Printf("Successfully produced delete messages to topic %s", outputTopic)
	}
	before, after, err := getBeforeAfter(payload)
	if err != nil {
		log.Printf("Error getting before/after: %v", err)
		return
	}
	var diffs []DiffResult
	findArrayDiffsRecursive("", before, after, &diffs)
	processArrayDiffs(diffs, parentID, prod, ctx, outputTopic)
}

func processPayload(payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) (map[string]interface{}, error) {
	// Make a copy of payload for the base document.
	basePayload := make(map[string]interface{})
	for k, v := range payload {
		basePayload[k] = v
	}

	// Prepare a slice to hold messages for array fields.
	// We'll produce these messages to topics like "finalmongotestmsk_<field>"
	for field, fieldVal := range payload {
		// Check if the field value is an array.
		topicName := transformTopicName(fmt.Sprintf("%s_%s", outputTopic, field))
		if arrayField, ok := fieldVal.([]interface{}); ok {
			// Remove the array field from the base document.
			delete(basePayload, field)

			var messages []kafka.Message
			// Process each element in the array.
			for _, arrayItem := range arrayField {
				// Build a message payload for the array element
				var msgPayload map[string]interface{}

				if obj, ok := arrayItem.(map[string]interface{}); ok {
					// Get child ID from array item
					childID, exists := obj["id"]
					if !exists {
						log.Printf("Array item in field %s missing 'id'", field)
						continue
					}

					// Create composite _id = parentID + "_" + childID
					compositeID := fmt.Sprintf("%s_%v", parentID, childID)

					// Build payload with composite _id
					msgPayload = map[string]interface{}{
						"_id":       compositeID, // Composite primary key
						"parent_id": parentID,    // Explicit parent reference
					}

					// Merge all fields from the object
					for key, value := range obj {
						msgPayload[key] = value
					}
				} else {
					// Handle primitive values differently
					compositeID := fmt.Sprintf("%s_%v", parentID, toHashID(arrayItem))
					msgPayload = map[string]interface{}{
						"_id":       compositeID,
						"parent_id": parentID,
						"value":     arrayItem,
					}
				}
				// Flatten nested structures in the message payload
				msgPayload = flattenMap(msgPayload, "", "_")
				// Build Kafka message using composite ID as key
				msgKey := msgPayload["_id"].(string)
				msg, err := buildKafkaMessage(topicName, msgKey, msgPayload)
				if err != nil {
					log.Printf("Error creating message: %v", err)
					continue
				}
				messages = append(messages, *msg)
			}

			// Produce the batch of messages for the array field.
			if len(messages) > 0 {
				if err := prod.ProduceBatch(ctx, topicName, messages); err != nil {
					log.Fatalf("Error producing batch for field %s: %v", field, err)
				}
				log.Printf("Successfully produced %d messages to topic %s", len(messages), topicName)
			}
		}
	}
	return basePayload, nil
}

func filterFields(data map[string]interface{}, includeFields []string) {
	if data == nil {
		return
	}

	for key := range data {
		if !contains(includeFields, key) {
			delete(data, key)
		}
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func flattenMap(input map[string]interface{}, parentKey string, delimiter string) map[string]interface{} {
	output := make(map[string]interface{})
	for key, value := range input {
		newKey := key
		if parentKey != "" {
			newKey = parentKey + delimiter + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested maps
			nested := flattenMap(v, newKey, delimiter)
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

func parseJSONField(data string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(data), &result)
	return result, err
}

func compareArrays(before, after map[string]interface{}) []DiffResult {
	var diffs []DiffResult

	var walk func(path string, a, b interface{})
	walk = func(path string, a, b interface{}) {
		// If both are maps, recurse into them
		mapA, okA := a.(map[string]interface{})
		mapB, okB := b.(map[string]interface{})
		if okA && okB {
			for key := range mapA {
				if _, ok := mapB[key]; ok {
					nextPath := key
					if path != "" {
						nextPath = path + "_" + key
					}
					walk(nextPath, mapA[key], mapB[key])
				}
			}
			return
		}

		// If both are arrays
		arrA, okA := a.([]interface{})
		arrB, okB := b.([]interface{})
		if okA && okB {
			isObjectArray := false
			if len(arrA) > 0 {
				_, isObjectArray = arrA[0].(map[string]interface{})
			}
			if !isObjectArray && len(arrB) > 0 {
				_, isObjectArray = arrB[0].(map[string]interface{})
			}

			var removed, added []interface{}
			if isObjectArray {
				removed = objectDifference(arrA, arrB)
			} else {
				removed = primitiveDifference(arrA, arrB)
				added = primitiveDifference(arrB, arrA)
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

	// Start recursive comparison
	for key := range before {
		if _, ok := after[key]; ok {
			walk(key, before[key], after[key])
		}
	}
	return diffs
}

func findArrayDiffsRecursive(prefix string, before interface{}, after interface{}, diffs *[]DiffResult) {
	switch b := before.(type) {
	case map[string]interface{}:
		var a map[string]interface{}
		if after != nil {
			a, _ = after.(map[string]interface{})
		}
		for key, val := range b {
			fullKey := key
			if prefix != "" {
				fullKey = prefix + "_" + key
			}
			findArrayDiffsRecursive(fullKey, val, a[key], diffs)
		}
	case []interface{}:
		// Determine if it's an array of objects or primitives
		isObjectArray := false
		if len(b) > 0 {
			_, isObjectArray = b[0].(map[string]interface{})
		}
		var removed []interface{}
		if isObjectArray {
			removed = objectDifference(b, nil)
		} else {
			removed = primitiveDifference(b, nil)
		}
		*diffs = append(*diffs, DiffResult{
			ArrayName:     prefix,
			IsObjectArray: isObjectArray,
			RemovedItems:  removed,
			AddedItems:    nil,
		})
	}
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

func produceBaseMessage(doc map[string]interface{}, prod *producer.Producer, ctx context.Context, id string, outputTopic string) {
	// Remove array fields from schema
	if schema, ok := doc["schema"].(map[string]interface{}); ok {
		if fields, ok := schema["fields"].([]interface{}); ok {
			newFields := make([]interface{}, 0, len(fields))
			for _, f := range fields {
				if fieldMap, ok := f.(map[string]interface{}); ok {
					if fieldType, ok := fieldMap["type"].(string); ok && fieldType == "array" {
						continue
					}
				}
				newFields = append(newFields, f)
			}
			schema["fields"] = newFields
		}
	}

	// Marshal and produce
	valueBytes, err := json.Marshal(doc)
	if err != nil {
		log.Printf("Error marshalling value: %v", err)
		return
	}

	keyBytes, err := json.Marshal(id)
	if err != nil {
		log.Printf("Error marshalling value: %v", err)
		return
	}

	baseMsg := kafka.Message{
		Key:   keyBytes,
		Value: valueBytes,
	}

	transformedOutputTopic := transformTopicName(outputTopic)

	if err := prod.ProduceBatch(ctx, transformedOutputTopic, []kafka.Message{baseMsg}); err != nil {
		log.Printf("Error producing base message: %v", err)
		return
	}
	log.Printf("Produced id: %s base message to %s", id, transformedOutputTopic)
}

func cleanPayload(payload map[string]interface{}) {
	delete(payload, "__op")
	delete(payload, "__before")
	delete(payload, "__after")
}

func cleanSchema(schema map[string]interface{}, includeFields []string) {
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
func getBeforeAfter(payload map[string]interface{}) (before, after map[string]interface{}, err error) {
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

	return before, after, nil
}

func processArrayDiffs(diffResults []DiffResult, parentID string, prod *producer.Producer, ctx context.Context, outputTopic string) {
	for _, diff := range diffResults {
		topic := transformTopicName(fmt.Sprintf("%s_%v", outputTopic, diff.ArrayName))
		var messages []kafka.Message

		if diff.IsObjectArray {
			for _, item := range diff.RemovedItems {
				if obj, ok := item.(map[string]interface{}); ok {
					if id, exists := obj["id"]; exists {
						compositeID := fmt.Sprintf("%s_%s", parentID, id)
						messages = append(messages, createTombstone(compositeID))
					}
				}
			}
		} else {
			for _, item := range diff.AddedItems {
				compositeID := fmt.Sprintf("%s_%v", parentID, toHashID(item))
				msgPayload := map[string]interface{}{
					"_id":       compositeID,
					"parent_id": parentID,
					"value":     item,
				}

				msg, err := buildKafkaMessage(topic, compositeID, msgPayload)
				if err != nil {
					log.Printf("Error creating message: %v", err)
					continue
				}
				messages = append(messages, *msg)
			}
			for _, item := range diff.RemovedItems {
				compositeID := fmt.Sprintf("%s_%v", parentID, toHashID(item))
				messages = append(messages, createTombstone(compositeID))
			}
		}

		if len(messages) > 0 {
			if err := prod.ProduceBatch(ctx, topic, messages); err != nil {
				log.Printf("Failed to produce to %s: %v", topic, err)
			} else {
				log.Printf("Successfully produced %d messages to topic %s", len(messages), topic)
			}
		}
	}
}

func toHashID(item interface{}) string {
	itemStr := fmt.Sprintf("%v", item)
	hash := sha256.Sum256([]byte(itemStr))
	return hex.EncodeToString(hash[:8])
}

func createTombstone(compositeID string) kafka.Message {
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

func transformTopicName(input string) string {
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

func getIncludeFields(topicName string) []string {
	for _, topic := range fieldMappings.Topics {
		if topic.Name == topicName {
			return topic.IncludeFields
		}
	}
	return nil
}
