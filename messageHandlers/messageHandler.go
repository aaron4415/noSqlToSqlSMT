package messageHandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-go/service/producer"
	"kafka-go/utils"
	"log"

	"github.com/segmentio/kafka-go"
)

func ProcessMessage(value []byte, prod *producer.Producer, ctx context.Context, outputTopic string) {
	// 1) Unmarshal into a generic map
	var doc map[string]interface{}
	if err := json.Unmarshal(value, &doc); err != nil {
		log.Printf("❌ Error parsing JSON: %v", err)
		return
	}

	// 2) Extract the payload object
	payloadRaw, ok := doc["payload"]
	if !ok {
		log.Printf("❌ Missing top-level 'payload' field")
		return
	}
	payload, ok := payloadRaw.(map[string]interface{})
	if !ok {
		log.Printf("❌ 'payload' is not an object (got %T)", payloadRaw)
		return
	}

	// 3) Extract the operation code
	opRaw, ok := payload["__op"]
	if !ok {
		log.Printf("❌ Missing '__op' in payload")
		return
	}
	op, ok := opRaw.(string)
	if !ok {
		log.Printf("❌ '__op' is not a string (got %T)", opRaw)
		return
	}

	// 4) Extract a stable ID field (try "id", then "_id")
	parentID, err := utils.GetStringField(payload, "id", "_id")
	if err != nil {
		log.Printf("❌ %v", err)
		return
	}

	// 5) Dispatch based on operation
	switch op {
	case "c":
		handleInsert(doc, payload, prod, ctx, parentID, outputTopic)
	case "u":
		handleUpdate(doc, payload, prod, ctx, parentID, outputTopic)
	case "d":
		HandleDelete(payload, prod, ctx, parentID, outputTopic)
	case "r":
		log.Printf("🔍 Read op ignored")
	default:
		log.Printf("❓ Unknown op %q", op)
	}
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
		topicName := utils.TransformTopicName(fmt.Sprintf("%s_%s", outputTopic, field))
		if arrayField, ok := fieldVal.([]interface{}); ok {

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
					compositeID := fmt.Sprintf("%s_%v", parentID, utils.ToHashID(arrayItem))
					msgPayload = map[string]interface{}{
						"_id":       compositeID,
						"parent_id": parentID,
						"value":     arrayItem,
					}
				}
				// Flatten nested structures in the message payload
				msgPayload = utils.FlattenMap(msgPayload, "", "_")
				// Build Kafka message using composite ID as key
				msgKey := msgPayload["_id"].(string)
				msg, err := utils.BuildKafkaMessage(topicName, msgKey, msgPayload)
				if err != nil {
					log.Printf("Error creating message: %v", err)
					continue
				}
				messages = append(messages, *msg)
			}

			// Produce the batch of messages for the array field.
			if len(messages) > 0 {
				if err := prod.CreateAndWriteTopic(ctx, topicName, messages); err != nil {
					log.Fatalf("Error producing batch for field %s: %v", field, err)
				}
				log.Printf("Successfully produced %d messages to topic %s", len(messages), topicName)
			}
		}
	}
	return basePayload, nil
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

	transformedOutputTopic := utils.TransformTopicName(outputTopic)

	if err := prod.CreateAndWriteTopic(ctx, transformedOutputTopic, []kafka.Message{baseMsg}); err != nil {
		log.Printf("Error producing base message: %v", err)
		return
	}
	log.Printf("Produced id: %s base message to %s", id, transformedOutputTopic)
}
