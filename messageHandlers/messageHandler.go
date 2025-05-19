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
		HandleDelete(payload, prod, ctx, parentID, outputTopic)
	case "r": // Delete
		log.Printf("No need to handle Read operation")
	default:
		log.Printf("Unknown operation: %s", op)
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
