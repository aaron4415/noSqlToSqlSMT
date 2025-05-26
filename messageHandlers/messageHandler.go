package messageHandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-go/service/producer"
	"kafka-go/utils"
	"log"
	"regexp"

	"github.com/segmentio/kafka-go"
)

func ProcessMessage(keyBytes, valueBytes []byte, prod *producer.Producer, ctx context.Context, outputTopic string) {
	// 1) Unmarshal into a generic map
	var doc map[string]interface{}
	if err := json.Unmarshal(valueBytes, &doc); err != nil {
		log.Printf("❌ Error parsing JSON: %v", err)
		return
	}
	keyStr := string(keyBytes)
	m := regexp.MustCompile(`id=([^\}]+)`).FindStringSubmatch(keyStr)
	if len(m) < 1 {
		log.Printf("❌ could not extract id from key: %q", keyStr)
		return
	}

	// 2) Convert to a real ObjectID if you need to use mongo-driver types
	documentID := m[1]
	log.Printf("🔑 got documentID = %s", documentID)

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

	// 5) Dispatch based on operation
	switch op {
	case "c":
		handleUpdateOrInsert(payload, prod, ctx, documentID, outputTopic)
	case "u":
		handleUpdateOrInsert(payload, prod, ctx, documentID, outputTopic)
	case "d":
		HandleDelete(payload, prod, ctx, documentID, outputTopic)
	case "r":
		handleUpdateOrInsert(payload, prod, ctx, documentID, outputTopic)
	default:
		log.Printf("❓ Unknown op %q", op)
	}
}

func ProcessArrayDiffs(diffResults []utils.DiffResult, parentID string, prod *producer.Producer, ctx context.Context, outputTopic string, includeFields []string, nested map[string][]string) {
	for _, diff := range diffResults {
		topic := utils.TransformTopicName(fmt.Sprintf("%s_%v", outputTopic, diff.ArrayName))
		var messages []kafka.Message
		if utils.Contains(includeFields, diff.ArrayName) {
			if diff.IsObjectArray {
				allowed := nested[diff.ArrayName]
				allowed = append(allowed, "parent_id")
				for _, item := range diff.RemovedItems {
					if obj, ok := item.(map[string]interface{}); ok {
						if id, exists := obj["id"]; exists {
							compositeID := fmt.Sprintf("%s_%v", parentID, id)
							messages = append(messages, utils.CreateTombstone(compositeID))
						}
					}
				}
				for _, item := range diff.AddedItems {
					if obj, ok := item.(map[string]interface{}); ok {
						if id, exists := obj["id"]; exists {
							compositeID := fmt.Sprintf("%s_%v", parentID, id)
							msgPayload := map[string]interface{}{
								"_id":       compositeID, // Composite primary key
								"parent_id": parentID,    // Explicit parent reference
							}

							// Merge all fields from the object
							for key, value := range obj {
								msgPayload[key] = value
							}

							msg, err := utils.BuildKafkaMessage(topic, compositeID, msgPayload, allowed)
							if err != nil {
								log.Printf("Error creating message: %v", err)
								continue
							}
							messages = append(messages, *msg)
						}
					}
				}
			} else {
				for _, item := range diff.AddedItems {
					compositeID := fmt.Sprintf("%s_%v", parentID, utils.ToHashID(item))
					msgPayload := map[string]interface{}{
						"_id":       compositeID,
						"parent_id": parentID,
						"value":     item,
					}

					msg, err := utils.BuildKafkaMessage(topic, compositeID, msgPayload, []string{"_id", "parent_id", "value"})
					if err != nil {
						log.Printf("Error creating message: %v", err)
						continue
					}
					messages = append(messages, *msg)
				}
				for _, item := range diff.RemovedItems {
					compositeID := fmt.Sprintf("%s_%v", parentID, utils.ToHashID(item))
					messages = append(messages, utils.CreateTombstone(compositeID))
				}
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

func produceBaseMessage(prod *producer.Producer, ctx context.Context, id string, outputTopic string, topLevel []string, payload map[string]interface{}) {
	transformedOutputTopic := utils.TransformTopicName(outputTopic)

	msg, err := utils.BuildKafkaMessage(transformedOutputTopic, id, payload, topLevel)
	if err != nil {
		log.Printf("Error building Kafka message for id %s: %v", id, err)
		return
	}

	if err := prod.CreateAndWriteTopic(ctx, transformedOutputTopic, []kafka.Message{*msg}); err != nil {
		log.Printf("Error producing base message: %v", err)
		return
	}
}
