package messageHandlers

import (
	"context"
	"kafka-go/service/producer"
	"kafka-go/utils"
	"log"
)

func handleInsert(doc map[string]interface{}, payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	includeFields := utils.GetIncludeFields(outputTopic)
	if includeFields == nil {
		log.Printf("No field mapping found for topic: %s", outputTopic)
		return
	}
	delete(payload, "__before")
	delete(payload, "__after")
	utils.CleanPayload(payload, includeFields)
	processedPayload, err := processPayload(payload, prod, ctx, parentID, outputTopic)
	if err != nil {
		log.Printf("Error processing payload: %v", err)
		return
	}

	doc["payload"] = processedPayload
	if schema, ok := doc["schema"].(map[string]interface{}); ok {
		utils.CleanSchema(schema, includeFields)
	} else {
		log.Printf("schema is not a map or is nil")
	}

	produceBaseMessage(doc, prod, ctx, parentID, outputTopic)
}
