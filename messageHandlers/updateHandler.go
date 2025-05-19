package messageHandlers

import (
	"context"
	"kafka-go/service/producer"
	"kafka-go/utils"
	"log"
)

func handleUpdate(doc map[string]interface{}, payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	includeFields := utils.GetIncludeFields(outputTopic)
	before, after, err := utils.GetBeforeAfter(payload)
	if err != nil {
		log.Printf("Error getting before/after: %v", err)
		return
	} else {
		utils.CleanPayload(before, includeFields)
		utils.CleanPayload(after, includeFields)
	}
	// Process main payload
	processedPayload, err := processPayload(payload, prod, ctx, parentID, outputTopic)
	if err != nil {
		log.Printf("Error processing payload: %v", err)
		return
	}

	diffResults, err := utils.ComputeArrayDiffs(before, after, true)
	if err != nil {
		log.Printf("Error Compute Array Diffs: %v", err)
		return
	}

	utils.ProcessArrayDiffs(diffResults, parentID, prod, ctx, outputTopic)

	doc["payload"] = processedPayload
	if schema, ok := doc["schema"].(map[string]interface{}); ok {
		utils.CleanSchema(schema, includeFields)
	} else {
		log.Printf("schema is not a map or is nil")
	}
	produceBaseMessage(doc, prod, ctx, parentID, outputTopic)
}
