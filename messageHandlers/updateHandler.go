package messageHandlers

import (
	"context"
	"kafka-go/service/producer"
	"kafka-go/utils"
	"log"
)

func handleUpdateOrInsert(payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	topLevel, nested := utils.GetIncludeFields(outputTopic)
	before, after, err := utils.GetBeforeAfter(payload)
	if err != nil {
		log.Printf("Error getting before/after: %v", err)
		return
	} else {
		delete(payload, "__before")
		delete(payload, "__after")
	}

	diffResults, err := utils.ComputeArrayDiffs(before, after, true)
	if err != nil {
		log.Printf("Error Compute Array Diffs: %v", err)
		return
	}

	ProcessArrayDiffs(diffResults, parentID, prod, ctx, outputTopic, topLevel, nested)

	produceBaseMessage(prod, ctx, parentID, outputTopic, topLevel, payload)
}
