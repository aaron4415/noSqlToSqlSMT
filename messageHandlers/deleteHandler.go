package messageHandlers

import (
	"context"
	"kafka-go/service/producer"
	"kafka-go/utils"
	"log"

	"github.com/segmentio/kafka-go"
)

func handleDelete(payload map[string]interface{}, prod *producer.Producer, ctx context.Context, parentID string, outputTopic string) {
	if err := prod.CreateAndWriteTopic(ctx, outputTopic, []kafka.Message{utils.CreateTombstone(parentID)}); err != nil {
		log.Fatalf("Error producing base message: %v", err)
	} else {
		log.Printf("Successfully produced delete messages id: %s to topic %s", parentID, outputTopic)
	}
	before, after, err := utils.GetBeforeAfter(payload)
	if err != nil {
		log.Printf("Error getting before/after: %v", err)
		return
	}

	diffResults, err := utils.ComputeArrayDiffs(before, after, false)
	if err != nil {
		log.Printf("Error Compute Array Diffs: %v", err)
		return
	}
	utils.ProcessArrayDiffs(diffResults, parentID, prod, ctx, outputTopic)
}
