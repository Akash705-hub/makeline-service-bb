package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// listenForOrdersASB keeps a persistent connection and processes messages as they arrive automatically
func ListenForOrdersASB(ctx context.Context, client *azservicebus.Client, orderQueueName string, handleOrder func(Order) error) error {
	receiver, err := client.NewReceiverForQueue(orderQueueName, nil)
	if err != nil {
		return fmt.Errorf("failed to create receiver: %w", err)
	}
	defer receiver.Close(ctx)

	log.Printf("Listening for orders on queue: %s...", orderQueueName)

	for {
		// ReceiveMessages blocks until a message arrives or context is cancelled
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)

		if err != nil {
			// If context is canceled (shutdown), return gracefully
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			log.Printf("Error receiving message: %v", err)
			// Optional: Backoff/sleep here to prevent log spam on network failure
			time.Sleep(2 * time.Second)
			continue
		}

		for _, message := range messages {
			log.Printf("ASB message received: %s", string(message.Body))

			var order Order
			err := json.Unmarshal(message.Body, &order)
			if err != nil {
				var jsonStr string
				if errString := json.Unmarshal(message.Body, &jsonStr); errString == nil {
					err = json.Unmarshal([]byte(jsonStr), &order)
				}
			}

			if err != nil {
				log.Printf("Failed to unmarshal order: %v. Abandoning.", err)
				receiver.AbandonMessage(ctx, message, nil)
				continue
			}

			// 2. Validate/Fix Order Data
			if order.OrderID == "" {
				order.OrderID = strconv.Itoa(rand.Intn(100000))
			}

			order.Status = 1 // Pending

			// 3. Pass to Business Logic (The Handler)
			if err := handleOrder(order); err != nil {
				log.Printf("Handler failed: %v. Abandoning.", err)
				receiver.AbandonMessage(ctx, message, nil)
				continue
			}

			// 4. Complete
			if err := receiver.CompleteMessage(ctx, message, nil); err != nil {
				log.Printf("Failed to complete message: %v", err)
			}
		}
	}
}

func unmarshalOrderFromQueue(data []byte) (Order, error) {
	var order Order

	err := json.Unmarshal(data, &order)
	if err != nil {
		log.Printf("failed to unmarshal order: %v\n", err)
		return Order{}, err
	}

	// add orderkey to order
	order.OrderID = strconv.Itoa(rand.Intn(100000))

	// set the status to pending
	order.Status = Pending

	return order, nil
}