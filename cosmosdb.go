package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/gofrs/uuid"
)

type PartitionKey struct {
	Key   string
	Value string
}

type CosmosDBOrderRepo struct {
	db           *azcosmos.ContainerClient
	partitionKey PartitionKey
}

func NewCosmosDBOrderRepoWithManagedIdentity(cosmosDbEndpoint string, dbName string, containerName string, partitionKey PartitionKey) (*CosmosDBOrderRepo, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("failed to create cosmosdb workload identity credential: %v\n", err)
		return nil, err
	}

	opts := azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
	}

	client, err := azcosmos.NewClient(cosmosDbEndpoint, cred, &opts)
	if err != nil {
		log.Printf("failed to create cosmosdb client: %v\n", err)
		return nil, err
	}

	// create a cosmos container
	container, err := client.NewContainer(dbName, containerName)
	if err != nil {
		log.Printf("failed to create cosmosdb container: %v\n", err)
		return nil, err
	}

	return &CosmosDBOrderRepo{container, partitionKey}, nil
}

func NewCosmosDBOrderRepo(cosmosDbEndpoint string, dbName string, containerName string, cosmosDbKey string, partitionKey PartitionKey) (*CosmosDBOrderRepo, error) {
	cred, err := azcosmos.NewKeyCredential(cosmosDbKey)
	if err != nil {
		log.Printf("failed to create cosmosdb key credential: %v\n", err)
		return nil, err
	}

	// create a cosmos client
	client, err := azcosmos.NewClientWithKey(cosmosDbEndpoint, cred, nil)
	if err != nil {
		log.Printf("failed to create cosmosdb client: %v\n", err)
		return nil, err
	}

	// create a cosmos container
	container, err := client.NewContainer(dbName, containerName)
	if err != nil {
		log.Printf("failed to create cosmosdb container: %v\n", err)
		return nil, err
	}

	return &CosmosDBOrderRepo{container, partitionKey}, nil
}

func (r *CosmosDBOrderRepo) GetPendingOrders() ([]Order, error) {
	var orders []Order

	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@status", Value: Pending},
		},
	}
	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o WHERE o.status = @status", pk, opt)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			log.Printf("failed to get next page: %v\n", err)
			return nil, err
		}

		for _, item := range queryResponse.Items {
			var order Order
			err := json.Unmarshal(item, &order)
			if err != nil {
				log.Printf("failed to deserialize order: %v\n", err)
				return nil, err
			}
			orders = append(orders, order)
		}
	}
	return orders, nil
}

func (r *CosmosDBOrderRepo) GetOrder(id string) (Order, error) {
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@orderId", Value: id},
		},
	}
	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o WHERE o.orderId = @orderId", pk, opt)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			log.Printf("failed to get next page: %v\n", err)
			return Order{}, err
		}

		for _, item := range queryResponse.Items {
			var order Order
			err := json.Unmarshal(item, &order)
			if err != nil {
				log.Printf("failed to deserialize order: %v\n", err)
				return Order{}, err
			}
			return order, nil
		}
	}
	return Order{}, nil
}

func (r *CosmosDBOrderRepo) InsertOrders(orders []Order) error {
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	for _, o := range orders {
		// Create document map directly
		doc := map[string]interface{}{
			"id":         strings.Replace(uuid.Must(uuid.NewV4()).String(), "-", "", -1),
			"orderId":    o.OrderID,
			"customerId": o.CustomerID,
			"items":      o.Items,
			"status":     o.Status,
		}
		doc[r.partitionKey.Key] = r.partitionKey.Value

		marshalledOrder, err := json.Marshal(doc)
		if err != nil {
			log.Printf("failed to marshal order: %v\n", err)
			return err
		}

		_, err = r.db.CreateItem(context.Background(), pk, marshalledOrder, nil)
		if err != nil {
			log.Printf("failed to create item: %v\n", err)
			return err
		}
	}

	log.Printf("Inserted %d documents into database\n", len(orders))
	return nil
}

func (r *CosmosDBOrderRepo) findOrderIdByOrderId(orderId string) (string, error) {
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@orderId", Value: orderId},
		},
	}
	queryPager := r.db.NewQueryItemsPager("SELECT o.id FROM o WHERE o.orderId = @orderId", pk, opt)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			return "", err
		}
		for _, item := range queryResponse.Items {
			var doc map[string]interface{}
			if err := json.Unmarshal(item, &doc); err == nil {
				if id, ok := doc["id"].(string); ok {
					return id, nil
				}
			}
		}
	}
	return "", nil
}

func (r *CosmosDBOrderRepo) UpdateOrder(order Order) error {
	existingOrderId, err := r.findOrderIdByOrderId(order.OrderID)
	if err != nil {
		log.Printf("failed to find order id: %v\n", err)
		return err
	}
	if existingOrderId == "" {
		log.Printf("Order %s not found for update", order.OrderID)
		return nil
	}

	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
	patch := azcosmos.PatchOperations{}
	patch.AppendReplace("/status", order.Status)
	patch.AppendReplace("/items", order.Items)

	_, err = r.db.PatchItem(context.Background(), pk, existingOrderId, patch, nil)
	if err != nil {
		log.Printf("failed to patch item: %v\n", err)
		return err
	}
	return nil
}