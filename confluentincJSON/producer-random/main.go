package main

// https://github.com/confluentinc/confluent-kafka-go/tree/master/examples/producer_example

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"math"
	"os"
	"time"
)

// User is a simple record example
type User struct {
	Name           string `json:"name"`
	FavoriteNumber int64  `json:"favorite_number"`
	FavoriteColor  string `json:"favorite_color"`
}

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092", // should have a second server in case one goes down
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	fmt.Printf("Created Producer %v\n", p)

	topic := "myTopic"
	// TODO what is URL ???
	url := "" ??
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		fmt.Printf("Failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used(see in confluentincProducer).
	deliveryChan := make(chan kafka.Event)

	value := User{
		Name:           "Paulux",
		FavoriteNumber: 34,
		FavoriteColor:  "blue",
	}
	payload, err := ser.Serialize(topic, &value)
	if err != nil {
		fmt.Printf("Failed to serialize payload: %s\n", err)
		os.Exit(1)
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: payload,
		Headers: []kafka.Header{{
			Key:   "myTestHeader",
			Value: []byte("header value are binary")},
		},
	}, deliveryChan)
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(1)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)

	// Flush and close the producer and the events channel
	// TODO not sure that is usefull in this case ???
	for p.Flush(10000) > 0 {
		fmt.Print("Still waiting to flush outstanding messages\n", err)
	}
}
