package main

import (
	"bytes"
	// "encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	// pb "getting-started-with-ccloud-golang/api/v1/proto"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	// "github.com/confluentinc/confluent-kafka-go/v2/kafka/rest"
	// "github.com/elodina/go-kafka-avro"
	// goavro "github.com/linkedin/goavro/v2"
	// "github.com/golang/protobuf/proto"
	// "github.com/linkedin/goavro/v2/schema"
	// "github.com/golang/protobuf/proto"
	// "github.com/riferrei/srclient"
)

const (
	producerMode string = "producer"
	consumerMode string = "consumer"
	schemaFile   string = "./api/v1/proto/Person.proto"
	// schemaFile   string = "./api/v1/proto/SensorReading.proto"
	// messageFile  string = "./api/v1/proto/Message.proto"
)

type Message struct {
	Text string
}

func main() {

	clientMode := os.Args[1]
	// props := LoadProperties()
	topic := "my-topic"

	if strings.Compare(clientMode, producerMode) == 0 {
		// producer(props, topic)
		producer(topic)
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		// consumer(topic)
		fmt.Println("Consumer is useless in this case as we use kafka connect")
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

func connectorExists(connectorName string) bool {
	url := fmt.Sprintf("http://127.0.0.1:8083/connectors/%s", connectorName)

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Failed to send GET request:", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true // Connector exists
	} else if resp.StatusCode == http.StatusNotFound {
		return false // Connector does not exist
	} else {
		fmt.Println("Failed to retrieve connector information. Status code:", resp.StatusCode)
		return false
	}
}

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

/**************************************************/
/******************** Producer ********************/
/**************************************************/

// func producer(props map[string]string, topic string) {
func producer(topic string) {

	// taskConfig := rest.TaskConfig{
	taskConfig := map[string]interface{}{
		"connector.class":                "com.mongodb.kafka.connect.MongoSinkConnector",
		"topics":                         "my-topic",
		"key.converter":                  "org.apache.kafka.connect.storage.StringConverter",
		"key.converter.schemas.enable":   "false",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
		"connection.uri":                 "mongodb://mongo:27017",
		"database":                       "my-database",
		"collection":                     "my-collection",
		"tasksMax":                       "3",
	}

	// Create an HTTP client
	// client := http.DefaultClient

	connExist := connectorExists("my-connector")

	if !connExist {
		// Create the Kafka Connect connector
		url := "http://127.0.0.1:8083/connectors"
		connectorRequest := map[string]interface{}{
			"name":   "my-connector",
			"config": taskConfig,
		}
		payload, err := json.Marshal(connectorRequest)
		if err != nil {
			panic(fmt.Sprintf("Failed to marshal connector request: %v", err))
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			fmt.Println("Failed to send POST request:", err)
			return
		}
		defer resp.Body.Close()

		// Check the response status code
		if resp.StatusCode == http.StatusCreated {
			fmt.Println("Connector created successfully")
		} else if resp.StatusCode == http.StatusBadRequest {
			// Handle bad request error
			fmt.Println("Failed to create connector(bad request). Status code:", resp.StatusCode)

			// Read the response body for error details
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("Failed to read response body:", err)
				return
			}

			fmt.Println("Response body:", string(body))
		} else {
			fmt.Println("Failed to create connector. Status code:", resp)
		}
	}

	// create a producer which will produce the messages
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": "127.0.0.1:29092",
		})

	if err != nil {
		log.Panic("err connecting to kafka: ", err)
	}

	defer producer.Close()

	go func() {
		for event := range producer.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error delivering the order '%s'\n", message.Key)
				} else {
					fmt.Printf("Reading sent to the partition %d with offset %d. \n",
						message.TopicPartition.Partition, message.TopicPartition.Offset)
				}
			}
		}
	}()

	count := 0

	// for jsonConverter
	person := Person{
		Name: "Annie Blue",
		Age:  54,
	}

	message, err := json.Marshal(person)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	var key string

	for {
		count++
		key = fmt.Sprintf("key-%v", count)

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny},
			Key:   []byte(key),
			Value: message}, nil)

		time.Sleep(1000 * time.Millisecond)
		fmt.Println("sent....")
	}
}

// > db.getCollection("my-collection").find()
//  "_id" : ObjectId("64ba529be8e3083890f841f4"), "name" : "John Doe", "age" : NumberLong(30) }
// { "_id" : ObjectId("64ba529ce8e3083890f841f5"), "name" : "John Doe", "age" : NumberLong(30) }
// { "_id" : ObjectId("64ba529de8e3083890f841f6"), "name" : "John Doe", "age" : NumberLong(30) }
// { "_id" : ObjectId("64ba531de8e3083890f841f7"), "name" : "Annie Blue", "age" : NumberLong(54) }
// { "_id" : ObjectId("64ba531ee8e3083890f841f8"), "name" : "Annie Blue", "age" : NumberLong(54) }
// { "_id" : ObjectId("64ba531fe8e3083890f841f9"), "name" : "Annie Blue", "age" : NumberLong(54) }
// { "_id" : ObjectId("64ba5320e8e3083890f841fa"), "name" : "Annie Blue", "age" : NumberLong(54) }

// func setTopic() {
// 	fmt.Println("in the set topic")
//
// 	// admin create a topic
// 	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
// 	if err != nil {
// 		fmt.Printf("Failed to create Admin client: %s\n", err)
// 		os.Exit(1)
// 	}
//
// 	// Contexts are used to abort or limit the amount of time
// 	// the Admin call blocks waiting for a result.
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
//
// 	// Create topics on cluster.
// 	// Set Admin options to wait for the operation to finish (or at most 60s)
// 	maxDur, err := time.ParseDuration("60s")
// 	if err != nil {
// 		panic("ParseDuration(60s)")
// 	}
// 	results, err := a.CreateTopics(
// 		ctx,
// 		// Multiple topics can be created simultaneously
// 		// by providing more TopicSpecification structs here.
// 		[]kafka.TopicSpecification{{
// 			Topic:             TopicName,
// 			NumPartitions:     4,
// 			ReplicationFactor: 1,
// 			Config:            map[string]string{"replication.factor": "1"},
// 		}},
// 		// Admin options
// 		kafka.SetAdminOperationTimeout(maxDur))
// 	if err != nil {
// 		fmt.Printf("Failed to create topic: %v\n", err)
// 		os.Exit(1)
// 	} else {
// 		fmt.Println("topic created: ", TopicName)
// 	}
//
// 	fmt.Println("after topics created: ")
//
// 	// Print results
// 	for _, result := range results {
// 		fmt.Printf("%s\n", result)
// 	}
//
// 	fmt.Println("after topics created end: ")
//
// 	a.Close()
// }
