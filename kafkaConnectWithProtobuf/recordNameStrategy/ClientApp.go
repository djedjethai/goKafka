package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	pb "getting-started-with-ccloud-golang/api/v1/proto"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
	"github.com/riferrei/srclient"
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

// const topicMessage = "message"

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

/**************************************************/
/******************** Producer ********************/
/**************************************************/

// func producer(props map[string]string, topic string) {
func producer(topic string) {

	// taskConfig := rest.TaskConfig{
	taskConfig := map[string]interface{}{
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
		"topics":          "my-topic",
		// "transforms":                   "unwrapField",
		// "transforms.unwrapField.type":  "org.apache.kafka.connect.transforms.ExtractField$Value",
		// "transforms.unwrapField.field": "Person",
		// "value.converter.subject.name.strategy":  "io.confluent.kafka.serializers.subject.TopicNameStrategy",
		// "transforms.unwrap.drop.invalid.message": "true",
		"key.converter.schemas.enable": "false",
		"key.converter":                "org.apache.kafka.connect.storage.StringConverter",
		// "key.converter.schema.registry.url":"http://schema-registry:8081",
		"value.converter.schema.registry.url":   "http://schema-registry:8081",
		"value.converter":                       "io.confluent.connect.protobuf.ProtobufConverter",
		"value.converter.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
		// "value.converter.schema.name":           "io.confluent.cloud.demo.domain1.Person",
		"connection.uri": "mongodb://mongo:27017",
		"database":       "my-database",
		"collection":     "my-collection",
		"tasksMax":       "1",
	}

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

	schemaRegistryURL := "http://127.0.0.1:8081"

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	schema, err := schemaRegistryClient.GetLatestSchema(topic)
	if err != nil {
		fmt.Println("Error retrieving schema: ", err)
	}

	// 	// // !!! if I need to update the shema version => recreate the schema
	// // what ever the changement in the schema, at the time it's recreated
	// // the old schema is replaced by the new one and version increase +1
	// // !!! BUT !!! already registered fields can not be modify(can add fields only)
	schema = nil
	if schema == nil {
		// var b bool = false
		// schemaBytes, _ := ioutil.ReadFile(schemaFile)

		schemaBytes := `
			syntax = "proto3";

			package io.confluent.cloud.demo.domain1;

			option go_package = "getting-started-with-ccloud-golang/api/v1/proto";

			message Person {
				string name = 1;
				float age = 2;
				string address = 3;
				int32 code_postal = 4;
				string firstname = 5;
				Test mytest = 6;
			};

			message Test{
				string text = 1;
			}`

		// message Address{
		// 	string country = 1;
		// 	string town = 2;
		// }`

		subjectName := "io.confluent.cloud.demo.domain1.Person-value"
		// subjectName1 := "io.confluent.cloud.demo.domain1.Address-value"

		// define 2 schema on the same topic
		schema, err = schemaRegistryClient.CreateSchema(subjectName, string(schemaBytes), "PROTOBUF")
		// schema, err = schemaRegistryClient.CreateSchema(subjectName1, string(schemaBytes), "PROTOBUF")
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
		fmt.Println("look like schema has been created...")
	}

	for {

		tt := &pb.Test{Text: "this a is a good test"}

		msg := pb.Person{
			Name:       "robert",
			Age:        23,
			Address:    "the address",
			CodePostal: 10111,
			Firstname:  "simon",
			Mytest:     tt,
		}

		// key := "key"

		recordValue := []byte{}

		recordValue = append(recordValue, byte(0))
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		recordValue = append(recordValue, schemaIDBytes...)
		messageIndexBytes := []byte{byte(2), byte(0)}
		recordValue = append(recordValue, messageIndexBytes...)

		valueBytes, _ := proto.Marshal(&msg)
		recordValue = append(recordValue, valueBytes...)

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny},
			Value: recordValue}, nil)
		// Key: []byte(key), Value: recordValue}, nil)

		time.Sleep(1000 * time.Millisecond)
		fmt.Println("sent....")
	}
}

// NOTE !!! that works but events must be produce events to the topic
// my-topic-value as the connector add -value(no idea why...).
// in DB
// > show collections
// my-collection
// > db.getCollection("my-collection").find()
// { "_id" : ObjectId("64bb8d0a9fe5c7589d9d656c"), "name" : "robert", "age" : 23, "address" : "the address", "code_postal" : 10111, "firstname" : "simon", "mytest" : { "text" : "this a is a good test" } }
// { "_id" : ObjectId("64bb8d0a9fe5c7589d9d656d"), "name" : "robert", "age" : 23, "address" : "the address", "code_postal" : 10111, "firstname" : "simon", "mytest" : { "text" : "this a is a good test" } }
// { "_id" : ObjectId("64bb8d0a9fe5c7589d9d656e"), "name" : "robert", "age" : 23, "address" : "the address", "code_postal" : 10111, "firstname" : "simon", "mytest" : { "text" : "this a is a good test" } }

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
