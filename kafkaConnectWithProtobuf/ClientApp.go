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
	// "github.com/confluentinc/confluent-kafka-go/v2/kafka/rest"
	// "github.com/elodina/go-kafka-avro"
	// goavro "github.com/linkedin/goavro/v2"
	"github.com/golang/protobuf/proto"
	// "github.com/linkedin/goavro/v2/schema"
	// "github.com/golang/protobuf/proto"
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

// type Record struct {
// 	Name string `json:"name"`
// 	Age  int    `json:"age"`
// }

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

	// use kafka connect to execute the kafka connect pipeline
	// client, err := rest.NewClient(&rest.ClientConfig{
	// 	// URL: "http://127.0.0.1:29092",
	// 	URL: "http://127.0.0.1:8083",
	// })
	// if err != nil {
	// 	panic(fmt.Sprintf("Failed to create Kafka Connect client: %v", err))
	// }

	// taskConfig := rest.TaskConfig{
	taskConfig := map[string]interface{}{
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
		"topics":          "my-topic",
		// "transforms":                   "unwrapField",
		// "transforms.unwrapField.type":  "org.apache.kafka.connect.transforms.ExtractField$Value",
		// "transforms.unwrapField.field": "Person",
		"value.converter.subject.name.strategy":  "io.confluent.kafka.serializers.subject.TopicNameStrategy",
		"transforms.unwrap.drop.invalid.message": "true",
		"key.converter":                          "org.apache.kafka.connect.converters.ByteArrayConverter",
		// "key.converter.schema.registry.url":"http://schema-registry:8081",
		"value.converter.schema.registry.url": "http://schema-registry:8081",
		"value.converter":                     "io.confluent.connect.protobuf.ProtobufConverter",
		"connection.uri":                      "mongodb://mongo:27017",
		"database":                            "my-database",
		"collection":                          "my-collection",
		"tasksMax":                            "1",
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

		// Test mytest = 6;
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), "PROTOBUF")
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

		key := "key"

		recordValue := []byte{}
		// recordValue := []byte("that is a test de fou")

		recordValue = append(recordValue, byte(0))
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		recordValue = append(recordValue, schemaIDBytes...)
		messageIndexBytes := []byte{byte(2), byte(0)}
		recordValue = append(recordValue, messageIndexBytes...)

		valueBytes, _ := proto.Marshal(&msg)
		recordValue = append(recordValue, valueBytes...)

		fmt.Println("see topic: ", topic)
		fmt.Println("see key: ", key)
		fmt.Println("see value: ", string(recordValue))

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny},
			Value: recordValue}, nil)
		// Key: []byte(key), Value: recordValue}, nil)

		time.Sleep(1000 * time.Millisecond)
		fmt.Println("sent....")
	}
}

/**************************************************/
/******************** Consumer ********************/
/**************************************************/

// func consumer(props map[string]string, topic string) {
// func consumer(topic string) {
//
// 	setTopic()
//
// 	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")
//
// 	schemaRegistryClient.CodecCreationEnabled(false)
//
// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers": "127.0.0.1:29092",
// 		"group.id":          "myGroup",
// 		"auto.offset.reset": "earliest",
// 	})
// 	defer c.Close()
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	// c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil)
//
// 	fmt.Println("bf the subscribe topic: ", topic)
//
// 	c.SubscribeTopics([]string{topic}, nil)
//
// 	run := true
//
// 	fmt.Println("bf the loop")
//
// 	for run {
// 		fmt.Println("grrr")
//
// 		record, err := c.ReadMessage(-1)
// 		if err == nil {
// 			// sensorReading := &pb.SensorReading{}
// 			msg := &pb.Person{}
// 			// err = proto.Unmarshal(record.Value[7:], sensorReading)
// 			err = proto.Unmarshal(record.Value[7:], msg)
// 			if err != nil {
// 				panic(fmt.Sprintf("Error deserializing the record: %s", err))
// 			}
// 			fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))
// 			fmt.Println("seeeee: ", msg)
// 			// fmt.Println("seeeee: ", msg.Name, "-", msg.Firstname, " / ", msg.Age, " / ", msg.CodePostal, "\n", msg.Mytest.Text)
// 			// fmt.Printf("SensorReading[device=%s, dateTime=%d, reading=%f]\n",
// 			// 	sensorReading.Device.GetDeviceID(),
// 			// 	sensorReading.GetDateTime(),
// 			// 	sensorReading.GetReading())
// 		} else if err.(kafka.Error).IsFatal() {
// 			// fmt.Println(err)
// 			log.Printf("Consumer error: %v \n", err)
// 		}
// 	}
// }
//
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