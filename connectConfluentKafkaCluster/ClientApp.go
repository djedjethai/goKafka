package main

import (
	"encoding/binary"
	"fmt"
	// "io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	pb "getting-started-with-ccloud-golang/api/v1/proto"
	"github.com/golang/protobuf/proto"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	producerMode string = "producer"
	consumerMode string = "consumer"
	schemaFile   string = "./api/v1/proto/SensorReading.proto"
	// messageFile  string = "./api/v1/proto/Message.proto"
)

var devices = []*SensorReading_Device{
	{
		DeviceID: NewUUID(),
		Enabled:  true,
	},
	{
		DeviceID: NewUUID(),
		Enabled:  true,
	},
	{
		DeviceID: NewUUID(),
		Enabled:  true,
	},
	{
		DeviceID: NewUUID(),
		Enabled:  true,
	},
	{
		DeviceID: NewUUID(),
		Enabled:  true,
	},
}

// const topicMessage = "message"

func main() {

	clientMode := os.Args[1]
	props := LoadProperties()
	topic := TopicName

	if strings.Compare(clientMode, producerMode) == 0 {
		producer(props, topic)
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		consumer(props, topic)
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}

}

/**************************************************/
/******************** Producer ********************/
/**************************************************/

func producer(props map[string]string, topic string) {

	CreateTopic(props)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": props["bootstrap.servers"],
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     props["sasl.username"],
		"sasl.password":     props["sasl.password"]})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer %s", err))
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

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(props["schema.registry.url"])
	schemaRegistryClient.CodecCreationEnabled(false)
	srBasicAuthUserInfo := props["schema.registry.basic.auth.user.info"]
	credentials := strings.Split(srBasicAuthUserInfo, ":")
	schemaRegistryClient.SetCredentials(credentials[0], credentials[1])

	// schema, err := schemaRegistryClient.GetLatestSchema(topic, false)
	schema, err := schemaRegistryClient.GetLatestSchema(topic)
	if schema == nil {
		// var b bool = false
		// schemaBytes, _ := ioutil.ReadFile(schemaFile)
		schemaBytes := []byte(
			`
			syntax = "proto3";

			package io.confluent.cloud.demo.domain1;

			option go_package = "getting-started-with-ccloud-golang/api/v1/proto";

			message Message {
				string text = 1;
			}`,
		)
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), "PROTOBUF")
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}

	for {

		msg := pb.Message{
			Text: "arrrrrchhhhhhhhhhh",
		}

		choosen := rand.Intn(len(devices))
		if choosen == 0 {
			choosen = 1
		}
		deviceSelected := devices[choosen-1]

		key := deviceSelected.DeviceID
		// sensorReading := SensorReading{
		// 	Device:   deviceSelected,
		// 	DateTime: time.Now().UnixNano(),
		// 	Reading:  rand.Float64(),
		// }

		recordValue := []byte{}

		// The code below is only necessary if we want to deserialize records
		// using Java via Confluent's deserializer implementation:
		// [io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer]
		// Therefore, we need to arrange the bytes in the following format:
		// [magicByte] + [schemaID] + [messageIndex] + [value]
		recordValue = append(recordValue, byte(0))
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		recordValue = append(recordValue, schemaIDBytes...)
		messageIndexBytes := []byte{byte(2), byte(0)}
		recordValue = append(recordValue, messageIndexBytes...)

		// Now write the bytes from the actual value...
		// valueBytes, _ := proto.Marshal(&sensorReading)
		valueBytes, _ := proto.Marshal(&msg)
		recordValue = append(recordValue, valueBytes...)

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic, Partition: kafka.PartitionAny},
			Key: []byte(key), Value: recordValue}, nil)

		time.Sleep(1000 * time.Millisecond)

	}

}

/**************************************************/
/******************** Consumer ********************/
/**************************************************/

func consumer(props map[string]string, topic string) {

	CreateTopic(props)

	// Code below has been commented out because in Go there is no
	// need to have the schema to be able to deserialize the record.
	// Thus keeping the code here for future use ¯\_(ツ)_/¯

	// schemaRegistryClient := srclient.CreateSchemaRegistryClient(props["schema.registry.url"])
	// schemaRegistryClient.CodecCreationEnabled(false)
	// srBasicAuthUserInfo := props["schema.registry.basic.auth.user.info"]
	// credentials := strings.Split(srBasicAuthUserInfo, ":")
	// schemaRegistryClient.SetCredentials(credentials[0], credentials[1])

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  props["bootstrap.servers"],
		"sasl.mechanisms":    props["sasl.mechanisms"],
		"security.protocol":  props["security.protocol"],
		"sasl.username":      props["sasl.username"],
		"sasl.password":      props["sasl.password"],
		"session.timeout.ms": 6000,
		"group.id":           "golang-consumer",
		"auto.offset.reset":  "latest"})
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer %s", err))
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{topic}, nil)

	for {
		record, err := consumer.ReadMessage(-1)
		if err == nil {
			// sensorReading := &pb.SensorReading{}
			msg := &pb.Message{}
			// err = proto.Unmarshal(record.Value[7:], sensorReading)
			err = proto.Unmarshal(record.Value[7:], msg)
			if err != nil {
				panic(fmt.Sprintf("Error deserializing the record: %s", err))
			}
			fmt.Println("seeeee: ", msg.Text)
			// fmt.Printf("SensorReading[device=%s, dateTime=%d, reading=%f]\n",
			// 	sensorReading.Device.GetDeviceID(),
			// 	sensorReading.GetDateTime(),
			// 	sensorReading.GetReading())
		} else {
			fmt.Println(err)
		}
	}
}
