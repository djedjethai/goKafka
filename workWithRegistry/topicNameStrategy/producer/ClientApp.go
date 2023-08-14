package main

import (
	"log"
	"time"

	pb "producer/api/v1/proto"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/golang/protobuf/proto"
)

const (
	schemaFile string = "./api/v1/proto/Person.proto"
)

const (
	topic    = "my-topic"
	kafkaURL = "127.0.0.1:29092"
	srURL    = "http://127.0.0.1:8081"
)

func main() {

	producer, err := NewProducer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	msg := &pb.Person{
		Name: "robert",
		Age:  23,
	}

	for {
		offset, err := producer.ProduceMessage(msg, topic)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

const (
	nullOffset = -1
)

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg proto.Message, topic string) (int64, error)
	Close()
}

type srProducer struct {
	producer   *kafka.Producer
	serializer serde.Serializer
}

// NewProducer returns kafka producer with schema registry
func NewProducer(kafkaURL, srURL string) (SRProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		return nil, err
	}
	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}
	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, err
	}
	return &srProducer{
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)
	payload, err := p.serializer.Serialize(topic, msg)
	if err != nil {
		return nullOffset, err
	}
	if err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          payload,
	}, kafkaChan); err != nil {
		return nullOffset, err
	}
	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		log.Println("message sent: ", string(ev.Value))
		return int64(ev.TopicPartition.Offset), nil
	case kafka.Error:
		return nullOffset, err
	}
	return nullOffset, nil
}

// Close schema registry and Kafka
func (p *srProducer) Close() {
	p.serializer.Close()
	p.producer.Close()
}

// /**************************************************/
// /******************** Producer ********************/
// /**************************************************/
//
// // func producer(props map[string]string, topic string) {
// func producer(topic string) {
//
// 	// CreateTopic(props)
//
// 	// producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka:9092"})
// 	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
// 	if err != nil {
// 		log.Panic("err connecting to kafka: ", err)
// 	}
//
// 	defer producer.Close()
//
// 	go func() {
// 		for event := range producer.Events() {
// 			switch ev := event.(type) {
// 			case *kafka.Message:
// 				message := ev
// 				if ev.TopicPartition.Error != nil {
// 					fmt.Printf("Error delivering the order '%s'\n", message.Key)
// 				} else {
// 					fmt.Printf("Reading sent to the partition %d with offset %d. \n",
// 						message.TopicPartition.Partition, message.TopicPartition.Offset)
// 				}
// 			}
// 		}
// 	}()
//
// 	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")
//
// 	schema, err := schemaRegistryClient.GetLatestSchema(topic)
//
// 	fmt.Println("The schema: ", schema)
//
// 	// // !!! if I need to update the shema version => recreate the schema
// 	// // what ever the changement in the schema, at the time it's recreated
// 	// // the old schema is replaced by the new one and version increase +1
// 	// // !!! BUT !!! already registered fields can not be modify(can add fields only)
// 	schema = nil
// 	if schema == nil {
// 		// var b bool = false
// 		// schemaBytes, _ := ioutil.ReadFile(schemaFile)
//
// 		schemaBytes := `
// 			syntax = "proto3";
//
// 			package io.confluent.cloud.demo.domain1;
//
// 			option go_package = "getting-started-with-ccloud-golang/api/v1/proto";
//
// 			message Person {
// 				string name = 1;
// 				float age = 2;
// 				string address = 3;
// 				int32 code_postal = 4;
// 				string firstname = 5;
// 				Test mytest = 6;
// 			};
//
// 			message Test{
// 				string text = 1;
// 			}`
//
// 		// Test mytest = 6;
// 		schema, err = schemaRegistryClient.CreateSchema(schemaName, string(schemaBytes), "PROTOBUF")
// 		if err != nil {
// 			panic(fmt.Sprintf("Error creating the schema %s", err))
// 		}
// 		fmt.Println("look like schema has been created...")
// 	}
//
// 	for {
//
// 		// tt := &pb.Test{Text: "this a is a good test"}
//
// 		msg := pb.Person{
// 			Name:       "robert",
// 			Age:        23,
// 			Address:    "the address",
// 			CodePostal: 10111,
// 			Firstname:  "simon",
// 			// Mytest:     tt,
// 		}
//
// 		key := "key"
//
// 		recordValue := []byte{}
// 		// recordValue := []byte("that is a test de fou")
//
// 		recordValue = append(recordValue, byte(0))
// 		schemaIDBytes := make([]byte, 4)
// 		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
// 		recordValue = append(recordValue, schemaIDBytes...)
// 		messageIndexBytes := []byte{byte(2), byte(0)}
// 		recordValue = append(recordValue, messageIndexBytes...)
//
// 		valueBytes, _ := proto.Marshal(&msg)
// 		recordValue = append(recordValue, valueBytes...)
//
// 		// fmt.Println("see topic: ", topic)
// 		// fmt.Println("see key: ", key)
// 		// fmt.Println("see value: ", string(recordValue))
//
// 		producer.Produce(&kafka.Message{
// 			TopicPartition: kafka.TopicPartition{
// 				Topic: &topic, Partition: kafka.PartitionAny},
// 			Key: []byte(key), Value: recordValue}, nil)
//
// 		time.Sleep(1000 * time.Millisecond)
// 		fmt.Println("sent....")
// 	}
// }
//
//
