package main

import (
	"errors"
	pb "examples/api/v1/proto"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	// schemaregistry "github.com/djedjethai/gokfk-regent"
	// "github.com/djedjethai/gokfk-regent/serde"
	// "github.com/djedjethai/gokfk-regent/serde/protobuf"
	"google.golang.org/protobuf/proto"
	// "google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"time"
)

const (
	producerMode string = "producer"
	consumerMode string = "consumer"
	nullOffset          = -1
	topic               = "my-topic"
	// kafkaURL                     = "127.0.0.1:29092"
	kafkaURL                     = "127.0.0.1:9093"
	srURL                        = "http://127.0.0.1:8081"
	schemaFile            string = "./api/v1/proto/Person.proto"
	consumerGroupID              = "test-consumer"
	defaultSessionTimeout        = 6000
	noTimeout                    = -1
	subjectPerson                = "test.v1.Person"
	subjectAddress               = "another.v1.Address"
)

func main() {

	clientMode := os.Args[1]

	if strings.Compare(clientMode, producerMode) == 0 {
		producer()
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		consumer()
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

func producer() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        kafkaURL,
		"security.protocol":        "ssl",
		"ssl.ca.location":          "/certs/ca.pem",     // Path to CA certificate
		"ssl.certificate.location": "/certs/client.pem", // Path to client certificate
		"ssl.key.location":         "/certs/client-key.pem",
	})
	if err != nil {
		log.Fatal(err)
	}

	msg := &pb.Person{
		Name: "robert",
		Age:  23,
	}

	// city := &pb.Address{
	// 	Street: "myStreet",
	// 	City:   "Bangkok",
	// }

	for {
		offset, err := produceMessage(msg, topic, subjectPerson, p)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		// offset, err = produceMessage(city, topic, subjectAddress, p)
		// if err != nil {
		// 	log.Println("Error producing Message: ", err)
		// }

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg proto.Message, topic, subject string) (int64, error)
	Close()
}

// ProduceMessage sends serialized message to kafka using schema registry
func produceMessage(msg proto.Message, topic string, subject string, p *kafka.Producer) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	// Serialize the protobuf message to bytes
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return 1, err
	}

	if err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: msgBytes,
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

/*
* ===============================
* CONSUMER
* ===============================
**/

var person = &pb.Person{}
var address = &pb.Address{}

func consumer() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        kafkaURL,
		"group.id":                 consumerGroupID,
		"session.timeout.ms":       defaultSessionTimeout,
		"enable.auto.commit":       false,
		"security.protocol":        "ssl",
		"ssl.ca.location":          "./certs/ca.pem",     // Path to CA certificate
		"ssl.certificate.location": "./certs/client.pem", // Path to client certificate
		"ssl.key.location":         "./certs/client-key.pem",
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatal(err)
	}

	// register the MessageFactory is facultatif
	// but is it useful to allow the event receiver to be an initialized object
	// c.deserializer.MessageFactory = c.RegisterMessageFactory()

	for {
		kafkaMsg, err := c.ReadMessage(noTimeout)
		if err != nil {
			log.Println(err)
		}

		// Deserialize the Kafka message into your protobuf message
		var msg pb.Person // Replace with the actual protobuf message type
		err = proto.Unmarshal(kafkaMsg.Value, &msg)
		if err != nil {
			log.Println("Error deserializing the message:", err)
			continue // Skip this message and continue with the next
		}

		// without RegisterMessageFactory()
		handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// // with RegisterMessageFactory()
		// if _, ok := msg.(*pb.Person); ok {
		// 	fmt.Println("Person: ", msg.(*pb.Person).Name, " - ", msg.(*pb.Person).Age)
		// } else {

		// 	fmt.Println("Address: ", msg.(*pb.Address).City, " - ", msg.(*pb.Address).Street)
		// }

		// // Deserialize into a struct
		// // receivers for DeserializeIntoRecordName
		// subjects := make(map[string]interface{})
		// subjects[subjectPerson] = person
		// subjects[subjectAddress] = address
		// err = c.deserializer.DeserializeIntoRecordName(subjects, kafkaMsg.Value)
		// if err != nil {
		// 	return err
		// }

		// fmt.Println("person: ", person.Name, " - ", person.Age)
		// fmt.Println("address: ", address.City, " - ", address.Street)

		if _, err = c.CommitMessage(kafkaMsg); err != nil {
			log.Println(err)
		}
	}
}

// RegisterMessageFactory will overwrite the default one
// In this case &pb.Person{} is the "msg" at "msg, err := c.deserializer.DeserializeRecordName()"
func RegisterMessageFactory() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case subjectPerson:
			return &pb.Person{}, nil
		case subjectAddress:
			return &pb.Address{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

func handleMessageAsInterface(message interface{}, offset int64) {
	fmt.Printf("message %v with offset %d\n", message, offset)
}

func Close(c *kafka.Consumer) {
	if err := c.Close(); err != nil {
		log.Fatal(err)
	}
}
