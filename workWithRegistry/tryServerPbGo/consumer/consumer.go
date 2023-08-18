package main

import (
	"fmt"
	"log"
	// "time"

	pb "consumer/api/v1/proto"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	schemaFile            string = "./api/v1/proto/Person.proto"
	consumerGroupID              = "test-consumer"
	defaultSessionTimeout        = 6000
	noTimeout                    = -1
)

const (
	topic    = "my-topic"
	kafkaURL = "127.0.0.1:29092"
	srURL    = "http://127.0.0.1:8081"
)

func main() {

	consumer, err := NewConsumer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	messageType := (&pb.Person{}).ProtoReflect().Type()
	err = consumer.Run(messageType, topic)
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}
}

// SRConsumer interface
type SRConsumer interface {
	Run(messageType protoreflect.MessageType, topic string) error
	Close()
}

type srConsumer struct {
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           consumerGroupID,
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &srConsumer{
		consumer:     c,
		deserializer: d,
	}, nil
}

// RegisterMessage add simpleHandler and register schema in SR
func (c *srConsumer) RegisterMessage(messageType protoreflect.MessageType) error {
	return nil
}

// Run consumer
func (c *srConsumer) Run(messageType protoreflect.MessageType, topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}
	if err := c.deserializer.ProtoRegistry.RegisterMessage(messageType); err != nil {
		return err
	}
	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		// the good point in that 2 next method is that the deserializer
		// use the SR behing the scene to deserialize the []byte
		// SO THE MESSAGE IS VALIDATED AGAINST THE SR RECORD !!!!
		// and I can use the Person.pb.go from the producer, that's excelent !!
		// TODO find a way to download/set a single type.pb.go into all client
		// deserialize into an interface{}
		msg, err := c.deserializer.Deserialize(topic, kafkaMsg.Value)
		if err != nil {
			return err
		}
		c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// deserialize into person struct
		person := &pb.Person{}
		err = c.deserializer.DeserializeInto(topic, kafkaMsg.Value, person)
		if err != nil {
			return err
		}
		fmt.Println("grrrr: ", person.Name, " - ", person.Age)

		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}

func (c *srConsumer) handleMessageAsInterface(message interface{}, offset int64) {
	fmt.Printf("message %v with offset %d\n", message, offset)

}

// Close all connections
func (c *srConsumer) Close() {
	if err := c.consumer.Close(); err != nil {
		log.Fatal(err)
	}
	c.deserializer.Close()
}
