package main

import (
	"fmt"
	"log"
	// "time"

	pb "consumer/api/v1/name"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	// "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	schemaregistry "github.com/djedjethai/kfk-schemaregistry"
	"github.com/djedjethai/kfk-schemaregistry/serde"
	"github.com/djedjethai/kfk-schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	producerMode          string = "producer"
	consumerMode          string = "consumer"
	nullOffset                   = -1
	topic                        = "my-topic"
	kafkaURL                     = "127.0.0.1:29092"
	srURL                        = "http://127.0.0.1:8081"
	schemaFile            string = "./api/v1/name/Person.proto"
	consumerGroupID              = "test-consumer"
	defaultSessionTimeout        = 6000
	noTimeout                    = -1
	subjectPerson                = "v1.name.Person"
	subjectAddress               = "v1.name.Address"
)

func main() {

	consumer()
}

/*
* ===============================
* CONSUMER
* ===============================
**/

var person = &pb.Person{}
var address = &pb.Address{}

func consumer() {
	consumer, err := NewConsumer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	personType := (&pb.Person{}).ProtoReflect().Type()
	addressType := (&pb.Address{}).ProtoReflect().Type()

	// // declare the events' subjects name expected
	// // works with DeserializeRecordName only, will fail with DeserializeIntoRecordName
	// subjects := make(map[string]interface{})
	// subjects[subjectPerson] = struct{}{}
	// subjects[subjectAddress] = struct{}{}

	// Deserialize into a struct
	// works with DeserializeRecordName and DeserializeIntoRecordName
	subjects := make(map[string]interface{})
	subjects[subjectPerson] = person
	subjects[subjectAddress] = address

	err = consumer.Run([]protoreflect.MessageType{personType, addressType}, topic, subjects)
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}

}

// SRConsumer interface
type SRConsumer interface {
	Run(messagesType []protoreflect.MessageType, topic string, subjects map[string]interface{}) error
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
func (c *srConsumer) Run(messagesType []protoreflect.MessageType, topic string, subjects map[string]interface{}) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}

	if len(messagesType) > 0 {
		for _, mt := range messagesType {
			if err := c.deserializer.ProtoRegistry.RegisterMessage(mt); err != nil {

				return err
			}
		}
	}

	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		msg, err := c.deserializer.DeserializeRecordName(subjects, kafkaMsg.Value)
		if err != nil {
			return err
		}
		c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// // could instanciate a second map(or overwrite the previous one)
		// subjects := make(map[string]interface{})
		// person := &pb.Person{}
		// subjects[subjectPerson] = person
		// address := &pb.Address{}
		// subjects[subjectAddress] = address

		err = c.deserializer.DeserializeIntoRecordName(subjects, kafkaMsg.Value)
		if err != nil {
			return err
		}

		fmt.Println("person: ", person.Name, " - ", person.Age)
		fmt.Println("message: ", address.Street, " - ", address.City)

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

// ==========================================================
// func producer() {
// 	producer, err := NewProducer(kafkaURL, srURL)
// 	if err != nil {
// 		log.Fatal("Can not create producer: ", err)
// 	}
//
// 	msg := &pb.Person{
// 		Name: "robert",
// 		Age:  23,
// 	}
//
// 	city := &pb.Address{
// 		Street: "myStreet",
// 		City:   "Bangkok",
// 	}
//
// 	for {
// 		offset, err := producer.ProduceMessage(msg, topic, subjectPerson)
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		offset, err = producer.ProduceMessage(city, topic, subjectAddress)
// 		if err != nil {
// 			log.Println("Error producing Message: ", err)
// 		}
//
// 		log.Println("Message produced, offset is: ", offset)
// 		time.Sleep(2 * time.Second)
// 	}
// }
//
// // SRProducer interface
// type SRProducer interface {
// 	ProduceMessage(msg proto.Message, topic, subject string) (int64, error)
// 	Close()
// }
//
// type srProducer struct {
// 	producer   *kafka.Producer
// 	serializer serde.Serializer
// }
//
// // NewProducer returns kafka producer with schema registry
// func NewProducer(kafkaURL, srURL string) (SRProducer, error) {
// 	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
// 	if err != nil {
// 		return nil, err
// 	}
// 	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
// 	if err != nil {
// 		return nil, err
// 	}
// 	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &srProducer{
// 		producer:   p,
// 		serializer: s,
// 	}, nil
// }
//
// // ProduceMessage sends serialized message to kafka using schema registry
// func (p *srProducer) ProduceMessage(msg proto.Message, topic, subject string) (int64, error) {
// 	kafkaChan := make(chan kafka.Event)
// 	defer close(kafkaChan)
//
// 	payload, err := p.serializer.Serialize(subject, msg)
// 	if err != nil {
// 		return nullOffset, err
// 	}
// 	if err = p.producer.Produce(&kafka.Message{
// 		TopicPartition: kafka.TopicPartition{Topic: &topic},
// 		Value:          payload,
// 	}, kafkaChan); err != nil {
// 		return nullOffset, err
// 	}
// 	e := <-kafkaChan
// 	switch ev := e.(type) {
// 	case *kafka.Message:
// 		log.Println("message sent: ", string(ev.Value))
// 		return int64(ev.TopicPartition.Offset), nil
// 	case kafka.Error:
// 		return nullOffset, err
// 	}
// 	return nullOffset, nil
// }
//
// // Close schema registry and Kafka
// func (p *srProducer) Close() {
// 	p.serializer.Close()
// 	p.producer.Close()
// }
// ==============================================================

// const (
// 	schemaFile            string = "./api/v1/proto/Person.proto"
// 	consumerGroupID              = "test-consumer"
// 	defaultSessionTimeout        = 6000
// 	noTimeout                    = -1
// )
//
// const (
// 	topic    = "my-topic"
// 	kafkaURL = "127.0.0.1:29092"
// 	srURL    = "http://127.0.0.1:8081"
// )
//
// func main() {
//
// 	consumer, err := NewConsumer(kafkaURL, srURL)
// 	if err != nil {
// 		log.Fatal("Can not create producer: ", err)
// 	}
//
// 	messageType := (&pb.Person{}).ProtoReflect().Type()
// 	err = consumer.Run(messageType, topic)
// 	if err != nil {
// 		log.Println("ConsumerRun Error: ", err)
// 	}
// }
//
// // SRConsumer interface
// type SRConsumer interface {
// 	Run(messageType protoreflect.MessageType, topic string) error
// 	Close()
// }
//
// type srConsumer struct {
// 	consumer     *kafka.Consumer
// 	deserializer *protobuf.Deserializer
// }
//
// // NewConsumer returns new consumer with schema registry
// func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {
// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers":  kafkaURL,
// 		"group.id":           consumerGroupID,
// 		"session.timeout.ms": defaultSessionTimeout,
// 		"enable.auto.commit": false,
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &srConsumer{
// 		consumer:     c,
// 		deserializer: d,
// 	}, nil
// }
//
// // RegisterMessage add simpleHandler and register schema in SR
// func (c *srConsumer) RegisterMessage(messageType protoreflect.MessageType) error {
// 	return nil
// }
//
// // Run consumer
// func (c *srConsumer) Run(messageType protoreflect.MessageType, topic string) error {
// 	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
// 		return err
// 	}
// 	if err := c.deserializer.ProtoRegistry.RegisterMessage(messageType); err != nil {
// 		return err
// 	}
// 	for {
// 		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
// 		if err != nil {
// 			return err
// 		}
//
// 		// the good point in that 2 next method is that the deserializer
// 		// use the SR behing the scene to deserialize the []byte
// 		// SO THE MESSAGE IS VALIDATED AGAINST THE SR RECORD !!!!
// 		// and I can use the Person.pb.go from the producer, that's excelent !!
// 		// TODO find a way to download/set a single type.pb.go into all client
// 		// deserialize into an interface{}
// 		msg, err := c.deserializer.Deserialize(topic, kafkaMsg.Value)
// 		if err != nil {
// 			return err
// 		}
// 		c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))
//
// 		// deserialize into person struct
// 		person := &pb.Person{}
// 		err = c.deserializer.DeserializeInto(topic, kafkaMsg.Value, person)
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Println("grrrr: ", person.Name, " - ", person.Age)
//
// 		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
// 			return err
// 		}
// 	}
// }
//
// func (c *srConsumer) handleMessageAsInterface(message interface{}, offset int64) {
// 	fmt.Printf("message %v with offset %d\n", message, offset)
//
// }
//
// // Close all connections
// func (c *srConsumer) Close() {
// 	if err := c.consumer.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// 	c.deserializer.Close()
// }
