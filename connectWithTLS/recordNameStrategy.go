package main

import (
	"context"
	// "crypto/tls"
	// "crypto/x509"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io/ioutil"
	"os"
	"os/signal"
	pb "paris/api/v1/proto"
	"strings"
	"syscall"
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
	createMode   string = "create"
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
	// Paths to certificate files
	producerCertPath = "./tmp/datahub-ca.crt"
	producerKeyPath  = "./tmp/datahub-ca.key"
	caCertPath       = "./tmp/broker-ca-signed.crt"
	// caCertPath = "./tmp/datahub-ca.crt"
)

func main() {

	clientMode := os.Args[1]

	if strings.Compare(clientMode, producerMode) == 0 {
		producer()
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		consumer()
	} else if strings.Compare(clientMode, createMode) == 0 {
		create()
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

func create() {

	// Load client certificate and private key
	cert, err := ioutil.ReadFile(producerCertPath)
	if err != nil {
		fmt.Println("Error reading client certificate:", err)
		return
	}

	key, err := ioutil.ReadFile(producerKeyPath)
	if err != nil {
		fmt.Println("Error reading private key:", err)
		return
	}

	// // Load CA certificate
	// caCert, err := ioutil.ReadFile(caCertPath)
	// if err != nil {
	// 	fmt.Println("Error loading CA certificate:", err)
	// 	return
	// }

	// Create a new admin client with SSL configuration
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers":   kafkaURL,
		"security.protocol":   "ssl",
		"ssl.certificate.pem": string(cert),
		"ssl.key.pem":         string(key),
		"ssl.ca.location":     caCertPath,
		"ssl.key.password":    "datahub",
	})

	if err != nil {
		fmt.Printf("Error creating admin client: %v\n", err)
		return
	}

	defer adminClient.Close()

	// Specify the topic configuration
	topicConfig := &kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	// Create the topic
	ctx := context.Background()
	_, err = adminClient.CreateTopics(ctx, []kafka.TopicSpecification{*topicConfig}, nil)
	if err != nil {
		fmt.Printf("Error creating topic: %v\n", err)
		return
	}

	fmt.Println("Topic created successfully")
}

func producer() {

	// keytool -importkeystore -srckeystore producer.keystore.jks -destkeystore producer.p12 -deststoretype PKCS12
	// openssl pkcs12 -in producer.p12 -nokeys -out producer.cer.pem
	// openssl pkcs12 -in producer.p12 -nodes -nocerts -out producer.key.pem
	pathKeyProducer := "/home/jerome/Documents/code/goKafka/studyKafkaWithConfluentInc/connectWithTLS/secrets/producer.key.pem"
	pathCertProducer := "/home/jerome/Documents/code/goKafka/studyKafkaWithConfluentInc/connectWithTLS/secrets/producer.cer.pem"

	// Create a new producer instance
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        kafkaURL,
		"security.protocol":        "ssl",
		"ssl.key.location":         pathKeyProducer,
		"ssl.key.password":         "datahub",
		"ssl.certificate.location": pathCertProducer,
		// selfSigned cert the cert is the ca as well
		"ssl.ca.location":                     "./secrets/broker.cer.pem",
		"enable.ssl.certificate.verification": false,
		"debug":                               "security,broker",
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

// NOTE to extract certif from keystore(cert and key) and truststore(ca)
// NOTE the broker.truststore.jks hold the ca of the broker,
// which is. as it's a self signed certif the cert of the broker...
// keytool -importkeystore -srckeystore broker.truststore.jks -destkeystore server.p12 -deststoretype PKCS12
// openssl pkcs12 -in server.p12 -nokeys -out broker.cer.pem

// keytool -importkeystore -srckeystore consumer.keystore.jks -destkeystore consumer.p12 -deststoretype PKCS12
// openssl pkcs12 -in consumer.p12 -nokeys -out consumer.cer.pem
// openssl pkcs12 -in consumer.p12 -nodes -nocerts -out consumer.key.pem

func consumer() {

	pathKey := "/home/jerome/Documents/code/goKafka/studyKafkaWithConfluentInc/connectWithTLS/secrets/consumer.key.pem"
	pathCert := "/home/jerome/Documents/code/goKafka/studyKafkaWithConfluentInc/connectWithTLS/secrets/consumer.cer.pem"

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                   kafkaURL,
		"group.id":                            consumerGroupID,
		"session.timeout.ms":                  defaultSessionTimeout,
		"enable.auto.commit":                  false,
		"security.protocol":                   "ssl",
		"ssl.key.location":                    pathKey,
		"ssl.certificate.location":            pathCert,
		"ssl.ca.location":                     "./secrets/broker.cer.pem",
		"ssl.key.password":                    "datahub",
		"enable.ssl.certificate.verification": false,
		"debug":                               "security,broker",
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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func(*kafka.Consumer) {
		sig := <-signals
		fmt.Println("see signals: ", sig)

		offset, _ := c.Commit()
		fmt.Println("see offset: ", offset)

		os.Exit(0)
	}(c)

	run := true

	for run {
		select {
		case <-signals:
			break
		default:
			ev := c.Poll(1000)
			switch e := ev.(type) {
			case *kafka.Message:
				// var msg pb.Person // Replace with the actual protobuf message type
				fmt.Println("see e.Value: ", string(e.Value))

				offset, _ := c.Commit()
				fmt.Println("commited affset: ", offset)

			}
		}
	}

	// for {
	// 	kafkaMsg, err := c.ReadMessage(noTimeout)
	// 	if err != nil {
	// 		log.Println(err)
	// 	}

	// 	// Deserialize the Kafka message into your protobuf message
	// 	err = proto.Unmarshal(kafkaMsg.Value, &msg)
	// 	if err != nil {
	// 		log.Println("Error deserializing the message:", err)
	// 		continue // Skip this message and continue with the next
	// 	}

	// 	// without RegisterMessageFactory()
	// 	handleMessageAsInterface(&msg, int64(kafkaMsg.TopicPartition.Offset))

	// 	// // with RegisterMessageFactory()
	// 	// if _, ok := msg.(*pb.Person); ok {
	// 	// 	fmt.Println("Person: ", msg.(*pb.Person).Name, " - ", msg.(*pb.Person).Age)
	// 	// } else {

	// 	// 	fmt.Println("Address: ", msg.(*pb.Address).City, " - ", msg.(*pb.Address).Street)
	// 	// }

	// 	// // Deserialize into a struct
	// 	// // receivers for DeserializeIntoRecordName
	// 	// subjects := make(map[string]interface{})
	// 	// subjects[subjectPerson] = person
	// 	// subjects[subjectAddress] = address
	// 	// err = c.deserializer.DeserializeIntoRecordName(subjects, kafkaMsg.Value)
	// 	// if err != nil {
	// 	// 	return err
	// 	// }

	// 	// fmt.Println("person: ", person.Name, " - ", person.Age)
	// 	// fmt.Println("address: ", address.City, " - ", address.Street)

	// 	if _, err = c.CommitMessage(kafkaMsg); err != nil {
	// 		log.Println(err)
	// 	}
	// }
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
