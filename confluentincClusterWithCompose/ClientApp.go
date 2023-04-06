package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
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
	schemaFile   string = "./api/v1/proto/Person.proto"
	// schemaFile   string = "./api/v1/proto/SensorReading.proto"
	// messageFile  string = "./api/v1/proto/Message.proto"
	connectToAll string = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
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

type Message struct {
	Text string
}

// const topicMessage = "message"

func main() {

	clientMode := os.Args[1]
	// props := LoadProperties()
	topic := TopicName

	if strings.Compare(clientMode, producerMode) == 0 {
		// producer(props, topic)
		producer(topic)
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		consumer(topic)
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

/**************************************************/
/******************** Producer ********************/
/**************************************************/

// func producer(props map[string]string, topic string) {
func producer(topic string) {

	// CreateTopic(props)

	// producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka:9092"})
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": connectToAll})
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

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")

	schema, err := schemaRegistryClient.GetLatestSchema(topic)

	fmt.Println("The schema: ", schema)

	// // !!! if I need to update the shema version => recreate the schema
	// // what ever the changement in the schema, at the time it's recreated
	// // the old schema is replaced by the new one and version increase +1
	// // !!! BUT !!! already registered fields can not be modify(can add fields only)
	schema = nil
	if schema == nil {
		// var b bool = false
		schemaBytes, _ := ioutil.ReadFile(schemaFile)
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

		choosen := rand.Intn(len(devices))
		if choosen == 0 {
			choosen = 1
		}
		deviceSelected := devices[choosen-1]

		key := deviceSelected.DeviceID

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

// func consumer(props map[string]string, topic string) {
func consumer(topic string) {

	setTopic()

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")

	schemaRegistryClient.CodecCreationEnabled(false)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": connectToAll,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	defer c.Close()

	if err != nil {
		panic(err)
	}

	// c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil)

	c.SubscribeTopics([]string{topic}, nil)

	run := true

	for run {

		record, err := c.ReadMessage(-1)
		if err == nil {
			// sensorReading := &pb.SensorReading{}
			msg := &pb.Person{}
			// err = proto.Unmarshal(record.Value[7:], sensorReading)
			err = proto.Unmarshal(record.Value[7:], msg)
			if err != nil {
				panic(fmt.Sprintf("Error deserializing the record: %s", err))
			}
			fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))
			fmt.Println("seeeee: ", msg)
			// fmt.Println("seeeee: ", msg.Name, "-", msg.Firstname, " / ", msg.Age, " / ", msg.CodePostal, "\n", msg.Mytest.Text)
			// fmt.Printf("SensorReading[device=%s, dateTime=%d, reading=%f]\n",
			// 	sensorReading.Device.GetDeviceID(),
			// 	sensorReading.GetDateTime(),
			// 	sensorReading.GetReading())
		} else if err.(kafka.Error).IsFatal() {
			// fmt.Println(err)
			log.Printf("Consumer error: %v \n", err)
		}
	}
}

func setTopic() {
	// admin create a topic
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": connectToAll})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             TopicName,
			NumPartitions:     4,
			ReplicationFactor: 1,
			Config:            map[string]string{"replication.factor": "1"},
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	a.Close()
}

// package main
//
// import (
// 	"context"
// 	// "encoding/binary"
// 	"fmt"
// 	// "io/ioutil"
// 	"log"
// 	"math/rand"
// 	"os"
// 	"strings"
// 	"time"
//
// 	// pb "getting-started-with-ccloud-golang/api/v1/proto"
// 	// "github.com/golang/protobuf/proto"
// 	// "github.com/riferrei/srclient"
// 	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
// )
//
// const (
// 	producerMode string = "producer"
// 	consumerMode string = "consumer"
// 	schemaFile   string = "./api/v1/proto/Person.proto"
// 	// schemaFile   string = "./api/v1/proto/SensorReading.proto"
// 	// messageFile  string = "./api/v1/proto/Message.proto"
// )
//
// var devices = []*SensorReading_Device{
// 	{
// 		DeviceID: NewUUID(),
// 		Enabled:  true,
// 	},
// 	{
// 		DeviceID: NewUUID(),
// 		Enabled:  true,
// 	},
// 	{
// 		DeviceID: NewUUID(),
// 		Enabled:  true,
// 	},
// 	{
// 		DeviceID: NewUUID(),
// 		Enabled:  true,
// 	},
// 	{
// 		DeviceID: NewUUID(),
// 		Enabled:  true,
// 	},
// }
//
// type Message struct {
// 	Text string
// }
//
// // const topicMessage = "message"
// const kfkAddr = "127.0.0.1:19092"
// const kfkOther = "127.0.0.1:29092"
// const kfkCons = "127.0.0.1:39092"
//
// const connectToAll = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
//
// func main() {
//
// 	clientMode := os.Args[1]
// 	// props := LoadProperties()
// 	topic := TopicName
//
// 	if strings.Compare(clientMode, producerMode) == 0 {
// 		// producer(props, topic)
// 		producer(topic)
// 	} else if strings.Compare(clientMode, consumerMode) == 0 {
// 		consumer(topic)
// 	} else {
// 		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
// 			producerMode, consumerMode)
// 	}
// }
//
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
// 	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": connectToAll})
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
// 	// schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")
//
// 	// schema, err := schemaRegistryClient.GetLatestSchema(topic)
//
// 	// fmt.Println("The schema: ", schema)
//
// 	// // // !!! if I need to update the shema version => recreate the schema
// 	// // // what ever the changement in the schema, at the time it's recreated
// 	// // // the old schema is replaced by the new one and version increase +1
// 	// // // !!! BUT !!! already registered fields can not be modify(can add fields only)
// 	// schema = nil
// 	// if schema == nil {
// 	// 	// var b bool = false
// 	// 	schemaBytes, _ := ioutil.ReadFile(schemaFile)
// 	// 	// Test mytest = 6;
// 	// 	schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), "PROTOBUF")
// 	// 	if err != nil {
// 	// 		panic(fmt.Sprintf("Error creating the schema %s", err))
// 	// 	}
// 	// 	fmt.Println("look like schema has been created...")
// 	// }
//
// 	for {
//
// 		// tt := &pb.Test{Text: "this a is a good test"}
//
// 		// msg := pb.Person{
// 		// 	Name:       "robert",
// 		// 	Age:        23,
// 		// 	Address:    "the address",
// 		// 	CodePostal: 10111,
// 		// 	Firstname:  "simon",
// 		// 	Mytest:     tt,
// 		// }
//
// 		choosen := rand.Intn(len(devices))
// 		if choosen == 0 {
// 			choosen = 1
// 		}
// 		deviceSelected := devices[choosen-1]
//
// 		key := deviceSelected.DeviceID
//
// 		// recordValue := []byte{}
// 		recordValue := []byte("that is a test de fou")
//
// 		// recordValue = append(recordValue, byte(0))
// 		// schemaIDBytes := make([]byte, 4)
// 		// binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
// 		// recordValue = append(recordValue, schemaIDBytes...)
// 		// messageIndexBytes := []byte{byte(2), byte(0)}
// 		// recordValue = append(recordValue, messageIndexBytes...)
//
// 		// valueBytes, _ := proto.Marshal(&msg)
// 		// recordValue = append(recordValue, valueBytes...)
//
// 		producer.Produce(&kafka.Message{
// 			TopicPartition: kafka.TopicPartition{
// 				Topic: &topic, Partition: kafka.PartitionAny},
// 			Key: []byte(key), Value: recordValue}, nil)
//
// 		time.Sleep(1000 * time.Millisecond)
// 	}
// }
//
// /**************************************************/
// /******************** Consumer ********************/
// /**************************************************/
//
// // func consumer(props map[string]string, topic string) {
// func consumer(topic string) {
//
// 	setTopic()
//
// 	// schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")
//
// 	// schemaRegistryClient.CodecCreationEnabled(false)
//
// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers": connectToAll,
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
// 	c.SubscribeTopics([]string{topic}, nil)
//
// 	run := true
//
// 	for run {
//
// 		// record, err := c.ReadMessage(-1)
// 		msg, err := c.ReadMessage(time.Second)
// 		if err == nil {
// 			// sensorReading := &pb.SensorReading{}
// 			// msg := &pb.Person{}
// 			// err = proto.Unmarshal(record.Value[7:], sensorReading)
// 			// err = proto.Unmarshal(record.Value[7:], msg)
// 			if err != nil {
// 				panic(fmt.Sprintf("Error deserializing the record: %s", err))
// 			}
// 			// fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))
// 			fmt.Println("seeeee: ", msg.TopicPartition, " -- ", string(msg.Value))
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
// 	// admin create a topic
// 	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": connectToAll})
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
// 	}
//
// 	// Print results
// 	for _, result := range results {
// 		fmt.Printf("%s\n", result)
// 	}
//
// 	a.Close()
// }
