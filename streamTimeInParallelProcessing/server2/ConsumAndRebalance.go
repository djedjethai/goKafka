package main

import (
	// "context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	// "sync"
	"syscall"
	"time"

	pb "getting-started-with-ccloud-golang/api/v1/proto"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
	"github.com/riferrei/srclient"
)

const (
	producerMode    string = "producer"
	consumerMode    string = "consumer"
	schemaFile      string = "./api/v1/proto/Person.proto"
	commitBatchSize int    = 5
	// schemaFile   string = "./api/v1/proto/SensorReading.proto"
)

type Data struct {
	Key  string
	Name string
	Ts   time.Time
}

var list = []Data{
	Data{Key: "2", Name: "2PaulOOO"},
	Data{Key: "2", Name: "2CelineOOO"},
	Data{Key: "2", Name: "2AliceOOO"},
	Data{Key: "2", Name: "2BerniOOO"},
	Data{Key: "1", Name: "1SimonOOO"},
	Data{Key: "2", Name: "2PauluxOOO"},
	Data{Key: "2", Name: "2LarryOOO"},
	Data{Key: "1", Name: "1AnnieOOO"},
	Data{Key: "2", Name: "2ZoeOOO"},
	Data{Key: "2", Name: "2CharlesOOO"},
	Data{Key: "1", Name: "1ArturOOO"},
}

// const topicMessage = "message"

func main() {

	clientMode := os.Args[1]
	// props := LoadProperties()
	topic := "input-stream"
	toSrv3 := make(chan Data)
	toSrv4 := make(chan Data)

	if strings.Compare(clientMode, producerMode) == 0 {
		// producer(props, topic)
		producer(topic)
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		go producerRedirect("repartition-stream", toSrv4, int32(1))
		go producerRedirect("repartition-stream", toSrv3, int32(0))
		consumer(topic, toSrv4, toSrv3)
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

/**************************************************/
/******************** Producers ********************/
/**************************************************/

// func producer(props map[string]string, topic string) {
func producer(topic string) {

	// CreateTopic(props)

	// producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka:9092"})
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
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

	for i := 0; i < len(list); i++ {

		msg := pb.Message{
			Key:  list[i].Key,
			Text: list[i].Name,
		}

		key := list[i].Key

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
				Topic: &topic, Partition: 1},
			Key: []byte(key), Value: recordValue}, nil)

		time.Sleep(1000 * time.Millisecond)
		fmt.Println("sent....")
	}
}

// redirect the invalide key(so datas) to the right consumer
func producerRedirect(topic string, dataToTransfert chan Data, partition int32) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
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

	for dt := range dataToTransfert {

		msg := pb.Message{
			Key:  dt.Key,
			Text: dt.Name,
		}

		key := dt.Key

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
				Topic: &topic, Partition: partition},
			Key:       []byte(key),
			Value:     recordValue,
			Timestamp: dt.Ts,
		}, nil)

		time.Sleep(1000 * time.Millisecond)
		fmt.Println("sent....")
	}
}

/**************************************************/
/******************** Consumer ********************/
/**************************************************/

// func consumer(props map[string]string, topic string) {
func consumer(topic string, toSrv4, toSrv3 chan Data) {

	// setTopic()

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")

	schemaRegistryClient.CodecCreationEnabled(false)

	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	config := &kafka.ConfigMap{
		"bootstrap.servers":  "127.0.0.1:29092",
		"group.id":           "groupByKey",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		// "partition.assignment.strategy": "sticky",
	}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{topic}, myRebalanceCallback)
	// c.SubscribeTopics([]string{topic}, nil)

	run := true

	// Handle unexpected shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle the signals
	go func(*kafka.Consumer) {
		// Block until a signal is received
		sig := <-signals
		fmt.Println("Received signal:", sig)

		// Perform any necessary cleanup or shutdown operations
		commitOffsets(c)

		// Exit the program gracefully
		os.Exit(0)
	}(c)

	var messageCount int

	for run {
		select {
		case <-signals:
			// Termination signal received. exit the loop.
			break

		default:

			// fmt.Printf("Received message !!!!!!!!! : %s\n", string(e.Value)
			ev := c.Poll(1000)

			switch e := ev.(type) {
			case *kafka.Message:
				// sensorReading := &pb.SensorReading{}
				msg := &pb.Message{}
				// err = proto.Unmarshal(record.Value[7:], sensorReading)
				err = proto.Unmarshal(e.Value[7:], msg)
				if err != nil {
					panic(fmt.Sprintf("Error deserializing the record: %s", err))
				}

				fmt.Println("")
				fmt.Printf("Message on %s: %s - %v\n", e.TopicPartition, string(e.Value), e.Timestamp)

				if string(e.Key) == "2" {
					time.Sleep(time.Second * 1)
					log.Println("Send to serv 4::::: ", msg)
					toSrv4 <- Data{Key: msg.Key, Name: msg.Text, Ts: e.Timestamp}
				} else if string(e.Key) == "1" {
					log.Println("send to serv 3:::: ", msg)
					toSrv3 <- Data{Key: msg.Key, Name: msg.Text, Ts: e.Timestamp}
				} else {
					log.Println("Error message key::: ", msg)
				}

				// here we will commit "async" each 5 message
				messageCount++
				if messageCount%commitBatchSize == 0 {
					go commitOffsets(c)
				}

			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
				// Handle the error here

			default:
				// Ignore other event types
			}
		}
	}
}

func commitOffsets(c *kafka.Consumer) {
	offsets, err := c.Commit()
	if err != nil {
		// Handle commit error
		fmt.Println("Failed to commit offsets:", err)
	} else {
		// Process the committed offsets if needed
		fmt.Println("Committed offsets:", offsets)
	}
}

func myRebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		// Handle assigned partitions
		fmt.Printf("Assigned partitions: %v\n", ev.Partitions)

		commitOffsets(c)
		// Start consuming from the newly assigned partitions

	case kafka.RevokedPartitions:
		// Handle revoked partitions
		fmt.Printf("Revoked partitions: %v\n", ev.Partitions)

		commitOffsets(c)

		// Stop consuming from the revoked partitions and perform any necessary cleanup or checkpointing operations
	}

	// Return nil or an error, depending on your requirements
	return nil
}

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
