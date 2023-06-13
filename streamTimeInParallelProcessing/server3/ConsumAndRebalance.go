package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
}

func main() {

	// topic := os.Args[1]
	topic := "repartition-stream"
	// group := os.Args[2]
	group := "aggregate"
	// props := LoadProperties()

	consumer(topic, group)

}

/**************************************************/
/******************** Consumer ********************/
/**************************************************/

// func consumer(props map[string]string, topic string) {
func consumer(topic, group string) {

	// setTopic()

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")

	schemaRegistryClient.CodecCreationEnabled(false)

	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	config := &kafka.ConfigMap{
		"bootstrap.servers":  "127.0.0.1:29092",
		"group.id":           group,
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
				fmt.Printf("Message on %s: %s - %v \n", e.TopicPartition, string(e.Value), e.Timestamp)

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

		// // fmt.Printf("Received message !!!!!!!!! : %s\n", string(e.Value))
		// record, err := c.ReadMessage(-1)
		// if err == nil {

		// 	// sensorReading := &pb.SensorReading{}
		// 	msg := &pb.Message{}
		// 	// err = proto.Unmarshal(record.Value[7:], sensorReading)
		// 	err = proto.Unmarshal(record.Value[7:], msg)
		// 	if err != nil {
		// 		panic(fmt.Sprintf("Error deserializing the record: %s", err))
		// 	}

		// 	fmt.Println("")
		// 	fmt.Printf("Message on %s: %s - %v \n", record.TopicPartition, string(record.Value), record.Time Change with poll)

		// 	// here we will commit "async" each 5 message
		// 	messageCount++
		// 	if messageCount%commitBatchSize == 0 {
		// 		go commitOffsets(c)
		// 	}

		// } else {
		// 	log.Println("The error from c.ReadMessage is not nil: ", err)
		// }
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
