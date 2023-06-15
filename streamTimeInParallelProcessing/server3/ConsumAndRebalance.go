package main

import (
	"container/list"
	"fmt"
	"log"
	"os"
	"os/signal"
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

func main() {

	// topic := os.Args[1]
	topic := "repartition-stream"
	// group := os.Args[2]
	group := "aggregate"
	// props := LoadProperties()

	consumer(topic, group)

}

func eventDelayWindow(events, toProcess chan Data) {
	log.Println("Event delay window is running")
	l := list.New()
	timeRef := time.Time{}
	for {
		// set a window of 5s
		timeRef = time.Now().Add(-5 * time.Second)
		select {
		case dt, ok := <-events:
			if !ok {
				// channel close exit
				return
			}
			if dt.Key != "" && dt.Name != "" && dt.Ts != (time.Time{}) {
				// log.Println("Valid event to be store: ", dt)
				inserted := false

				for e := l.Front(); e != nil; e = e.Next() {
					if d, ok := e.Value.(Data); ok {
						// if dt.Ts is before that d.TS
						if dt.Ts.Before(d.Ts) {
							l.InsertBefore(dt, e)
							inserted = true
							break
						}
					}
				}

				if !inserted {
					// log.Println("PushBack event: ", dt)
					l.PushBack(dt)
				}
			}
		default:
			// ckeck if headOfList ok return
			if l.Len() > 0 {
				// log.Println("See the front event element: ", l.Front().Value)
				for e := l.Front(); e != nil; e = e.Next() {
					if d, ok := e.Value.(Data); ok {
						// Check if d.Ts is before timeRef
						if d.Ts.Before(timeRef) {
							toProcess <- d
							_ = l.Remove(l.Front())
						}
					}
				}
			}
		}
	}
}

func consumeEvent(toProcess chan Data, c *kafka.Consumer) {
	// ckeck if headOfList ok return

	log.Println("Consume event is running")
	for dt := range toProcess {
		fmt.Println("Process event: ", dt)
		commitOffsets(c)
	}
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
	var events = make(chan Data)
	var toProcess = make(chan Data)

	// Start a goroutine to handle the signals
	go func(*kafka.Consumer) {
		// Block until a signal is received
		sig := <-signals
		fmt.Println("Received signal:", sig)

		close(events)
		close(toProcess)

		// Perform any necessary cleanup or shutdown operations
		commitOffsets(c)

		// Exit the program gracefully
		os.Exit(0)
	}(c)

	// handle the delay window
	go eventDelayWindow(events, toProcess)

	// consume the event after 3s standby, making sue the late events are re-ordered
	go consumeEvent(toProcess, c)

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
				// fmt.Printf("Message on %s: %s - %v \n", e.TopicPartition, string(e.Value), e.Timestamp)
				dt := Data{
					Key:  msg.Key,
					Name: msg.Text,
					Ts:   e.Timestamp,
				}

				events <- dt

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
