package main

// https://github.com/confluentinc/confluent-kafka-go/tree/master/examples/producer_example

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"math"
	"time"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092", // should have a second server in case one goes down
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// message delivery report, indicate success or
				// permanent failure after retries have exhausted
				// Application level retries won't help since  the client
				// is already configure to do that
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic,
						m.TopicPartition.Partition,
						m.TopicPartition.Offset)

				}
			case kafka.Error:
				// generic client instance-level error, such as
				// brocker connection failure authentication issue, etc
				//
				// these errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered,
				// the application does not need to take action on them
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}

		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	totalMsgcnt := 3
	msgcnt := 0
	var backOff = 1 * time.Second
	for msgcnt < totalMsgcnt {
		value := fmt.Sprintf("Producer example, message #%d", msgcnt)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny},
			// Partition: 3},
			Value: []byte(value),
			Headers: []kafka.Header{
				{Key: "thisIsTheKey", Value: []byte("header value is binary")},
			},
		}, nil)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Producer queue is full, wait 1s for messages
				// to be delivered then try again.

				// we want to increase the delay each time we backOff
				// raise its duration to power of two
				backOff = time.Duration(math.Pow(float64(msgcnt), 2)) * time.Second
				time.Sleep(backOff)
				continue
			}
			fmt.Printf("Failed to produce message: %v\n", err)
		}
		msgcnt++
	}

	// Flush and close the producer and the events channel
	for p.Flush(10000) > 0 {
		fmt.Print("Still waiting to flush outstanding messages\n", err)
	}
}
