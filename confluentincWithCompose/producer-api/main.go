package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka:9092"})
	if err != nil {
		log.Panic("err connecting to kafka: ", err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	// for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
	// 	p.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte(word),
	// 	}, nil)
	// }

	// Add handle func for producer.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatalln(err)
		}

		log.Println("see the body: ", body)

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(body),
		}, nil)

		// msg := kafka.Message{
		// 	Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
		// 	Value: body,
		// }
		// err = kafkaWriter.WriteMessages(req.Context(), msg)

		// if err != nil {
		// 	wrt.Write([]byte(err.Error()))
		// 	log.Fatalln(err)
		// }

	})

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

	fmt.Println("start producer-api on port 8080... !!")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
