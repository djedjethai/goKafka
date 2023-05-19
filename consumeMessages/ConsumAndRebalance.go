package main

// import (
// 	"context"
// 	"encoding/binary"
// 	"fmt"
// 	// "io/ioutil"
// 	"log"
// 	"math/rand"
// 	"os"
// 	"os/signal"
// 	"strings"
// 	// "sync"
// 	"syscall"
// 	"time"
//
// 	pb "getting-started-with-ccloud-golang/api/v1/proto"
// 	"github.com/golang/protobuf/proto"
// 	"github.com/riferrei/srclient"
// 	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
// 	// "github.com/confluentinc/confluent-kafka-go/v2/kafka"
// )
//
// const (
// 	producerMode    string = "producer"
// 	consumerMode    string = "consumer"
// 	schemaFile      string = "./api/v1/proto/Person.proto"
// 	commitBatchSize int    = 5
// 	// schemaFile   string = "./api/v1/proto/SensorReading.proto"
// 	// messageFile  string = "./api/v1/proto/Message.proto"
// )
//
// // type Event interface {
// // 	// String returns a human-readable representation of the event
// // 	String() string
// // }
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
// 	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
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
// 	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")
//
// 	schema, err := schemaRegistryClient.GetLatestSchema(topic)
//
// 	// fmt.Println("The schema: ", schema)
//
// 	// // !!! if I need to update the shema version => recreate the schema
// 	// // what ever the changement in the schema, at the time it's recreated
// 	// // the old schema is replaced by the new one and version increase +1
// 	// // !!! BUT !!! already registered fields can not be modify(can add fields only)
// 	schema = nil
// 	if schema == nil {
// 		// var b bool = false
// 		// schemaBytes, _ := ioutil.ReadFile(schemaFile)
//
// 		schemaBytes := `
// 			syntax = "proto3";
//
// 			package io.confluent.cloud.demo.domain1;
//
// 			option go_package = "getting-started-with-ccloud-golang/api/v1/proto";
//
// 			message Person {
// 				string name = 1;
// 				float age = 2;
// 				string address = 3;
// 				int32 code_postal = 4;
// 				string firstname = 5;
// 				Test mytest = 6;
// 			};
//
// 			message Test{
// 				string text = 1;
// 			}`
//
// 		// Test mytest = 6;
// 		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), "PROTOBUF")
// 		if err != nil {
// 			panic(fmt.Sprintf("Error creating the schema %s", err))
// 		}
// 		fmt.Println("look like schema has been created...")
// 	}
//
// 	for {
//
// 		tt := &pb.Test{Text: "this a is a good test"}
//
// 		msg := pb.Person{
// 			Name:       "robert",
// 			Age:        23,
// 			Address:    "the address",
// 			CodePostal: 10111,
// 			Firstname:  "simon",
// 			Mytest:     tt,
// 		}
//
// 		choosen := rand.Intn(len(devices))
// 		if choosen == 0 {
// 			choosen = 1
// 		}
// 		deviceSelected := devices[choosen-1]
//
// 		key := deviceSelected.DeviceID
//
// 		recordValue := []byte{}
// 		// recordValue := []byte("that is a test de fou")
//
// 		recordValue = append(recordValue, byte(0))
// 		schemaIDBytes := make([]byte, 4)
// 		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
// 		recordValue = append(recordValue, schemaIDBytes...)
// 		messageIndexBytes := []byte{byte(2), byte(0)}
// 		recordValue = append(recordValue, messageIndexBytes...)
//
// 		valueBytes, _ := proto.Marshal(&msg)
// 		recordValue = append(recordValue, valueBytes...)
//
// 		// fmt.Println("see topic: ", topic)
// 		// fmt.Println("see key: ", key)
// 		// fmt.Println("see value: ", string(recordValue))
//
// 		producer.Produce(&kafka.Message{
// 			TopicPartition: kafka.TopicPartition{
// 				Topic: &topic, Partition: kafka.PartitionAny},
// 			Key: []byte(key), Value: recordValue}, nil)
//
// 		time.Sleep(1000 * time.Millisecond)
// 		fmt.Println("sent....")
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
// 	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")
//
// 	schemaRegistryClient.CodecCreationEnabled(false)
//
// 	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 	config := &kafka.ConfigMap{
// 		"bootstrap.servers":  "127.0.0.1:29092",
// 		"group.id":           "myGroup",
// 		"auto.offset.reset":  "earliest",
// 		"enable.auto.commit": false,
// 		// "partition.assignment.strategy": "sticky",
// 	}
//
// 	c, err := kafka.NewConsumer(config)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer c.Close()
//
// 	c.SubscribeTopics([]string{topic}, myRebalanceCallback)
// 	// c.SubscribeTopics([]string{topic}, nil)
//
// 	run := true
//
// 	// Handle unexpected shutdown
// 	signals := make(chan os.Signal, 1)
// 	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
//
// 	// Start a goroutine to handle the signals
// 	go func(*kafka.Consumer) {
// 		// Block until a signal is received
// 		sig := <-signals
// 		fmt.Println("Received signal:", sig)
//
// 		// Perform any necessary cleanup or shutdown operations
// 		commitOffsets(c)
//
// 		// Exit the program gracefully
// 		os.Exit(0)
// 	}(c)
//
// 	var messageCount int
//
// 	for run {
// 		// fmt.Printf("Received message !!!!!!!!! : %s\n", string(e.Value))
// 		record, err := c.ReadMessage(-1)
// 		if err == nil {
//
// 			// sensorReading := &pb.SensorReading{}
// 			msg := &pb.Person{}
// 			// err = proto.Unmarshal(record.Value[7:], sensorReading)
// 			err = proto.Unmarshal(record.Value[7:], msg)
// 			if err != nil {
// 				panic(fmt.Sprintf("Error deserializing the record: %s", err))
// 			}
//
// 			fmt.Println("")
// 			fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))
//
// 			// here we will commit "async" each 5 message
// 			messageCount++
// 			if messageCount%commitBatchSize == 0 {
// 				go commitOffsets(c)
// 			}
//
// 			// go func() {
// 			// 	_, err := c.CommitMessage(record)
// 			// 	if err != nil {
// 			// 		fmt.Printf("Failed to commit offset: %s\n", err)
// 			// 		// Handle the error accordingly
// 			// 	}
// 			// }()
// 		} else {
// 			log.Println("The error from c.ReadMessage is not nil: ", err)
// 		}
// 	}
// }
//
// func commitOffsets(c *kafka.Consumer) {
// 	offsets, err := c.Commit()
// 	if err != nil {
// 		// Handle commit error
// 		fmt.Println("Failed to commit offsets:", err)
// 	} else {
// 		// Process the committed offsets if needed
// 		fmt.Println("Committed offsets:", offsets)
// 	}
// }
//
// func myRebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
// 	switch ev := event.(type) {
// 	case kafka.AssignedPartitions:
// 		// Handle assigned partitions
// 		fmt.Printf("Assigned partitions: %v\n", ev.Partitions)
//
// 		commitOffsets(c)
// 		// Start consuming from the newly assigned partitions
//
// 	case kafka.RevokedPartitions:
// 		// Handle revoked partitions
// 		fmt.Printf("Revoked partitions: %v\n", ev.Partitions)
//
// 		commitOffsets(c)
//
// 		// Stop consuming from the revoked partitions and perform any necessary cleanup or checkpointing operations
// 	}
//
// 	// Return nil or an error, depending on your requirements
// 	return nil
// }
//
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
//
// // type StickyAssignor struct {
// // 	consumer           *kafka.Consumer
// // 	assignedPartitions map[int32]bool
// // }
// //
// // func NewStickyAssignor(consumer *kafka.Consumer) *StickyAssignor {
// // 	return &StickyAssignor{
// // 		consumer:           consumer,
// // 		assignedPartitions: make(map[int32]bool),
// // 	}
// // }
// //
// // func (s *StickyAssignor) AssignPartitions(topic string, partitions []kafka.TopicPartition) {
// // 	currentAssignment, err := s.consumer.Assignment()
// // 	log.Println("See the current assignement: ", currentAssignment)
// // 	log.Println("See the stored assignement: ", s.assignedPartitions)
// // 	if err != nil {
// // 		panic(err)
// // 	}
// //
// // 	// get existing partition
// // 	meta, err := s.consumer.GetMetadata(nil, true, 5000)
// // 	if err != nil {
// // 		fmt.Fprintf(os.Stderr, "Failed to retrieve metadata: %v\n", err)
// // 	}
// //
// // 	topicMeta, ok := meta.Topics[topic]
// // 	if !ok {
// // 		fmt.Fprintf(os.Stderr, "Topic not found in metadata: my-topic\n")
// // 	}
// //
// // 	// set the already assigned partitions
// // 	for _, partition := range topicMeta.Partitions {
// // 		s.assignedPartitions[partition.ID] = true
// // 	}
// //
// // 	// handle the partition assignment
// //
// // 	// loop on the assigned partitions then if some available assign to the next
// //
// // 	// Find the partitions that are already assigned to the consumer
// // 	// assignedPartitions := make(map[int32]bool)
// // 	// for _, p := range currentAssignment {
// // 	// 	if *p.Topic == topic {
// // 	// 		s.assignedPartitions[p.Partition] = true
// // 	// 	}
// // 	// }
// //
// // 	// // Assign the unassigned partitions
// // 	// var newAssignment []kafka.TopicPartition
// // 	// for _, p := range partitions {
// // 	// 	if !s.assignedPartitions[p.Partition] {
// // 	// 		newAssignment = append(newAssignment, p)
// // 	// 		// s.assignedPartitions[p.Partition] = true
// // 	// 	}
// // 	// }
// //
// // 	// // Manually assign the unassigned partitions
// // 	// err = s.consumer.Assign(newAssignment)
// // 	// if err != nil {
// // 	// 	panic(err)
// // 	// }
// // 	// // update the s.assignedPartitions
// // 	// for _, p := range newAssignment {
// // 	// 	s.assignedPartitions[p.Partition] = true
// // 	// }
// //
// // 	// fmt.Println("Assigned partitions:", newAssignment)
// // }
// //
// // func (s *StickyAssignor) OnRebalance(partitions []kafka.TopicPartition) error {
// // 	fmt.Printf("Rebalance event: %v\n", partitions)
// //
// // 	// Iterate through partitions and reassign the desired ones
// // 	for _, partition := range partitions {
// // 		if s.assignedPartitions[partition.Partition] {
// // 			fmt.Printf("Reassigning partition %d\n", partition.Partition)
// // 			err := s.consumer.Assign([]kafka.TopicPartition{
// // 				kafka.TopicPartition{
// // 					// Topic:     &partition.Topic,
// // 					Topic:     partition.Topic,
// // 					Partition: partition.Partition,
// // 					Offset:    kafka.OffsetInvalid,
// // 				},
// // 			})
// //
// // 			if err != nil {
// // 				fmt.Printf("Error reassigning partition %d: %v\n", partition.Partition, err)
// // 				return err
// // 			}
// // 		}
// // 	}
// //
// // 	return nil
// // }
//
// // for run {
// // 	fmt.Println("grrr")
//
// // 	// commit each 10 messages
// // 	record, err := c.ReadMessage(-1)
// // 	if err == nil {
//
// // 		// sensorReading := &pb.SensorReading{}
// // 		msg := &pb.Person{}
// // 		// err = proto.Unmarshal(record.Value[7:], sensorReading)
// // 		err = proto.Unmarshal(record.Value[7:], msg)
// // 		if err != nil {
// // 			panic(fmt.Sprintf("Error deserializing the record: %s", err))
// // 		}
// // 		fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))
// // 		fmt.Println("seeeee: ", msg)
//
// // 		if count%10 == 0 {
// // 			log.Println("commit .......")
// // 			// Commit the offset asynchronously
// // 			go func() {
// // 				_, err := c.CommitMessage(record)
// // 				if err != nil {
// // 					fmt.Printf("Failed to commit offset: %s\n", err)
// // 					// Handle the error accordingly
// // 				}
// // 			}()
// // 		}
// // 		count++
// // 		if count%1000000000 == 0 {
// // 			count = 0
// // 		}
//
// // 		// commit asynchronously at each reveived message
// // 		// // sensorReading := &pb.SensorReading{}
// // 		// msg := &pb.Person{}
// // 		// // err = proto.Unmarshal(record.Value[7:], sensorReading)
// // 		// err = proto.Unmarshal(record.Value[7:], msg)
// // 		// if err != nil {
// // 		// 	panic(fmt.Sprintf("Error deserializing the record: %s", err))
// // 		// }
// // 		// fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))
// // 		// fmt.Println("seeeee: ", msg)
//
// // 		// // Commit the offset asynchronously
// // 		// go func() {
// // 		// 	_, err := c.CommitMessage(record)
// // 		// 	if err != nil {
// // 		// 		fmt.Printf("Failed to commit offset: %s\n", err)
// // 		// 		// Handle the error accordingly
// // 		// 	}
// // 		// }()
//
// // 	} else if err.(kafka.Error).IsFatal() {
// // 		// fmt.Println(err)
// // 		log.Printf("Consumer error: %v \n", err)
// // 		break
// // 	} else {
// // 		// In case of any other errors(which are not Fatal),
// // 		// we try to commit the last offset, and we stay in the loop
// // 		// at the oposit of the Fatal err(previously) where we
// // 		// exit the loop and terminate the program
// // 		log.Println("See the err in the else: ", err)
//
// // 		wg.Add(1)
// // 		go handleLastcommit(c, record, &wg)
// // 		wg.Wait()
// // 	}
// // }
//
// // wg.Add(1)
// // go handleLastcommit(c, record, &wg)
// // wg.Wait()
//
// // log.Println("consumer shut down")
//
// // type StickyAssignor struct {
// // 	consumer *kafka.Consumer
// // }
// //
// // func (a *StickyAssignor) Assign(
// // 	partitions []kafka.TopicPartition,
// // 	members []string,
// // ) (map[string][]kafka.TopicPartition, error) {
// // 	assignments := make(map[string][]kafka.TopicPartition)
// // 	memberCount := len(members)
// //
// // 	// Assign partitions in a sticky manner
// // 	for i, partition := range partitions {
// // 		memberIndex := i % memberCount
// // 		memberID := members[memberIndex]
// // 		assignments[memberID] = append(assignments[memberID], partition)
// // 	}
// //
// // 	return assignments, nil
// // }
// //
// // func (a *StickyAssignor) OnRevocation(revokedPartitions []kafka.TopicPartition) {
// // 	// Handle logic when partitions are revoked from consumers
// // 	for _, revokedPartition := range revokedPartitions {
// // 		// Commit the current offset for revoked partitions
// // 		a.consumer.CommitOffsets([]kafka.TopicPartition{revokedPartition})
// //
// // 		// Seek to the new position for revoked partitions (end offset)
// // 		tp := kafka.TopicPartition{
// // 			Topic:     revokedPartition.Topic,
// // 			Partition: revokedPartition.Partition,
// // 			Offset:    kafka.OffsetEnd,
// // 		}
// //
// // 		_, high, err := a.consumer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, 5000)
// // 		if err != nil {
// // 			// Handle error
// // 			continue
// // 		}
// //
// // 		tp.Offset = kafka.Offset(high)
// //
// // 		a.consumer.Seek(tp, 0)
// //
// // 		// Perform any other necessary actions for revoked partitions
// // 	}
// // }
