package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	// "sync"
	"syscall"
	"time"

	pb "getting-started-with-ccloud-golang/api/v1/proto"
	"github.com/golang/protobuf/proto"
	"github.com/riferrei/srclient"
	// "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	producerMode    string = "producer"
	consumerMode    string = "consumer"
	schemaFile      string = "./api/v1/proto/Person.proto"
	commitBatchSize int    = 10
	// schemaFile   string = "./api/v1/proto/SensorReading.proto"
	// messageFile  string = "./api/v1/proto/Message.proto"
)

// type Event interface {
// 	// String returns a human-readable representation of the event
// 	String() string
// }

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

type Data struct {
	Name string
	// Offset int64
}

var list = []Data{
	Data{Name: "Paul"},
	Data{Name: "Celine"},
	Data{Name: "Alice"},
	Data{Name: "Berni"},
	Data{Name: "Simon"},
	Data{Name: "Paulux"},
	Data{Name: "Larry"},
	Data{Name: "Annie"},
	Data{Name: "Zoe"},
	Data{Name: "Charles"},
	Data{Name: "Artur"},
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

	// fmt.Println("The schema: ", schema)

	// // !!! if I need to update the shema version => recreate the schema
	// // what ever the changement in the schema, at the time it's recreated
	// // the old schema is replaced by the new one and version increase +1
	// // !!! BUT !!! already registered fields can not be modify(can add fields only)
	schema = nil
	if schema == nil {
		// var b bool = false
		schemaBytes, _ := ioutil.ReadFile(schemaFile)

		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), "PROTOBUF")
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
		fmt.Println("look like schema has been created...")
	}

	for i := 0; i < len(list); i++ {

		msg := pb.Data{
			Name: list[i].Name,
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
		fmt.Println("sent....")
	}

}

/**************************************************/
/******************** Consumer ********************/
/**************************************************/

// type Record struct {
// 	Topic     string
// 	Partition int32
// 	Offset    string
// }

type Record struct {
	TP kafka.TopicPartition
}

var database []Data
var kafkaTrace []kafka.TopicPartition

var count int = 0

var n string = TopicName

var partitionCnt int = 0

var filePath string

// func consumer(props map[string]string, topic string) {
func consumer(topic string) {
	setTopic()

	fileP, err := findPath("data.txt")
	if err != nil {
		log.Println("error finding the path: ", err)
	}

	filePath = fileP

	writeCH := make(chan string)
	// readCH := make(chan string)

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")

	schemaRegistryClient.CodecCreationEnabled(false)

	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	config := &kafka.ConfigMap{
		"bootstrap.servers":               "127.0.0.1:29092",
		"group.id":                        "myGroup",
		"auto.offset.reset":               "earliest",
		"enable.auto.commit":              false,
		"go.application.rebalance.enable": true,
		// "partition.assignment.strategy": "sticky",
	}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// pass the callback which will trigger the logic depend of the event
	c.SubscribeTopics([]string{topic}, myRebalanceCallback)

	run := true

	// Handle unexpected shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle the signals
	go func(*kafka.Consumer) {
		// Block until a signal is received
		sig := <-signals
		fmt.Println("Received signal:", sig)

		// Handle the cleanup tasks before shut down
		// like close connection to the db
		close(writeCH)

		// Exit the program gracefully
		os.Exit(0)
	}(c)

	// NOTE here, before Reading any message we want to position on each partition
	// of the offset which has been saved to db.
	// (as we don't want duplicated data neither lose some)
	// for the sake of this ex, we use a file, which each TopicPartition string
	// saved in it mean that the matching event's data are saved in db.
	// !!!! In the real world, the saving of the event's data and the TopicPartition
	// string should be in the db an should occur as a transaction.

	fh, err := fileHandler(filePath)
	if err != nil {
		log.Println("Err from file handler: ", err)
	}
	defer fh.Close()

	// fileReader(filePath)

	// will seek() each partition to the correct offset which should be next to be proceed
	// seekPartitions(c)

	go fileWriter(fh, writeCH)

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
				msg := &pb.Data{}
				// err = proto.Unmarshal(record.Value[7:], sensorReading)
				err = proto.Unmarshal(e.Value[7:], msg)
				if err != nil {
					panic(fmt.Sprintf("Error deserializing the record: %s", err))
				}

				fmt.Println("")
				fmt.Printf("Message on %s: %s\n", e.TopicPartition, string(e.Value))
				// fmt.Println("See the message: ", msg)

				writeCH <- e.String()

				if count%commitBatchSize == 0 {
					// NOTE, to avoid writing to db at each event
					// we could create a write(batch) wich write each x records
					// it would save a lot of queries
					_ = commitOffsets(c)
				}

				count++

			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
				// Handle the error here

			default:
				// Ignore other event types
			}

		}
	}
}

func findPath(filename string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Println("Failed to get current working directory:", err)
		return "", err
	}

	// Construct the file path by joining the working directory and filename
	filePath := wd + string(os.PathSeparator) + filename

	fmt.Println("File Path:", filePath)
	return filePath, nil
}

func fileHandler(filePath string) (*os.File, error) {
	// file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return nil, err
	}
	return file, nil
}

func fileWriter(file *os.File, ch chan string) {
	for msg := range ch {
		// fmt.Println("grrr the msg: ", msg)
		_, err := fmt.Fprintln(file, msg)
		if err != nil {
			fmt.Println("Failed to write to file:", err)
			close(ch)
			return
		}
	}
}

// func fileReader(filePath string, ch chan string) {
func fileReader(filePath string) {
	// kafkaTrace = []kafka.TopicPartition{}

	log.Println("In the reader see the path: ", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer file.Close()

	// Create a scanner to read line by line
	scanner := bufio.NewScanner(file)

	// Read line by line
	for scanner.Scan() {
		line := scanner.Text()

		// convert the string to type kafka.TopicPartition
		topicPartition := stringToTopicPartition(line)

		// set a slice with all previously recorded TopicPartition
		// means with all TopicPartition where matched event's data
		// are already saved into db
		kafkaTrace = append(kafkaTrace, topicPartition)

		// ch <- scanner.Text()
		fmt.Println(line)
	}

	// Check if any errors occurred during scanning
	// or end of file
	if err := scanner.Err(); err != nil {
		fmt.Println("Failed to read file:", err)
		// close(ch)
		return
	}
	// close(ch)
}

func stringToTopicPartition(str string) kafka.TopicPartition {
	split := strings.Split(str, "@")
	topicParts := strings.Split(split[0], "[")
	topic := topicParts[0]
	partition, _ := strconv.Atoi(strings.TrimRight(topicParts[1], "]"))
	offset, _ := strconv.ParseInt(split[1], 10, 64)

	// Create a new TopicPartition
	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: int32(partition),
	}

	// Set the offset for the TopicPartition
	topicPartition.Offset = kafka.Offset(offset)
	return topicPartition
}

// retry logic
type kafkaCommitment func(
	context.Context,
	*kafka.Consumer,
	[]kafka.TopicPartition,
	int,
) ([]kafka.TopicPartition, error)

func assignmentWrapper(ctx context.Context, c *kafka.Consumer, listOffsets []kafka.TopicPartition, useless int) ([]kafka.TopicPartition, error) {
	ass, err := c.Assignment()
	if len(ass) < 1 {
		return ass, errors.New("assignement still empty")
	} else {
		return ass, err
	}
}

func commitOffsetsWrapper(ctx context.Context, c *kafka.Consumer, listOffsets []kafka.TopicPartition, useless int) ([]kafka.TopicPartition, error) {
	return c.CommitOffsets(listOffsets)
}

func committedWrapper(ctx context.Context, c *kafka.Consumer, listPartitions []kafka.TopicPartition, delayToWait int) ([]kafka.TopicPartition, error) {

	return c.Committed(listPartitions, delayToWait)
}

func retry(kfkFunc kafkaCommitment, retry int, delay time.Duration) kafkaCommitment {
	return func(ctx context.Context, c *kafka.Consumer, ktp []kafka.TopicPartition, d int) ([]kafka.TopicPartition, error) {
		for r := 0; ; r++ {
			respKpt, err := kfkFunc(ctx, c, ktp, d)
			if err == nil || r >= retry {
				return respKpt, err
			}

			log.Printf("Attempt %d failed; retrying in %v", r+1, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return []kafka.TopicPartition{}, errors.New("Cannot get response")
			}
		}
	}
}

// Run the logic to commit un-commited offsets
func helperCommitOffsets(ctx context.Context, c *kafka.Consumer, refX kafka.TopicPartition, retryCommitOffsets, retryCommitted kafkaCommitment) {

	// commit the partition first to make sure kafka won't rebalance dor any reason,
	// which may create a re-play of the even, even we seek()
	// tps, err := c.Committed([]kafka.TopicPartition{ref0}, 5000)
	tps, err := retryCommitted(ctx, c, []kafka.TopicPartition{refX}, 100)
	if err != nil {
		fmt.Println("Err commit uncommited offsets at part0: ", err)
	}

	log.Println("helper see tps: ", tps, " - ", refX)

	// commit all TopicPartition between the last commited and last saved in db
	var diff int64 = 0
	ofs := int64(tps[0].Offset)
	log.Println("see the position: ", ofs)
	if ofs < 0 {
		// we don't want to re-commit 4000 events
		if int64(refX.Offset) < 20 {
			tps[0].Offset = kafka.Offset(0)
		} else {
			tps[0].Offset = kafka.Offset(int64(refX.Offset) - int64(20))
		}
	}
	if len(tps) > 0 && int64(tps[0].Offset) < int64(refX.Offset) {
		diff = int64(refX.Offset) - int64(tps[0].Offset)
	}

	fmt.Println("see the diff length: ", diff)

	listOffsets := []kafka.TopicPartition{}
	if diff > 0 {
		for i := 0; i < int(diff); i++ {
			tps[0].Offset = kafka.Offset(int64(tps[0].Offset) + int64(1))
			listOffsets = append(listOffsets, tps[0])
		}
		fmt.Println("See the list of offset to commit: ", listOffsets)
	}
	// add one more offset to the actual position
	// waranty that the last event is not re-played
	refX.Offset = kafka.Offset(int64(refX.Offset) + int64(1))

	listOffsets = append(listOffsets, refX)
	// commit all the offsets to kafka
	// commited, err := c.CommitOffsets(listOffsets)
	commited, err := retryCommitOffsets(ctx, c, listOffsets, 0)
	if err == nil {
		fmt.Println("See commited offsets' list: ", commited)

	} else {
		log.Println("Error commited listOffsets: ", err)
	}

}

// When start or re-balance this handle already proceed events but still un-commited
func seekPartitions(c *kafka.Consumer, topicPartitions []kafka.TopicPartition) {

	maxTopicPartitions := len(topicPartitions)
	savedTopicPartitions := []kafka.TopicPartition{}
	savedTPReference := make(map[int32]bool)

	// re-assign the same partitions as it was
	if len(kafkaTrace) > 0 {
		for i := len(kafkaTrace) - 1; i >= 0; i-- {
			fmt.Println("see the range: ", kafkaTrace[i].Partition, " - ", kafkaTrace[i].Offset)

			// see if the assigned partitions match some already saved in the db
			// if yes see the diff between saved in db and commited then commit diff

			if len(topicPartitions) > 0 && maxTopicPartitions > 0 {
				for _, tp := range topicPartitions {
					// see if partition is saved
					if _, ok := savedTPReference[tp.Partition]; !ok {
						if kafkaTrace[i].Partition == tp.Partition {
							savedTopicPartitions = append(savedTopicPartitions, kafkaTrace[i])
							savedTPReference[tp.Partition] = true
							maxTopicPartitions--
						}
					}
				}
			}

		}
	}

	// log.Println("see the saved tp from math: ", savedTopicPartitions)
	// log.Println("see the tp from kafka: ", topicPartitions)

	retryCommitted := retry(committedWrapper, 6, time.Second*10)
	retryCommitOffsets := retry(commitOffsetsWrapper, 6, time.Second*10)
	ctx := context.Background()

	for _, tp := range savedTopicPartitions {
		helperCommitOffsets(ctx, c, tp, retryCommitOffsets, retryCommitted)
	}

}

func saveRecordToDB(data string) {
	database = append(database, Data{data})
}

func commitOffsets(c *kafka.Consumer) []kafka.TopicPartition {

	offsets, err := c.Commit()
	if err != nil {
		// Handle commit error
		fmt.Println("Failed to commit offsets:", err)
		return nil

	} else {
		// Process the committed offsets if needed
		fmt.Println("Committed offsets:", offsets)

		return offsets
	}
}

// This will be call when the service start or rebalance
func myRebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:

		// read the file(would be a db) with the already proceeded events
		fileReader(filePath)

		// Handle assigned partitions
		fmt.Printf("Assigned partitions: %v\n", ev.Partitions)

		seekPartitions(c, ev.Partitions)

	case kafka.RevokedPartitions:

		fmt.Printf("Revoked partitions: %v\n", ev.Partitions)
	}

	// Return nil or an error, depending on your requirements
	return nil
}

func setTopic() {
	fmt.Println("in the set topic")

	// admin create a topic
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
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
	} else {
		fmt.Println("topic created: ", TopicName)
	}

	fmt.Println("after topics created: ")

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	fmt.Println("after topics created end: ")

	a.Close()
}
