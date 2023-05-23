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
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	// "github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

var ref0 = kafka.TopicPartition{Topic: &n, Partition: int32(0), Offset: kafka.Offset(0)}
var ref1 = kafka.TopicPartition{Topic: &n, Partition: int32(1), Offset: kafka.Offset(0)}
var ref2 = kafka.TopicPartition{Topic: &n, Partition: int32(2), Offset: kafka.Offset(0)}
var ref3 = kafka.TopicPartition{Topic: &n, Partition: int32(3), Offset: kafka.Offset(0)}
var partitionCnt int = 0

// func consumer(props map[string]string, topic string) {
func consumer(topic string) {
	setTopic()

	filePath, err := findPath("data.txt")
	if err != nil {
		log.Println("error finding the path: ", err)
	}
	writeCH := make(chan string)
	// readCH := make(chan string)

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://127.0.0.1:8081")

	schemaRegistryClient.CodecCreationEnabled(false)

	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	config := &kafka.ConfigMap{
		"bootstrap.servers":  "127.0.0.1:29092",
		"group.id":           "myGroup",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		// "partition.assignment.strategy": "sticky",
	}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// We do not use the call back in this case,
	// instead we wait the service to restart which will re-trigger the event
	// which then will be save
	// c.SubscribeTopics([]string{topic}, myRebalanceCallback)

	// !!! NOTE !!! in this case of ConsumingSpecificOffset
	// we CAN NOT in increase the partitions bc the TopicPartition records won't match anymore
	// or we should insure a StickyMode rebalancing, which look like this library does not allow
	c.SubscribeTopics([]string{topic}, nil)

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

	fileReader(filePath)

	// will seek() each partition to the correct offset which should be next to be proceed
	seekPartitions(c)

	go fileWriter(fh, writeCH)

	for run {
		// fmt.Printf("Received message !!!!!!!!! : %s\n", string(e.Value)

		record, err := c.ReadMessage(-1)
		if err == nil {

			// fmt.Println("See the kafkaTrace: ", kafkaTrace)

			// sensorReading := &pb.SensorReading{}
			msg := &pb.Data{}
			// err = proto.Unmarshal(record.Value[7:], sensorReading)
			err = proto.Unmarshal(record.Value[7:], msg)
			if err != nil {
				panic(fmt.Sprintf("Error deserializing the record: %s", err))
			}

			fmt.Println("")
			fmt.Printf("Message on %s: %s\n", record.TopicPartition, string(record.Value))

			// save the record to db
			// then save the kafka info to db
			// NOTE that 2 save should be done as a transaction
			// then commit the record to kafka

			// NOTE NOW IMAGINE the data and kafka infos are saved to db(as its a transaction)
			// then the app shut down, these data are not commited into kafka
			// and therefore will be replay when restart(and duplicate into db)
			// to avoid that, when the consumer start, it must get the last kafkaDataFromDB
			// then seek() from there and start processing
			saveRecordToDB(string(record.Value))
			writeCH <- record.String()

			// save the record.String() to file

			if count%commitBatchSize == 0 {
				// NOTE, to avoid writing to db at each event
				// we could create a write(batch) wich write each x records
				// it would save a lot of queries
				_ = commitOffsets(c)
			}

			count++

		} else {
			log.Println("The error from c.ReadMessage is not nil: ", err)
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
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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

// end retry logic

// helper
func helperCommitOffsets(ctx context.Context, c *kafka.Consumer, refX kafka.TopicPartition, retryCommitOffsets, retryCommitted kafkaCommitment) {

	// commit the partition first to make sure kafka won't rebalance dor any reason,
	// which may create a re-play of the even, even we seek()
	// tps, err := c.Committed([]kafka.TopicPartition{ref0}, 5000)
	tps, err := retryCommitted(ctx, c, []kafka.TopicPartition{refX}, 100)
	if err != nil {
		fmt.Println("Err commit uncommited offsets at part0: ", err)
	}

	fmt.Println("see commit at 0: ", tps)

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

	// TODO and what if there is a single event on a partition...
	// then diff will be 0..... fix this...
	if diff > 0 {
		listOffsets := []kafka.TopicPartition{}
		for i := 0; i < int(diff); i++ {
			tps[0].Offset = kafka.Offset(int64(tps[0].Offset) + int64(1))
			listOffsets = append(listOffsets, tps[0])
		}
		fmt.Println("See the list of offset to commit: ", listOffsets)

		// commit all the offsets to kafka
		// commited, err := c.CommitOffsets(listOffsets)
		commited, err := retryCommitOffsets(ctx, c, listOffsets, 0)
		if err == nil {
			fmt.Println("See commited offsets' list: ", commited)
		} else {
			log.Println("Error commited listOffsets: ", err)
		}
	}

	// NOTE the pb here is that even I seek() to the next offsets
	// the consumer will redisplay the last commited event...
	// even worst, as I seek() to +1 from the commited offset
	// kafka consumer keep this index as the record's offset
	// but in kafka brocker it's still the real one(mean -1)
	// ex
	// Message on data-database[2]@14: // from kafka broker
	// Annie
	// Committed offsets: [data-database[2]@15] // report of commited offset

	// time.Sleep(time.Second * 10)

	// set the offset to the next one
	// as we do not want to reprocess an already recorded event
	// refX.Offset = kafka.Offset(int64(refX.Offset) + int64(1))

	// log.Println("refx last offset increased: ", refX)

	// err = c.Assign([]kafka.TopicPartition{refX})
	// if err != nil {
	// 	log.Println("Error assign() refX: ", err)
	// }

	// err = c.Seek(refX, 100)
	// if err != nil {
	// 	fmt.Println("Seek error refX: ", err)
	// }

	// time.Sleep(time.Second * 10)
}

func seekPartitions(c *kafka.Consumer) {
	ctx := context.Background()

	// get current partitions
	topicPartitions, err := c.Assignment()
	if err != nil {
		log.Println("Error getting the actual partitions")
	}

	fmt.Println("see the partitions: ", topicPartitions)

	if len(kafkaTrace) > 0 {
		for i := len(kafkaTrace) - 1; i >= 0; i-- {
			fmt.Println("see the range: ", kafkaTrace[i].Partition, " - ", kafkaTrace[i].Offset)
			if partitionCnt < 4 {
				switch kafkaTrace[i].Partition {
				case int32(0):
					if ref0.Offset < kafkaTrace[i].Offset {
						ref0 = kafkaTrace[i]
						partitionCnt++
					}
				case int32(1):
					if ref1.Offset < kafkaTrace[i].Offset {
						ref1 = kafkaTrace[i]
						partitionCnt++
					}
				case int32(2):
					if ref2.Offset < kafkaTrace[i].Offset {
						ref2 = kafkaTrace[i]
						partitionCnt++
					}
				case int32(3):
					if ref3.Offset < kafkaTrace[i].Offset {
						ref3 = kafkaTrace[i]
						partitionCnt++
					}
				default:
					fmt.Println("I am in default what the fu...")
				}
			} else {
				break
			}
		}
	}

	// assign manually all partitions first
	// even the one I settedup by default will be assign
	// which is ok as if they never been used yet they will be set to offset 0
	tps := []kafka.TopicPartition{ref0, ref1, ref2, ref3}
	err = c.Assign(tps)
	if err != nil {
		log.Println("Err assigning TopicPartitions: ", err)
	}

	retryCommitted := retry(committedWrapper, 6, time.Second*10)
	retryCommitOffsets := retry(commitOffsetsWrapper, 6, time.Second*10)

	// seek() on each partition only if the partition offest is not 0
	if int64(ref0.Offset) != int64(0) {
		fmt.Println("in 0: ", ref0)

		helperCommitOffsets(ctx, c, ref0, retryCommitOffsets, retryCommitted)

	}
	if int64(ref1.Offset) != int64(0) {
		fmt.Println("in 1: ", ref1)

		helperCommitOffsets(ctx, c, ref1, retryCommitOffsets, retryCommitted)

	}
	if int64(ref2.Offset) != int64(0) {
		fmt.Println("in 2: ", ref2)

		helperCommitOffsets(ctx, c, ref2, retryCommitOffsets, retryCommitted)

	}
	if int64(ref3.Offset) != int64(0) {
		fmt.Println("in 3: ", ref3)

		helperCommitOffsets(ctx, c, ref3, retryCommitOffsets, retryCommitted)

	}

}

// database = append(database, Data{data})

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

// We do not use the call back in this case,
// instead we wait the service to restart which will re-trigger the event
// which then will be save
// !!! NOTE !!! in this case of ConsumingSpecificOffset
// we CAN NOT in increase the partitions bc the TopicPartition records won't match anymore
// or we should insure a StickyMode rebalancing, which look like this library does not allow
func myRebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		// TODO check the assigned partition
		// parse the file and set only the TopicPartition matching the assigned partitions
		// seek()

		// Handle assigned partitions
		fmt.Printf("Assigned partitions: %v\n", ev.Partitions)

		// commitOffsets(c)
		// Start consuming from the newly assigned partitions

	case kafka.RevokedPartitions:
		// Handle revoked partitions
		fmt.Printf("Revoked partitions: %v\n", ev.Partitions)

		commitOffsets(c)
		// TODO
		// save to the file/db the commited partition

		// Stop consuming from the revoked partitions and perform any necessary cleanup or checkpointing operations
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
