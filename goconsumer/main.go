package main

import (
	"confluentkafkago"
	"encoding/json"
	"log"
	"models"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

var modelurls = make(map[string]string)
var labelurls = make(map[string]string)
var modelParams = make(map[int]*modelParam)
var videoDisplay = make(chan gocv.Mat)

type modelParam struct {
	modelHandler models.Handler
	modelName    string
	pred         string
}

func init() {
	// Read-in modelurls and labelurls
	err := json.Unmarshal([]byte(os.Getenv("MODELURLS")), &modelurls)
	if err != nil {
		log.Fatal("Invalid model urls", err)
	}
	err = json.Unmarshal([]byte(os.Getenv("LABELURLS")), &labelurls)
	if err != nil {
		log.Fatal("Invalid label urls", err)
	}

	//Create models and start prediction
	ind := -1
	for modelName, modelurl := range modelurls {
		labelurl, ok := labelurls[modelName]
		if !ok {
			log.Fatal("Missing label url", err)
		}

		var modelHandler models.Handler
		switch modelName {
		case "imagenet":
			modelHandler, err = models.NewImagenet(modelurl, labelurl)
			if err != nil {
				log.Fatal("Failed to create modelHandler", err)
			}
		default:
			log.Fatal("Model not recognised")
		}
		go modelHandler.Predict()

		ind = ind + 1
		modelParams[ind] = &modelParam{
			modelHandler: modelHandler,
			modelName:    modelName,
			pred:         "Nothing"}
	}
}

func main() {

	// Load env variables
	broker := os.Getenv("KAFKAPORTIN")
	topic := []string{os.Getenv("TOPICNAMEIN")}
	group := os.Getenv("GROUPNAME")

	//Create consumer
	c, err := confluentkafkago.NewConsumer(broker, group)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	//Subscribe to topics
	err = c.SubscribeTopics(topic, nil)
	if err != nil {
		c.Close()
		log.Fatal(err)
	}

	// Start goroutine to write back processed data into Kafka queue
	go writeOutput(videoDisplay)

	// Consume messages
	for e := range c.Events() {
		switch ev := e.(type) {
		case kafka.AssignedPartitions:
			// log.Printf("%% %v\n", ev)
			c.Assign(ev.Partitions)
		case kafka.RevokedPartitions:
			// log.Printf("%% %v\n", ev)
			c.Unassign()
		case kafka.PartitionEOF:
			// log.Printf("%% Reached %v\n", ev)
		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			// log.Printf("%% Error: %v\n", ev)
		case *kafka.Message:
			err := message(ev)
			log.Println("Error in reading Kafka.Message", err)
		default:
			// log.Println("Ignored")
			// continue
		}

		//Record the current topic-partition assignments
		tpSlice, err := c.Assignment()
		if err != nil {
			log.Println(err)
			continue
		}

		//Obtain the last message offset for all topic-partition
		for index, tp := range tpSlice {
			_, high, err := c.QueryWatermarkOffsets(*(tp.Topic), tp.Partition, 100)
			if err != nil {
				log.Println(err)
				continue
			}
			tpSlice[index].Offset = kafka.Offset(high)
		}

		//Consume the last message in topic-partition
		c.Assign(tpSlice)
	}
}

func writeOutput(videoDisplay chan gocv.Mat) {
	defer func() {
		if r := recover(); r != nil {
			go writeOutput(videoDisplay)
		}
	}()

	broker := os.Getenv("KAFKAPORTOUT")
	topic := os.Getenv("TOPICNAMEOUT")

	p, _, err := confluentkafkago.NewProducer(broker)
	if err != nil {
		p.Close()
		log.Panic(err)
	}
	defer func() {
		// Close the producer
		p.Flush(10000)
		p.Close()
	}()

	for frame := range videoDisplay {
		//Form the struct to be sent to Kafka message queue
		doc := topicMsg{
			Mat:      frame.ToBytes(),
			Channels: frame.Channels(),
			Rows:     frame.Rows(),
			Cols:     frame.Cols(),
			Type:     frame.Type(),
		}

		//Prepare message to be sent to Kafka
		docBytes, err := json.Marshal(doc)
		if err != nil {
			log.Println("Json marshalling error. Error:", err.Error())
			continue
		}

		//Send message into Kafka queue
		p.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          docBytes,
			Timestamp:      time.Now(),
		}

		log.Println("row :", frame.Rows(), " col: ", frame.Cols())
	}
}
