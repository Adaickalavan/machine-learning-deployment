package main

import (
	"confluentkafkago"
	"encoding/json"
	"log"
	"models"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/profile"
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
		parts := strings.Split(modelName, "_")
		switch parts[0] {
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
	// Profiling CPU
	defer profile.Start(profile.CPUProfile, profile.ProfilePath("/tmp"), profile.NoShutdownHook).Stop()
	// Profiling memory
	// defer profile.Start(profile.MemProfile, profile.ProfilePath("/tmp"), profile.NoShutdownHook).Stop()

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
			log.Printf("%% %v\n", ev)
			c.Assign(ev.Partitions)
			continue
		case kafka.RevokedPartitions:
			log.Printf("%% %v\n", ev)
			c.Unassign()
			continue
		case kafka.PartitionEOF:
			log.Printf("%% Reached %v\n", ev)
			continue
		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			log.Printf("%% Error: %v\n", ev)
			continue
		case *kafka.Message:
			err := message(ev)
			if err != nil {
				log.Println("Error in reading Kafka.Message", err)
			}
		default:
			log.Println("Ignored")
			continue
		}

		// Reset offset to latest committed message
		confluentkafkago.LatestOffset(c, 100)
	}
}

func writeOutput(videoDisplay chan gocv.Mat) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("main.writeOutput():PANICKED AND RESTARTING")
			log.Println("Panic:", r)
			go writeOutput(videoDisplay)
		}
	}()

	broker := os.Getenv("KAFKAPORTOUT")
	topic := os.Getenv("TOPICNAMEOUT")
	compression := os.Getenv("COMPRESSIONTYPE")

	p, _, err := confluentkafkago.NewProducer(broker, compression)
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

		log.Printf("%% Message rewritten into Kafka at %v\n", time.Now())
	}
}
