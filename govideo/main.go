package main

import (
	"confluentkafkago"
	"encoding/json"
	"fmt"
	"log"
	"mjpeg"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

var (
	stream      *mjpeg.Stream
	broker      = os.Getenv("KAFKAPORT")
	topics      = []string{os.Getenv("TOPICNAME")}
	group       = os.Getenv("GROUPNAME")
	displayport = os.Getenv("DISPLAYPORT")
	nodeport    = os.Getenv("NODEPORT")
)

func main() {
	frameInterval, err := time.ParseDuration(os.Getenv("FRAMEINTERVAL"))
	if err != nil {
		log.Fatal("Invalid frame interval", err)
	}
	// Create new mjpeg stream
	stream = mjpeg.NewStream(frameInterval)

	// Create new Consumer in a new ConsumerGroup
	c, err := confluentkafkago.NewConsumer(broker, group)
	if err != nil {
		log.Fatal("Error in creating NewConsumer.", err)
	}
	// Close the consumer
	defer c.Close()

	// Subscribe to topics
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		c.Close()
		log.Fatal("Error in SubscribeTopics.", err)
	}

	// Start consuming messages
	go consumeMessages(c)

	// Start capturing
	fmt.Println("Capturing. Point your browser to " + nodeport)

	// Start http server
	http.Handle("/", stream)
	log.Fatal(http.ListenAndServe(displayport, nil))
}

func consumeMessages(c *kafka.Consumer) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("main.consumeMessages():PANICKED AND RESTARTING")
			log.Println("Panic:", r)
			go consumeMessages(c)
		}
	}()

	doc := &topicMsg{}

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

			//Read message into `topicMsg` struct
			err := json.Unmarshal(ev.Value, doc)
			if err != nil {
				log.Println(err)
				continue
			}

			//Retrieve img
			log.Printf("%% Message sent %v on %s\n", ev.Timestamp, ev.TopicPartition)
			img, err := gocv.NewMatFromBytes(doc.Rows, doc.Cols, doc.Type, doc.Mat)
			if err != nil {
				log.Println("Frame:", err)
				continue
			}

			//Encode gocv mat to jpeg
			buf, err := gocv.IMEncode(gocv.JPEGFileExt, img)
			if err != nil {
				log.Println("Error in IMEncode:", err)
				continue
			}

			stream.UpdateJPEG(buf)

		default:
			log.Println("Ignored")
			continue
		}

		// Reset offset to latest committed message
		confluentkafkago.LatestOffset(c, 100)
	}
}

type topicMsg struct {
	Mat      []byte       `json:"mat"`
	Channels int          `json:"channels"`
	Rows     int          `json:"rows"`
	Cols     int          `json:"cols"`
	Type     gocv.MatType `json:"type"`
}
