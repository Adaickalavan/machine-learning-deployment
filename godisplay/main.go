package main

import (
	"confluentkafkago"
	"encoding/json"
	"image"
	"image/color"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

func main() {

	// Load env variables
	broker := os.Getenv("KAFKAPORT")
	topics := []string{os.Getenv("TOPICNAME")}
	group := os.Getenv("GROUPNAME")

	//Create consumer
	c, err := confluentkafkago.NewConsumer(broker, group)
	if err != nil {
		log.Fatal(err)
	}

	//Subscribe to topics
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatal(err)
	}

	//Initialize message handler
	msg := newMessage()

	// Consume messages
	for e := range c.Events() {
		switch ev := e.(type) {
		case kafka.AssignedPartitions:
			log.Printf("%% %v\n", ev)
			c.Assign(ev.Partitions)
		case kafka.RevokedPartitions:
			log.Printf("%% %v\n", ev)
			c.Unassign()
		case kafka.PartitionEOF:
			log.Printf("%% Reached %v\n", ev)
		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			log.Printf("%% Error: %v\n", ev)
		case *kafka.Message:
			if string(ev.Key) == "video" {
				msg.videoHandler(ev)
			} else {
				msg.statusHandler(ev)
			}
		default:
			log.Println("Ignored")
			continue
		}
	}

	// Close the consumer
	c.Close()
}

type message struct {
	status      map[string]string
	statusColor color.RGBA
	window      *gocv.Window
}

func newMessage() *message {
	return &message{
		status:      make(map[string]string),
		statusColor: color.RGBA{200, 150, 50, 0},
		window:      gocv.NewWindow("Consumer Video"),
	}
}

func (msg *message) videoHandler(ev *kafka.Message) error {

	//Read message into `topicMsg` struct
	doc := &topicMsg{}
	err := json.Unmarshal(ev.Value, doc)
	if err != nil {
		log.Println(err)
		return err
	}

	//Retrieve frame
	log.Printf("%% Message sent %v on %s\n", ev.Timestamp, ev.TopicPartition)
	frame, err := gocv.NewMatFromBytes(doc.Rows, doc.Cols, doc.Type, doc.Mat)
	if err != nil {
		log.Println("Frame:", err)
		return err
	}

	msg.window.ResizeWindow(doc.Cols, doc.Rows)
	msg.window.IMShow(frame)
	msg.window.WaitKey(1)

	return nil
}

type topicVideo struct {
	Mat      []byte       `json:"mat"`
	Channels int          `json:"channels"`
	Rows     int          `json:"rows"`
	Cols     int          `json:"cols"`
	Type     gocv.MatType `json:"type"`
}

func (msg *message) statusHandler(ev *kafka.Message) error {

	//Read message into `topicMsg` struct
	doc := &topicStatus{}
	err := json.Unmarshal(ev.Value, doc)
	if err != nil {
		log.Println(err)
		return err
	}

	//Store updated status in map
	msg.status[string(ev.key)] = doc.status

	//Display data
	for key, val := range msg.status {
		gocv.PutText(&frame, val, image.Pt(10, 20), gocv.FontHersheyPlain, 1.2, msg.statusColor, 2)
	}
	msg.window.IMShow(frame)
	msg.window.WaitKey(1)

	return nil
}

type topicStatus struct {
	status string `json:"status"`
}
