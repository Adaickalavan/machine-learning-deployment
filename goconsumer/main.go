package main

import (
	"confluentkafkago"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	err    error
	webcam *gocv.VideoCapture
	stream *mjpeg.Stream
)

func main() {

	// Load env variables
	broker := os.Getenv("KAFKAPORT")
	topics := []string{os.Getenv("TOPICNAME")}
	group := os.Getenv("GROUPNAME")
	host := os.Getenv("HOST")

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

	// create the mjpeg stream
	stream = mjpeg.NewStream()

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
			msg.handler(ev)
		default:
			log.Println("Ignored")
			continue
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

	// Close the consumer
	c.Close()
}
