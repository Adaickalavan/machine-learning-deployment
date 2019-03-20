package main

import (
	"confluentkafkago"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	// Load env variables
	broker := os.Getenv("KAFKAPORT")
	topicIn := []string{os.Getenv("TOPICNAMEIN")}
	group := os.Getenv("GROUPNAME")

	//Create consumer
	c, err := confluentkafkago.NewConsumer(broker, group)
	if err != nil {
		log.Fatal(err)
	}

	//Subscribe to topics
	err = c.SubscribeTopics(topicIn, nil)
	if err != nil {
		log.Fatal(err)
	}

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
			handler(ev)
		default:
			// log.Println("Ignored")
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

func writeOutput() {
	// Stream images from RTSP to Kafka message queue
	frame := gocv.NewMat()
	for {
		if !webcam.Read(&frame) {
			continue
		}

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
			log.Fatal("Json marshalling error. Error:", err.Error())
		}

		//Send message into Kafka queue
		p.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          docBytes,
			Timestamp:      time.Now(),
		}

		log.Println("row :", frame.Rows(), " col: ", frame.Cols())

	}

	// Close the producer
	p.Flush(10000)
	p.Close()
}
