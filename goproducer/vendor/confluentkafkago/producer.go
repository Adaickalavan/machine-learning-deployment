package confluentkafkago

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// NewProducer returns a new producers
func NewProducer(broker string, compression string) (*kafka.Producer, chan bool, error) {
	doneChan := make(chan bool)

	// Create producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"message.max.bytes": 100000000, //100MB. Defaults to 1MB. Maximum is 1GB.
		"compression.type":  compression,
	})
	if err != nil {
		log.Printf("Failed to create producer: %s\n", err)
		return p, doneChan, err
	}
	log.Printf("Created new producer %v\n", p)

	//Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					// log.Printf("Delivered message to topic %s [%d] at offset %v\n",
					// 	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					//Send delivery report
					// doneChan <- true
				}
			default:
				log.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	return p, doneChan, err
}
