package confluentkafkago

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//NewConsumer returns a new consumer
func NewConsumer(broker string, group string) (*kafka.Consumer, error) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.events.channel.size":          1,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "earliest",
	})

	if err != nil {
		log.Printf("Failed to create consumer: %s\n", err)
		return c, err
	}
	log.Printf("Created consumer %v\n", c)

	go func() {
		sig := <-sigChan
		log.Printf("Caught signal %v: terminating\n", sig)
		log.Printf("Closing consumer %v\n", c)
		c.Close()
	}()

	return c, err
}

// LatestOffset resets consumer offset to the latest message in the topic,
// if the difference between the low and high watermark is more than the
// desired diff.
func LatestOffset(c *kafka.Consumer, diff int) error {

	// Record the current topic-partition assignments
	tpSlice, err := c.Assignment()
	if err != nil {
		return err
	}

	//Obtain the last message offset for all topic-partition
	for index, tp := range tpSlice {
		low, high, err := c.QueryWatermarkOffsets(*(tp.Topic), tp.Partition, 100)
		if err != nil {
			return err
		}
		if high-low < int64(diff) {
			return errors.New("Offset difference between Low and High is smaller than diff")
		}
		tpSlice[index].Offset = kafka.Offset(high)
	}

	//Consume the last message in topic-partition
	err = c.Assign(tpSlice)
	if err != nil {
		return err
	}
	return nil
}
