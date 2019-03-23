package confluentkafkago

import (
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
