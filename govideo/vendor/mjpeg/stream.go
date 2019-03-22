// Package mjpeg implements a simple MJPEG streamer.
//
// Stream objects implement the http.Handler interface, allowing to use them with the net/http package like so:
//	stream = mjpeg.NewStream()
//	http.Handle("/camera", stream)
// Then push new JPEG frames to the connected clients using stream.UpdateJPEG().
package mjpeg

import (
	"confluentkafkago"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

// Stream represents a single video feed.
type Stream struct {
	broker        string
	topics        []string
	group         string
	frameInterval time.Duration
}

const boundaryWord = "MJPEGBOUNDARY"
const headerf = "\r\n" +
	"--" + boundaryWord + "\r\n" +
	"Content-Type: image/jpeg\r\n" +
	"Content-Length: %d\r\n" +
	"X-Timestamp: 0.000000\r\n" +
	"\r\n"

// ServeHTTP responds to HTTP requests with the MJPEG stream, implementing the http.Handler interface.
func (s *Stream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println("Stream:", r.RemoteAddr, "connected")
	w.Header().Add("Content-Type", "multipart/x-mixed-replace;boundary="+boundaryWord)

	doc := &topicMsg{}

	//Create new Consumer in a new ConsumerGroup
	c, err := confluentkafkago.NewConsumer(s.broker, time.Now().String())
	if err != nil {
		log.Fatal("Error in creating NewConsumer.", err)
	}
	// Close the consumer
	defer c.Close()

	//Subscribe to topics
	err = c.SubscribeTopics(s.topics, nil)
	if err != nil {
		c.Close()
		log.Fatal("Error in SubscribeTopics.", err)
	}

	// Consume messages
ConsumeLoop:
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

			//Read message into `topicMsg` struct
			err := json.Unmarshal(ev.Value, doc)
			if err != nil {
				// log.Println(err)
				continue
			}

			//Retrieve img
			// log.Printf("%% Message sent %v on %s\n", ev.Timestamp, ev.TopicPartition)
			img, err := gocv.NewMatFromBytes(doc.Rows, doc.Cols, doc.Type, doc.Mat)
			if err != nil {
				// log.Println("Frame:", err)
				continue
			}

			//Encode gocv mat to jpeg
			jpeg, err := gocv.IMEncode(gocv.JPEGFileExt, img)
			if err != nil {
				// log.Println("Error in IMEncode:", err)
				continue
			}

			// UpdateJPEG pushes a new JPEG frame onto the clients.
			header := fmt.Sprintf(headerf, len(jpeg))
			frame := make([]byte, (len(jpeg)+len(header))*2)
			copy(frame, header)
			copy(frame[len(header):], jpeg)

			//Write new frame to web
			_, err = w.Write(frame)
			if err != nil {
				break ConsumeLoop
			}

			//Wait for xx milliseconds
			time.Sleep(s.frameInterval * time.Millisecond)

		default:
			// log.Println("Ignored")
			continue
		}

		//Record the current topic-partition assignments
		tpSlice, err := c.Assignment()
		if err != nil {
			// log.Println(err)
			continue
		}

		//Obtain the last message offset for all topic-partition
		for index, tp := range tpSlice {
			_, high, err := c.QueryWatermarkOffsets(*(tp.Topic), tp.Partition, 100)
			if err != nil {
				// log.Println(err)
				continue
			}
			tpSlice[index].Offset = kafka.Offset(high)
		}

		//Consume the last message in topic-partition
		c.Assign(tpSlice)
	}

	log.Println("Stream:", r.RemoteAddr, "disconnected")
}

type topicMsg struct {
	Mat      []byte       `json:"mat"`
	Channels int          `json:"channels"`
	Rows     int          `json:"rows"`
	Cols     int          `json:"cols"`
	Type     gocv.MatType `json:"type"`
}

// NewStream initializes and returns a new Stream.
func NewStream(broker string, topics []string, group string, frameInterval time.Duration) *Stream {
	return &Stream{
		broker:        broker,
		topics:        topics,
		group:         group,
		frameInterval: frameInterval,
	}
}
