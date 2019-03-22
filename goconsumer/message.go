package main

import (
	"encoding/json"
	"image"
	"image/color"
	"log"
	"models"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

var statusColor = color.RGBA{200, 150, 50, 0}

func message(ev *kafka.Message) error {

	//Read message into `topicMsg` struct
	doc := &topicMsg{}
	err := json.Unmarshal(ev.Value, doc)
	if err != nil {
		log.Println(err)
		return err
	}

	// Retrieve frame
	log.Printf("%% Message sent %v on %s\n", ev.Timestamp, ev.TopicPartition)
	frame, err := gocv.NewMatFromBytes(doc.Rows, doc.Cols, doc.Type, doc.Mat)
	if err != nil {
		log.Println("Frame:", err)
		return err
	}

	// Query machine learning model.
	for _, mp := range modelParams {
		// Read from input channels
		mp.modelHandler.Post(models.Input{Img: frame})

		// Write to output channels
		res, err := mp.modelHandler.Get()
		if err == nil {
			mp.pred = res.Class
		}
	}

	// Form output image
	for ind := 0; ind < len(modelParams); ind++ {
		gocv.PutText(
			&frame,
			modelParams[ind].modelName+" : "+modelParams[ind].pred,
			image.Pt(10, ind*20+20),
			gocv.FontHersheyPlain, 1.2,
			statusColor, 2,
		)
	}

	// Write image to output Kafka queue
	videoDisplay <- frame

	return nil
}

type topicMsg struct {
	Mat      []byte       `json:"mat"`
	Channels int          `json:"channels"`
	Rows     int          `json:"rows"`
	Cols     int          `json:"cols"`
	Type     gocv.MatType `json:"type"`
}
