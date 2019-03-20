package main

import (
	"encoding/json"
	"image"
	"image/color"
	"log"
	"models"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

var modelurls = make(map[string]string)
var labelurls = make(map[string]string)
var modelParams = make(map[int]*modelParam)
var statusColor = color.RGBA{200, 150, 50, 0}

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
		switch modelName {
		case "imagenet":
			modelHandler = models.NewImagenet(modelurl, labelurl)
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

func handler(ev *kafka.Message) error {

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

	//Display data
	for ind := 0; ind < len(modelParams); ind++ {
		gocv.PutText(
			&frame,
			modelParams[ind].modelName+" : "+modelParams[ind].pred,
			image.Pt(10, 20),
			gocv.FontHersheyPlain, 1.2,
			statusColor, 2,
		)
	}

	return nil
}

type topicMsg struct {
	Mat      []byte       `json:"mat"`
	Channels int          `json:"channels"`
	Rows     int          `json:"rows"`
	Cols     int          `json:"cols"`
	Type     gocv.MatType `json:"type"`
}
