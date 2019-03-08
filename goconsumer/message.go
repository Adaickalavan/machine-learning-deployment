package main

import (
	"encoding/json"
	"image"
	"image/color"
	"imagenet"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocv.io/x/gocv"
)

var imNet1 = imagenet.NewModel(os.Getenv("MLURL1")) // Initialize imagenet model
var imNet2 = imagenet.NewModel(os.Getenv("MLURL1")) // Initialize imagenet model

func init() {
	go imNet1.Predict()
	go imNet2.Predict()
}

type message struct {
	status1      string
	status2      string
	statusColor color.RGBA
	window      *gocv.Window
}

func newMessage() *message {
	return &message{
		status1:      "Nothing1",
		status2:      "Nothing2",
		statusColor: color.RGBA{200, 150, 50, 0},
		window:      gocv.NewWindow("Consumer Video"),
	}
}

func (msg *message) handler(ev *kafka.Message) error {

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

	//Query machine learning model. Read and write to their input and output channels.
	select {
	case res := <-imNet1.ChOut:
		msg.status1 = res.Class
		imNet1.ChIn <- imagenet.In{Img: frame} //Load latest available image 
	default:
	}
	select {
	case res := <-imNet2.ChOut:
		msg.status2 = res.Class
		imNet2.ChIn <- imagenet.In{Img: frame} //Load latest available image 
	default:
	}

	//Display data
	gocv.PutText(&frame, msg.status1, image.Pt(10, 20), gocv.FontHersheyPlain, 1.2, msg.statusColor, 2)
	gocv.PutText(&frame, msg.status2, image.Pt(10, 40), gocv.FontHersheyPlain, 1.2, msg.statusColor, 2)
	msg.window.ResizeWindow(doc.Cols, doc.Rows)
	msg.window.IMShow(frame)
	msg.window.WaitKey(1)

	return nil
}

type topicMsg struct {
	Mat      []byte       `json:"mat"`
	Channels int          `json:"channels"`
	Rows     int          `json:"rows"`
	Cols     int          `json:"cols"`
	Type     gocv.MatType `json:"type"`
}
