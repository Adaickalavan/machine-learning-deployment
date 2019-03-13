package main

import (
	"fmt"
	"log"
	"mjpeg"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	err    error
	stream *mjpeg.Stream
	// Load env variables
	broker        = os.Getenv("KAFKAPORT")
	topics        = []string{os.Getenv("TOPICNAME")}
	// group         = os.Getenv("GROUPNAME")
	displayaddr   = os.Getenv("DISPLAYADDR")
	frameinterval = time.Duration(getenvint("FRAMEINTERVAL"))
)

func main() {
	// create the mjpeg stream
	stream = mjpeg.NewStream()

	stream.FrameInterval = frameinterval
	stream.Broker = broker
	stream.Topics = topics

	// start capturing
	fmt.Println("Capturing. Point your browser to " + displayaddr)

	// start http server
	http.Handle("/", stream)
	log.Fatal(http.ListenAndServe(displayaddr, nil))
}

func getenvint(str string) int {
	i, err := strconv.Atoi(os.Getenv(str))
	if err != nil {
		log.Fatal(err)
	}
	return i
}
