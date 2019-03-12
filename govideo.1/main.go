package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/hybridgroup/mjpeg"
	"gocv.io/x/gocv"
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

	// Capture video from internet stream
	webcam, err = gocv.OpenVideoCapture(os.Getenv("VIDEOLINK"))
	if err != nil {
		panic("Error in opening webcam: " + err.Error())
	}
	defer webcam.Close()

	// create the mjpeg stream
	stream = mjpeg.NewStream()

	// start capturing
	go mjpegCapture()

	fmt.Println("Capturing. Point your browser to " + host)

	// start http server
	http.Handle("/", stream)
	log.Fatal(http.ListenAndServe(host, nil))
}

func mjpegCapture() {
	img := gocv.NewMat()
	defer img.Close()

	for {
		if ok := webcam.Read(&img); !ok {
			fmt.Printf("Stream closed\n")
			return
		}
		if img.Empty() {
			continue
		}

		buf, _ := gocv.IMEncode(".jpg", img)
		stream.UpdateJPEG(buf)
	}
}
