package imagenet

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"gocv.io/x/gocv"
)

//Model trained for imagenet. Consumes input via ChIn channel and returns via Chout channel.
type Model struct {
	labels map[int]string
	url    string
	ChIn   chan In
	ChOut  chan Out
}

//NewModel is a Model constructor
func NewModel(url string) *Model {

	labels := make(map[int]string)

	// Read-in labels for mlurl1
	dat, err := ioutil.ReadFile(os.Getenv("MLLABELS1"))
	if err != nil {
		log.Fatal("File read-in error.", err)
	}
	err = json.Unmarshal(dat, &labels)
	if err != nil {
		log.Fatal("Invalid labels", err)
	}
	labels[1000] = "Nothing"

	return &Model{
		labels: labels,
		url:    url,
		ChIn:   make(chan In, 1),
		ChOut:  make(chan Out, 1),
	}
}

//In represents input for the model
type In struct {
	Img gocv.Mat
}

//Out represents output for the model
type Out struct {
	Class string
}

//Predict classifies input images
func (model *Model) Predict() {

	//Write initial prediction into shared output channel
	model.ChOut <- Out{Class: "Nothing"}

	for elem := range model.ChIn {
		img := elem.Img

		//Encode gocv mat to jpeg
		buf, err := gocv.IMEncode(gocv.JPEGFileExt, img)
		if err != nil {
			log.Println("Error in IMEncode:", err)
			continue
		}

		//Prepare request message
		payload := infer{
			Instances: []instance{
				instance{Image: b64Encode{B64: buf}},
			},
		}

		//Query the machine learning model
		reqBody, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error in Marshal: ", err)
			continue
		}
		req, err := http.NewRequest("POST", model.url, bytes.NewBuffer(reqBody))
		if err != nil {
			log.Println("Error in NewRequest: ", err)
			continue
		}
		req.Header.Add("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("Error in DefaultClient: ", err)
			continue
		}
		defer res.Body.Close()

		//Process response from machine learning model
		var resBody responseBody
		decoder := json.NewDecoder(res.Body)
		if err := decoder.Decode(&resBody); err != nil {
			log.Println("Error in Decode: ", err)
			continue
		}
		predClass := resBody.Predictions[0].Classes
		pred, ok := model.labels[predClass-1]
		if !ok {
			pred = model.labels[1000]
		}

		//Write prediction into shared output channel
		model.ChOut <- Out{Class: pred}
	}
}

type infer struct {
	Instances []instance `json:"instances"`
}

type instance struct {
	Image b64Encode `json:"image_bytes"` //json name must end with `_bytes` for json to know that it is binary data
}

type b64Encode struct {
	B64 []byte `json:"b64"`
}

type responseBody struct {
	Predictions []prediction `json:"predictions"`
}

type prediction struct {
	Classes       int       `json:"classes"`
	Probabilities []float64 `json:"probabilities"`
}
