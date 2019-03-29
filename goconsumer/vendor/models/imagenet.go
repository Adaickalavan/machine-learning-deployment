package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"gocv.io/x/gocv"
)

type imagenet struct {
	baseHandler
}

//NewImagenet returns a new handle to specified machine learning model
func NewImagenet(modelurl string, labelurl string) (Handler, error) {

	labels := make(map[int]string)

	// Read-in labels
	dat, err := ioutil.ReadFile(labelurl)
	if err != nil {
		return &imagenet{}, errors.New("Failed to read in labelurl. " + err.Error())
	}
	err = json.Unmarshal(dat, &labels)
	if err != nil {
		return &imagenet{}, errors.New("Failure in unmarshalling labels. " + err.Error())
	}
	labels[1000] = "Nothing"

	return &imagenet{
		baseHandler{
			labels: labels,
			url:    modelurl,
			chIn:   make(chan Input),
			chOut:  make(chan Output),
		},
	}, nil
}

//Predict classifies input images
func (imn *imagenet) Predict() {
	var resBody responseBody
	defer func() {
		if r := recover(); r != nil {
			log.Println("models.*imagenet.Predict():PANICKED AND RESTARTING")
			log.Println("Panic:", r)
			go imn.Predict()
		}
	}()

	//Write initial prediction into shared output channel
	imn.chOut <- Output{Class: "Nothing"}

	for elem := range imn.chIn {
		img := elem.Img

		//Encode gocv mat to jpeg
		buf, err := gocv.IMEncode(gocv.JPEGFileExt, img)
		if err != nil {
			log.Println("Error in IMEncode:", err)
			continue
		}

		//Prepare request message
		inference := infer{
			Instances: []instance{
				instance{Image: b64Encode{B64: buf}},
			},
		}

		//Query the machine learning model
		reqBody, err := json.Marshal(inference)
		if err != nil {
			log.Println("Error in Marshal: ", err)
			continue
		}
		req, err := http.NewRequest("POST", imn.url, bytes.NewBuffer(reqBody))
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
		// var resBody responseBody
		decoder := json.NewDecoder(res.Body)
		if err := decoder.Decode(&resBody); err != nil {
			log.Println("Error in Decode: ", err)
			continue
		}
		predClass := resBody.Predictions[0].Classes
		pred, ok := imn.labels[predClass-1]
		if !ok {
			pred = imn.labels[1000]
		}

		//Write prediction into shared output channel
		imn.chOut <- Output{Class: pred}
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
