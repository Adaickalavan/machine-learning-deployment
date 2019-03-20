package models

import (
	"errors"

	"gocv.io/x/gocv"
)

//Handler provides interface to send and receive data from the TensorFlowServing API
type Handler interface {
	Predict()
	Get() (Output, error)
	Post(Input)
}

//Input represents input to machine learning model
type Input struct {
	Img gocv.Mat
}

//Output represents output of machine learning model
type Output struct {
	Class string
}

type baseHandler struct {
	labels map[int]string
	url    string
	chIn   chan Input
	chOut  chan Output
}

func (base *baseHandler) Post(input Input) {
	select {
	case base.chIn <- input:
	default:
	}
}

func (base *baseHandler) Get() (Output, error) {
	select {
	case result := <-base.chOut:
		return result, nil
	default:
		return Output{}, errors.New("No TFServing reply available")
	}
}
