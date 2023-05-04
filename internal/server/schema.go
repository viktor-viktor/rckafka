package server

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
)

// produceRequest is a schema for /produce endpoint.
// producer is a name of previously registered producers.
// data is any string->any json data that is passed to producer. Validation of the data
// preformed by producer itself.
type produceRequest struct {
	Producer string                 `json:"producer"`
	Data     map[string]interface{} `json:"data"`
}

func (s *produceRequest) initialize(r io.ReadCloser) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Error decoding body: %v", err.Error())
	}
	err = json.Unmarshal(body, s)
	if err != nil {
		return fmt.Errorf("Error decoding body: %v", err.Error())
	}

	return nil
}

func (s produceRequest) Validate() error {
	msg := "Body validation failed: %v"
	if s.Producer == "" { // Should check against the list of predefined topics
		return fmt.Errorf(msg, "producer is empty")
	} else if len(s.Data) == 0 {
		return fmt.Errorf("message data isn't provided")
	}

	return nil
}

// registerProducerRequest represents request schema for "/register-producer" endpoint.
// type should have one of the supported clients to communicate. Currently it's only 'kafka'
// name is simply an identifier of this producer that is used in /produce.
// data any data required by the specific producer type.
type registerProducerRequest struct {
	Type string                 `json:"type"`
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

func (c *registerProducerRequest) initialize(r io.ReadCloser) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Error decoding body: %v", err.Error())
	}
	err = json.Unmarshal(body, c)
	if err != nil {
		return fmt.Errorf("Error decoding body: %v", err.Error())
	}

	return nil
}
