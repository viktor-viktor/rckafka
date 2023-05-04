package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/kafka-go"
)

type kafkaWriter struct {
	writer *kafka.Writer
}

var _ Producer = &kafkaWriter{}

func (c *kafkaWriter) Connect(data interface{}) error {
	kd := kafkaConfig{}
	err := mapstructure.Decode(data, &kd)
	if err != nil {
		return fmt.Errorf("error when decoding kafka config: %s", err.Error())
	}

	c.writer = &kafka.Writer{
		Addr:     kafka.TCP(kd.Brokers...),
		Topic:    kd.Topic,
		Async:    false,
		Balancer: &kafka.LeastBytes{},
	}

	return nil
}

func (c *kafkaWriter) Send(message interface{}) error {
	m := kafkaMessage{}
	err := mapstructure.Decode(message, &m)
	if err != nil {
		return fmt.Errorf("failed to cast message to kafkaMessage: %v", err.Error())
	}

	fmt.Println("Sending message: ", m, message)

	bm, err := json.Marshal(m.Message)
	if err != nil {
		return fmt.Errorf("failed to encrypt message. Error: :%v", err.Error())
	}

	err = c.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(m.Key),
		Value: []byte(bm),
	})
	if err != nil {
		return fmt.Errorf("Failed to send message. Error: :%v", err.Error())
	}

	return nil
}

func NewKafkaWriter() Producer {
	return &kafkaWriter{}
}

// kafkaConfig is a schema for /register-writer request for data of 'kafka' type.
type kafkaConfig struct {
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
}

// kafkaMessage is a schema per /produce request for data of the 'kafka' type.
type kafkaMessage struct {
	Key     string                 `json:"key"`
	Message map[string]interface{} `json:"message"`
}
