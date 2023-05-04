package server

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/viktor-viktor/rckafka/internal/producer"
)

var (
	producers map[string]producer.Producer
)

func init() {
	producers = make(map[string]producer.Producer)
}

type server struct {
	engine *gin.Engine
	addr   string
}

func NewServer(addr string) server {
	return server{engine: gin.Default(), addr: addr}
}

func (s *server) Serve() error {
	return s.engine.Run(s.addr)
}

func (s *server) Initialize() {
	s.engine.POST("/produce", handleProduce)
	s.engine.POST("/register-writer", handleRegisterWriter)
}

// handleProduce produces a message to according to the data specified at produceRequest
func handleProduce(c *gin.Context) {
	s := produceRequest{}
	if err := s.initialize(c.Request.Body); err != nil {
		setBadRequest(c, err)
		return
	}
	if err := s.Validate(); err != nil {
		setBadRequest(c, err)
		return
	}
	p := producers[s.Producer]
	if p == nil {
		setBadRequest(c, fmt.Errorf("couldn't find following producer: %s", s.Producer))
		return
	}

	if err := p.Send(s.Data); err != nil {
		setBadRequest(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"Status": "Message sent", // To topic, maybe more info.
	})
}

// handleRegisterWriter registers new producer depending on provided registerProducerRequest.
func handleRegisterWriter(c *gin.Context) {
	wc := registerProducerRequest{}
	if err := wc.initialize(c.Request.Body); err != nil {
		setBadRequest(c, err)
		return
	}

	if err := producerFabric(wc); err != nil {
		setBadRequest(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"Status": "Registered", // To topic, maybe more info.
	})
}

func producerFabric(wc registerProducerRequest) (err error) {
	switch wc.Type {
	case "kafka":
		p := producer.NewKafkaWriter()
		err = p.Connect(wc.Data)
		if err == nil {
			producers[wc.Name] = p
		}
	default:
		err = fmt.Errorf("currently suppored producer type is 'kafka' only. You provided: %s", wc.Type)
	}

	return
}

func setBadRequest(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, gin.H{
		"Error": err.Error(),
	})
}
