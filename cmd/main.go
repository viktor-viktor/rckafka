package main

import "github.com/viktor-viktor/rckafka/internal/server"

func main() {
	s := server.NewServer("0.0.0.0:8081")
	s.Initialize()
	s.Serve()
}
