package main

import (
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	"log"
	"os"
	"time"
)

func main() {

	frank

	natsUrl := os.Getenv("NATS_URL")
	natsToken := os.Getenv("NATS_JWT")
	natsKey := os.Getenv("NATS_KEY")

	if natsUrl == "" {
		log.Panic("Environment variable NATS_URL is missing.")
	}

	if natsToken == "" {
		log.Panic("Environment variable NATS_JWT is missing.")
	}

	if natsKey == "" {
		log.Panic("Environment variable NATS_KEY is missing.")
	}

	var opts []nats.Option
	opts = []nats.Option{nats.UserJWTAndSeed(natsToken, natsKey)}
	opts = nats_service.SetupConnOptions(opts)
	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		log.Printf("Connect failed error: %v", err)
	}

	if err != nil {
		log.Panic(err)
	}

	//for i := 0; i < 100; i++ {
	//	go makeCall(nc)
	//}

	makeFetchClientCall(nc)
	makeFetchClientCallError(nc)

	//runtime.Goexit()
}

func makeCall(nc *nats.Conn) {
	msg, err := nc.Request("getTime", []byte("hello"), time.Second*10)
	if err != nil {
		log.Printf("error: %v", err)
	}

	log.Printf("Reply: %s", msg.Data)
}

func makeFetchClientCall(nc *nats.Conn) {

	requestMsg := nats.Msg{}
	requestMsg.Header = nats.Header{}
	requestMsg.Subject = "rx.api.getTime/utc"

	msg, err := nc.RequestMsg(&requestMsg, time.Second*10)
	if err != nil {
		log.Printf("error: %v", err)
	}

	log.Printf("Reply: %s", msg.Data)
}

func makeFetchClientCallError(nc *nats.Conn) {

	requestMsg := nats.Msg{}
	requestMsg.Header = nats.Header{}
	requestMsg.Subject = "rx.api.getTimeError"

	msg, err := nc.RequestMsg(&requestMsg, time.Second*10)
	if err != nil {
		log.Printf("error: %v", err)
	}

	log.Printf("Reply: %s", msg.Data)
}
