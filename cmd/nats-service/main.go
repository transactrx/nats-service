package main

import (
	"fmt"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func getTime(msg *nats_service.NatsMessage) error {
	s := fmt.Sprintf("The time is %s", time.Now())
	msg.ResponseBody = []byte(s)

	msg.Logger.Printf("received a message")

	return nil
}

func main() {

	natservice, err := nats_service.New("rx.api")

	if err != nil {
		log.Panicln(err)
	}

	natservice.AddEndpoint("getTime", getTime)

	err = natservice.Start()
	if err != nil {
		log.Panicln(err)
	}

	sigChan := make(chan os.Signal)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		_ = <-sigChan

		err := natservice.Shutdown()
		if err != nil {
			log.Printf("Error: Shutdown failed, %v", err)
		} else {
			log.Printf("Shutdown Succesfull, yea!")
		}
	}()

	runtime.Goexit()

	log.Printf("exiting...")
}
