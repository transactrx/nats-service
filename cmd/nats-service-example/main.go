package main

import (
	"errors"
	"fmt"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func getTime(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {

	s := fmt.Sprintf("The time is %s", time.Now())

	msg.Logger.Printf("received a message")
	msg.ResponseBody = []byte(s)

	msg.Logger.Printf("completed function ")

	return nil
}

func getTimeError(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {

	natsError := nats_service.NewValidationError("this is the error message", 2500, errors.New("this is an exception - usually nil"))

	return &natsError
}

func main() {

	natservice, err := nats_service.New("rx.api")

	if err != nil {
		log.Panicln(err)
	}

	natservice.AddEndpoint("getTime/.*", getTime)
	natservice.AddEndpoint("getTimeError", getTimeError)

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
