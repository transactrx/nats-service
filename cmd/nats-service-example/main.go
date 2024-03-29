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

	paramOne := msg.Parameters["parameterOne"]
	paramTwo := msg.Parameters["parameterTwo"]

	log.Printf("paramterOne: %s parameterTwo: %s", paramOne, paramTwo)

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

	err = natservice.AddEndpoint("getTime/:parameterOne/:parameterTwo", getTime)
	if err != nil {
		log.Panicln(err)
	}
	natservice.AddEndpoint("getTimeError", getTimeError)
	natservice.AddEndpoint("getCompressedResponse", getCompressedResponse)

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
			os.Exit(0)
		}
	}()

	runtime.Goexit()

	log.Printf("exiting...")
}

func getCompressedResponse(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {

	//read file contents into byte array

	byteArray, _ := os.ReadFile("/Users/manuelelaraj/tmp/disSSkdrill.dmg")

	//respBody := []byte("this is a very large body that will be compressed this is a very large body that will be compressed this is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedasdasthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressedthis is a very large body that will be compressed")
	msg.ResponseBody = byteArray
	msg.Logger.Printf("completed function ")
	return nil
}
