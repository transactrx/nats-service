package main

import (
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {

	natservice, err := nats_service.New("rx.api")

	if err != nil {
		log.Panicln(err)
	}

	// TODO: The original example had path parameters for getTime.
	// The new shared handler nats_service.GetTime does not currently support them.
	// For now, we'll register it without path parameters. This might need adjustment
	// if path parameters are essential for this example.
	err = natservice.AddEndpoint("getTime", nats_service.GetTime)
	if err != nil {
		log.Panicln(err)
	}
	natservice.AddEndpoint("getTimeError", nats_service.GetTimeError)
	natservice.AddEndpoint("getCompressedResponse", nats_service.GetCompressedResponse)

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
