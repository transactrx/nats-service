package main

import (
	nats_service_client "github.com/transactrx/nats-service/pkg/nats-service-client"
	"log"
	"os"
	"time"
)

func main() {
	client, err := nats_service_client.NewClient()

	if err != nil {
		log.Panic(err)
	}

	//exampleWithOutExceptions(client)
	//
	//exampleWithExceptions(client)
	//
	//exampleAPIProducesError(client)
	exampleLargeBodyTestCompression(client)
}

func exampleWithOutExceptions(client *nats_service_client.Client) {
	rspMsg, svcErr, err := client.DoRequest("id13213", "rx.api.getTime/utc", nil, nil, time.Second*10)

	//	error connecting to service
	if err != nil {
		log.Printf("unable to make call to service: %v", err)
		return
	}

	//	service returned an error
	if svcErr != nil {
		log.Printf("service returned error: %v", svcErr)
		return
	}

	//	all is good - we got a good response - do what you got to do...
	log.Printf("data: %s", rspMsg.Data)
	log.Printf("header: %v", rspMsg.Header)
}

func exampleWithExceptions(client *nats_service_client.Client) {
	rspMsg, svcErr, err := client.DoRequest("id13213", "rx.api.DOES_NOT_EXIST", nil, nil, time.Second*10)

	//	error connecting to service
	if err != nil {
		log.Printf("unable to make call to service: %v", err)
		return
	}
	//	service returned an error
	if svcErr != nil {
		log.Printf("service returned error: %v", svcErr)
		return
	}
	asdasd
	//	NOTE: never gets here because we have an error
	log.Printf("data: %s", rspMsg.Data)
	log.Printf("header: %v", rspMsg.Header)
}

func exampleAPIProducesError(client *nats_service_client.Client) {
	rspMsg, svcErr, err := client.DoRequest("id13213", "rx.api.getTimeError", nil, nil, time.Second*10)

	//	error connecting to service
	if err != nil {
		log.Printf("unable to make call to service: %v", err)
		return
	}

	//	service returned an error
	if svcErr != nil {
		log.Printf("service returned error: %v", svcErr)
		return
	}

	//	NOTE: never gets here because we have an error
	log.Printf("data: %s", rspMsg.Data)
	log.Printf("header: %v", rspMsg.Header)
}

func exampleLargeBodyTestCompression(client *nats_service_client.Client) {

	rspMsg, svcErr, err := client.DoRequest("", "rx.api.getCompressedResponse", nil, nil, time.Second*10)

	//	error connecting to service
	if err != nil {
		log.Printf("unable to make call to service: %v", err)
		return
	}

	//	service returned an error
	if svcErr != nil {
		log.Printf("service returned error: %v", svcErr)
		return
	}

	//	all is good - we got a good response - do what you got to do...
	os.WriteFile("/Users/manuelelaraj/tmp/downloaded.txt", rspMsg.Data, 0644)
	log.Printf("header: %v", rspMsg.Header)
}
