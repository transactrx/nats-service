package nats_service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
	"log"
	"strconv"
	"time"
)

func (ns *NatService) startChunkResponder() error {

	subject := ns.basePath + "_chunked_" + uuid.New().String()
	subscribe, err := ns.nc.Subscribe(subject, ns.handleChunkDataRequest)
	if err != nil {
		return err
	}
	ns.chunkedSubscription = subscribe
	go ns.cleanCacheService()
	return nil
}

func (ns *NatService) cleanCacheService() {
	for true {
		ns.chunkCache.DeleteExpired()
		time.Sleep(45 * time.Second)
	}
}

// chunk request handler
func (ns *NatService) handleChunkDataRequest(chunkReq *nats.Msg) {

	responseMsg := nats.Msg{}
	chunksId, chunkIndex, logger, err := ns.validateAndGetChunkRequestInfo(chunkReq)
	if err != nil {
		log.Printf("Unable to validate chunk request: %s", err)
		return
	}

	logger.Printf("handleChunkDataRequest: chunksId: %s, chunkIndex: %d", chunksId, chunkIndex)

	responseMsg.Data = ns.chunkCache.Get(chunksId).Value()[chunkIndex]
	err = chunkReq.RespondMsg(&responseMsg)
	if err != nil {
		logger.Printf("handleChunkDataRequest: error sending response: %s", err)
	}

}

// validation functions, They respond to the request if there is an error
func validateChunkStringHeader(req *nats.Msg, headerName string, errStatus int) (string, error) {
	header := req.Header.Get(headerName)
	if header == "" {

		natsErr := NatsServiceError{
			Status:        errStatus,
			ErrorMessage:  "missing header: " + headerName,
			ApiStatusCode: errStatus,
			InternalErr:   "400",
		}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			log.Printf("Unable to marshal error: %s", err)
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set("status", "400")
		err = req.RespondMsg(&respMsg)

		return "", fmt.Errorf("missing header: %s", headerName)
	}
	return header, nil
}
func validateChunkIntHeader(req *nats.Msg, headerName string, errStatus int) (int, error) {
	header := req.Header.Get(headerName)
	if header == "" {

		natsErr := NatsServiceError{
			Status:        errStatus,
			ErrorMessage:  "missing header: " + headerName,
			ApiStatusCode: errStatus,
			InternalErr:   "400",
		}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			log.Printf("Unable to marshal error: %s", err)
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set("status", "400")
		err = req.RespondMsg(&respMsg)

		return -1, fmt.Errorf("missing header: %s", headerName)
	}
	intHeader, err := strconv.Atoi(header)
	if err != nil {
		natsErr := NatsServiceError{
			Status:        errStatus,
			ErrorMessage:  "missing header: " + headerName,
			ApiStatusCode: errStatus,
			InternalErr:   "400",
		}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			log.Printf("Unable to marshal error: %s", err)
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set("status", "400")
		err = req.RespondMsg(&respMsg)

		return -1, fmt.Errorf("header: %s must be an integer", headerName)
	}
	return intHeader, nil
}
func (ns *NatService) validateAndGetChunkRequestInfo(chunkReq *nats.Msg) (string, int, *log.Logger, error) {
	//check if header is not null
	if chunkReq.Header == nil {
		natsErr := NatsServiceError{Status: 400, ErrorMessage: "missing header", ApiStatusCode: 400}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			return "", -1, nil, err
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set("status", "400")
		err = chunkReq.RespondMsg(&respMsg)
		if err != nil {
			return "", -1, nil, err
		}
		return "", -1, nil, errors.New("missing header")
	}
	chunksId, err := validateChunkStringHeader(chunkReq, nats_service_common.CHUNKS_ID, 400)
	if err != nil {
		return "", -1, nil, err
	}
	chunkIndex, err := validateChunkIntHeader(chunkReq, nats_service_common.CHUNK_INDEX, 400)
	if err != nil {
		return "", -1, nil, err
	}

	chunks := ns.chunkCache.Get(chunksId)
	if chunks == nil {
		natsErr := NatsServiceError{Status: 400, ErrorMessage: fmt.Sprintf("chunksId %s is not in memory", chunkReq.Header.Get(nats_service_common.CHUNKS_ID)), ApiStatusCode: 400}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			return "", -1, nil, err
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set("status", "400")
		err = chunkReq.RespondMsg(&respMsg)
		if err != nil {
			return "", -1, nil, err
		}
		return "", -1, nil, errors.New("CHUNKS_ID not in cache")
	}

	if chunkIndex > len(chunks.Value()) {
		natsErr := NatsServiceError{Status: 400, ErrorMessage: fmt.Sprintf("chunkIndex %d is out of range", chunkIndex), ApiStatusCode: 400}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			return "", -1, nil, err
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set("status", "400")
		err = chunkReq.RespondMsg(&respMsg)
		if err != nil {
			return "", -1, nil, err
		}
		return "", -1, nil, errors.New("CHUNK_INDEX out of range")
	}
	//chunksId, chunkIndex, logger, err
	msgId := chunkReq.Header.Get(MESSAGE_ID)
	if msgId == "" {
		msgId = uuid.New().String()
	}

	return chunkReq.Header.Get(nats_service_common.CHUNKS_ID), chunkIndex, createLogger(msgId), nil

}
