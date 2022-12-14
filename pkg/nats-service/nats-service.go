package nats_service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"
)

type NatService struct {
	url                   string
	nc                    *nats.Conn
	subscription          *nats.Subscription
	chunkedSubscription   *nats.Subscription
	chunkCache            *ttlcache.Cache[string, [][]byte]
	endPoints             []*NatsEndpoint
	basePath              string
	queueName             string
	maxRespSizeToCompress int
	maxRespSizeToChunk    int
}

type NatsMessage struct {
	Body           []byte
	Header         nats.Header
	Path           string
	ResponseBody   []byte
	MessageId      string
	UserId         string
	ResponseHeader nats.Header
	Logger         *log.Logger
}

type NatsEndpoint struct {
	path         string
	endPointFunc NatsEndpointFunc
	regex        *regexp.Regexp
}

type NatsEndpointFunc func(msg *NatsMessage) *NatsServiceError

var ConfigError = errors.New("configuration error")

func New(basePath string) (*NatService, error) {
	natsUrl := getEnvironmentVariableOrPanic("NATS_URL")
	natsToken := getEnvironmentVariableOrPanic("NATS_JWT")
	natsKey := getEnvironmentVariableOrPanic("NATS_KEY")
	natsQueueName := getEnvironmentVariableOrPanic("NATS_QUEUE_NAME")

	return NewLowLevel(basePath, natsQueueName, natsUrl, natsToken, natsKey, 1024*2, 1024*300)
}

func NewLowLevel(basePath, natsQueueName, natsUrl, natsToken, natsKey string, maxRespSizeToCompress, maxRespSizeToChunk int) (*NatService, error) {

	var opts []nats.Option
	opts = []nats.Option{nats.UserJWTAndSeed(natsToken, natsKey)}
	opts = setupConnOptions(opts)
	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		log.Printf("%s Connect failed error: %s", time.Now(), err)
		return nil, err
	}
	log.Printf("%s Connect CONNECTED to %s SUCCESS ", time.Now(), natsUrl)

	ns := NatService{
		url:                   natsUrl,
		nc:                    nc,
		basePath:              basePath,
		queueName:             natsQueueName,
		maxRespSizeToCompress: maxRespSizeToCompress,
		maxRespSizeToChunk:    maxRespSizeToChunk,
		chunkCache:            ttlcache.New[string, [][]byte](),
	}

	return &ns, nil
}

func (ns *NatService) AddEndpoint(path string, endPoint NatsEndpointFunc) error {

	for _, endPoint := range ns.endPoints {
		if endPoint.path == path {
			return fmt.Errorf("endpoint already in use: %w", ConfigError)
		}
	}

	if endPoint == nil {
		return fmt.Errorf("endpoint cannot be nil: %w", ConfigError)
	}

	fullPath := "^" + ns.basePath + "." + path + "$"

	reg, err := regexp.Compile(fullPath)
	if err != nil {
		return fmt.Errorf("invalid regex expression: %w", err)
	}

	natsEndpoint := NatsEndpoint{
		path:         path,
		endPointFunc: endPoint,
		regex:        reg,
	}

	ns.endPoints = append(ns.endPoints, &natsEndpoint)

	return nil
}

func (ns *NatService) Start() error {

	if len(ns.endPoints) == 0 {
		return fmt.Errorf("no endpoints configured")
	}

	subscribe, err := ns.nc.QueueSubscribe(ns.basePath+".>", ns.queueName, func(msg *nats.Msg) {
		for _, endPoint := range ns.endPoints {
			if endPoint.regex.MatchString(msg.Subject) {
				go ns.handleEndpointCall(endPoint, msg)
				return
			}
		}

		handleEndpointNotFound(msg)
	})

	if err != nil {
		return err
	}

	ns.subscription = subscribe

	err = ns.startChunkResponder()

	return err
}

func (ns *NatService) handleEndpointCall(endPoint *NatsEndpoint, msg *nats.Msg) {

	startTime := time.Now().UnixMicro()

	natsMessage := createNatsMessageFromRequest(msg)

	err := endPoint.endPointFunc(&natsMessage)
	endTime := time.Now().UnixMicro()
	elapsedTime := endTime - startTime

	responseMsg := nats.Msg{}

	var status string
	var responseMsgLog []byte

	if err != nil {
		status = fmt.Sprintf("%d", err.Status)
		responseMsg.Header = nats.Header{}
		responseMsg.Header.Set("status", status)
		responseMsg.Header.Set(nats_service_common.MESSAGE_ID, natsMessage.MessageId)

		jsonBA, jsonError := json.Marshal(err)
		if jsonError != nil {
			errorMsg := fmt.Sprintf("error creating json from response: %v", jsonError)
			natsMessage.Logger.Print(errorMsg)
			responseMsg.Data = []byte(errorMsg)
		} else {
			responseMsg.Data = jsonBA
		}

		if len(responseMsg.Data) <= 1024 {
			responseMsgLog = responseMsg.Data
		} else {
			responseMsgLog = responseMsg.Data[:1024]
		}
	} else {
		status = "200"
		if natsMessage.ResponseHeader != nil {
			responseMsg.Header = natsMessage.ResponseHeader
		} else {
			responseMsg.Header = nats.Header{}
		}

		//capture data for logging
		if len(natsMessage.ResponseBody) <= 1024 {
			responseMsgLog = natsMessage.ResponseBody
		} else {
			responseMsgLog = natsMessage.ResponseBody[:1024]
		}

		if len(natsMessage.ResponseBody) > ns.maxRespSizeToCompress {
			natsMessage.Logger.Printf("response size %d is bigger than max size to compress %d,  compressing", len(natsMessage.ResponseBody), ns.maxRespSizeToCompress)
			responseMsg.Header.Set(nats_service_common.COMPRESSED_HEADER, nats_service_common.GZIP_COMPRESSION_TYPE)
			natsMessage.ResponseBody = nats_service_common.GZipBytes(natsMessage.ResponseBody)
			natsMessage.Logger.Printf("response size after compression %d", len(natsMessage.ResponseBody))
		}
		responseMsg.Data = natsMessage.ResponseBody
		responseMsg.Header.Set("status", status)
		responseMsg.Header.Set(nats_service_common.MESSAGE_ID, natsMessage.MessageId)
	}

	var reqMsgLog []byte
	if len(msg.Data) > 1024 {
		reqMsgLog = msg.Data[:1024]
	} else {
		reqMsgLog = msg.Data
	}

	errResponding := ns.respondToRequest(msg, &responseMsg)
	if errResponding != nil {
		natsMessage.Logger.Printf("error returning response: %v", errResponding)
	}

	natsMessage.Logger.Printf("apiStatus: %s, user:%s latency: %dμs, sub: %s, req:%s, resp: %s", status, natsMessage.UserId, elapsedTime, msg.Subject, reqMsgLog, responseMsgLog)
}

func createNatsMessageFromRequest(msg *nats.Msg) NatsMessage {
	var messageId string
	var userId string
	if msg.Header != nil && msg.Header.Get(nats_service_common.MESSAGE_ID) != "" {
		messageId = msg.Header.Get(nats_service_common.MESSAGE_ID)
		userId = msg.Header.Get(nats_service_common.USER_ID)
	} else {
		userId = ""
		messageId = uuid.New().String()
	}

	natsMessage := NatsMessage{
		Body:      msg.Data,
		Header:    msg.Header,
		Path:      msg.Subject,
		MessageId: messageId,
		UserId:    userId,
		Logger:    createLogger(messageId),
	}
	return natsMessage
}

func createLogger(messageId string) *log.Logger {
	return log.New(os.Stdout, messageId+" - ", log.Ltime|log.Ldate|log.Lshortfile|log.Lmsgprefix)
}

func handleEndpointNotFound(msg *nats.Msg) {
	responseMsg := nats.Msg{}
	responseMsg.Header = nats.Header{}

	responseMsg.Header.Set("status", "404")
	notFoundError := NewEndpointNotFoundError(msg.Subject)

	errorText, err := json.Marshal(notFoundError)

	if err != nil {
		errorText = []byte("unable to marshall EndpointNotFoundError")
		log.Printf("%s", errorText)
	}

	responseMsg.Data = errorText
	msg.RespondMsg(&responseMsg)
}

func (ns *NatService) Shutdown() error {
	return ns.subscription.Drain()
}

func (ns *NatService) respondToRequest(req *nats.Msg, responseMsg *nats.Msg) error {

	//if it is small enough, then we send it back
	if len(responseMsg.Data) < ns.maxRespSizeToChunk {
		return req.RespondMsg(responseMsg)
	}

	//the data is too large, so we are going to chunk it
	chunkedData := nats_service_common.ChunkByteArray(responseMsg.Data, ns.maxRespSizeToChunk)

	responseMsg.Header.Set(nats_service_common.CHUNKED_SUBJECT, ns.chunkedSubscription.Subject)
	responseMsg.Header.Set(nats_service_common.CHUNKED_LENGTH, strconv.Itoa(len(chunkedData)))

	chunksId := uuid.New().String()
	responseMsg.Header.Set(nats_service_common.CHUNKS_ID, chunksId)
	ns.chunkCache.Set(chunksId, chunkedData, time.Minute*3)
	responseMsg.Data = []byte("")
	return req.RespondMsg(responseMsg)
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 15 * time.Minute
	reconnectDelay := time.Second
	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("%s nats.DisconnectErrHandler disconnected due to: %s, will attempt reconnects for %.0fm", time.Now(), err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("%s nats.ReconnectHandler reconnected [%s]", time.Now(), nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Printf("%s nats.ClosedHandlerExiting: %v", time.Now(), nc.LastError())
		os.Exit(-1)
	}))

	return opts
}

func getEnvironmentVariableOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Panicf("Environment variable %s is missing", key)
	}
	return value
}
