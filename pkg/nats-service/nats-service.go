package nats_service

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"regexp"
	"time"
)

type NatService struct {
	url          string
	nc           *nats.Conn
	subscription *nats.Subscription
	endPoints    []*NatsEndpoint
	basePath     string
	queueName    string
}

type NatsMessage struct {
	Body           []byte
	Header         nats.Header
	Path           string
	ResponseBody   []byte
	ResponseHeader nats.Header
	Logger         *log.Logger
}

type NatsEndpoint struct {
	path         string
	endPointFunc NatsEndpointFunc
	regex        *regexp.Regexp
}

type NatsEndpointFunc func(msg *NatsMessage) error

var ConfigError = errors.New("configuration error")

func New(basePath string) (*NatService, error) {
	natsUrl := os.Getenv("NATS_URL")
	natsToken := os.Getenv("NATS_JWT")
	natsKey := os.Getenv("NATS_KEY")
	natsQueueName := os.Getenv("NATS_QUEUE_NAME")

	if natsUrl == "" {
		return nil, fmt.Errorf("environment variable NATS_URL is missing: %w", ConfigError)
	}

	if natsToken == "" {
		return nil, fmt.Errorf("environment variable NATS_JWT is missing: %w", ConfigError)
	}

	if natsKey == "" {
		return nil, fmt.Errorf("environment variable NATS_KEY is missing: %w", ConfigError)
	}

	if natsQueueName == "" {
		return nil, fmt.Errorf("environment variable NATS_QUEUE_NAME is missing: %w", ConfigError)
	}

	return NewLowLevel(basePath, natsQueueName, natsUrl, natsToken, natsKey)
}

func NewLowLevel(basePath, natsQueueName, natsUrl, natsToken, natsKey string) (*NatService, error) {

	var opts []nats.Option
	opts = []nats.Option{nats.UserJWTAndSeed(natsToken, natsKey)}
	opts = SetupConnOptions(opts)
	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		log.Printf("%s Connect failed error: %s", err)
		return nil, err
	}
	log.Printf("%s Connect CONNECTED to %s SUCCESS ", time.Now(), natsUrl)

	ns := NatService{
		url:       natsUrl,
		nc:        nc,
		basePath:  basePath,
		queueName: natsQueueName,
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

	fullPath := ns.basePath + "." + path

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
				go handleEndpointCall(endPoint, msg)
				return
			}
		}

		handleEndpointNotFound(msg)
	})

	if err != nil {
		return err
	}

	ns.subscription = subscribe

	return nil
}

func handleEndpointCall(endPoint *NatsEndpoint, msg *nats.Msg) {

	startTime := time.Now().UnixMicro()

	natsMessage := NatsMessage{
		Body:   msg.Data,
		Header: msg.Header,
		Path:   msg.Subject,
		Logger: log.New(os.Stdout, uuid.New().String()+" - ", log.Ltime|log.Ldate|log.Lshortfile|log.Lmsgprefix),
	}

	err := endPoint.endPointFunc(&natsMessage)

	endTime := time.Now().UnixMicro()

	elapsedTime := endTime - startTime

	responseMsg := nats.Msg{}

	var status string

	if err != nil {
		status = "500"
		responseMsg.Header = nats.Header{}

		responseMsg.Header.Set("status", status)
		responseMsg.Data = []byte(fmt.Sprintf("oops: %v", err))
	} else {
		status = "200"

		if natsMessage.ResponseHeader != nil {
			responseMsg.Header = natsMessage.ResponseHeader
		} else {
			responseMsg.Header = nats.Header{}
		}

		responseMsg.Data = natsMessage.ResponseBody
		responseMsg.Header.Set("status", status)
	}

	err = msg.RespondMsg(&responseMsg)
	if err != nil {
		natsMessage.Logger.Printf("error returning response: %v", err)
	}

	natsMessage.Logger.Printf("api call completed: %s, elapsed time: %d, subject: %s", status, elapsedTime, msg.Subject)
}

func handleEndpointNotFound(msg *nats.Msg) {
	responseMsg := nats.Msg{}
	responseMsg.Header = nats.Header{}

	responseMsg.Header.Set("status", "404")
	responseMsg.Data = []byte(fmt.Sprintf("endpoint not found"))
	msg.RespondMsg(&responseMsg)
}

func (ns *NatService) Shutdown() error {
	return ns.subscription.Drain()
}

func SetupConnOptions(opts []nats.Option) []nats.Option {
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
