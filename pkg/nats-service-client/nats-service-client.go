package nats_service_client

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	"log"
	"os"
	"strings"
	"time"
)

const MESSAGE_ID = "MESSAGE_ID"

type Client struct {
	nc *nats.Conn
}

type NatsResponseMessage struct {
	Data   []byte
	Header Header
}

func NewClient() (*Client, error) {
	natsUrl := os.Getenv("NATS_URL")
	natsToken := os.Getenv("NATS_JWT")
	natsKey := os.Getenv("NATS_KEY")

	if natsUrl == "" {
		return nil, fmt.Errorf("environment variable NATS_URL is missing: %w", nats_service.ConfigError)
	}

	if natsToken == "" {
		return nil, fmt.Errorf("environment variable NATS_JWT is missing: %w", nats_service.ConfigError)
	}

	if natsKey == "" {
		return nil, fmt.Errorf("environment variable NATS_KEY is missing: %w", nats_service.ConfigError)
	}

	return NewLowLevelClient(natsUrl, natsToken, natsKey)
}

func NewLowLevelClient(natsUrl, natsToken, natsKey string) (*Client, error) {

	var opts []nats.Option
	opts = []nats.Option{nats.UserJWTAndSeed(natsToken, natsKey)}
	opts = setupConnOptions(opts)
	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		log.Printf("%s Connect failed error: %s", time.Now(), err)
		return nil, err
	}
	log.Printf("%s Connect CONNECTED to %s SUCCESS ", time.Now(), natsUrl)

	client := Client{
		nc: nc,
	}

	return &client, nil
}

func (cl *Client) DoRequest(correlationId, subject string, header Header, data []byte, timeout time.Duration) (*NatsResponseMessage, *nats_service.NatsServiceError, error) {
	requestMsg := nats.Msg{}

	if strings.Trim(correlationId, " ") == "" {
		correlationId = uuid.New().String()
	}
	log.Printf("corrolationId: %s", correlationId)

	requestMsg.Header = nats.Header{}
	requestMsg.Header.Set(MESSAGE_ID, correlationId)

	if header != nil && len(header) > 0 {
		for key, values := range header {
			for _, value := range values {
				requestMsg.Header.Add(key, value)
			}
		}
	}

	logger := log.New(os.Stdout, correlationId+" - ", log.Ltime|log.Ldate|log.Lshortfile|log.Lmsgprefix)

	requestMsg.Subject = subject
	requestMsg.Data = data

	var natsError *nats_service.NatsServiceError
	var natsResultMsg *NatsResponseMessage

	logger.Printf("Sending request to %s", subject)
	startTime := time.Now().UnixMicro()
	msg, err := cl.nc.RequestMsg(&requestMsg, timeout)
	if err == nil {
		var status string
		if msg.Header != nil {
			status = msg.Header.Get("status")
		}

		if status == "200" {
			//	success
			natsResultMsg = &NatsResponseMessage{
				Data:   msg.Data,
				Header: convertNatsHeaderToHeader(msg.Header),
			}
			logger.Printf("Request successfully completed in %dμs", time.Now().UnixMicro()-startTime)
		} else {
			//	error condition
			natsError = &nats_service.NatsServiceError{}
			parsError := json.Unmarshal(msg.Data, natsError)
			if parsError != nil {
				err = fmt.Errorf("invalid response from service: %s, %w", msg.Data, parsError)
			}
			logger.Printf("Request failed in %dμs with error: %v", time.Now().UnixMicro()-startTime, natsError)
		}
	} else {
		logger.Printf("Request failed to start in %dμs with error: %v", time.Now().UnixMicro()-startTime, err)
	}

	return natsResultMsg, natsError, err
}

func convertNatsHeaderToHeader(header nats.Header) Header {

	if header != nil && len(header) > 0 {
		clientHeader := Header{}
		for key, values := range header {
			for _, value := range values {
				clientHeader.Add(key, value)
			}
		}

		return clientHeader
	}

	return nil
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
