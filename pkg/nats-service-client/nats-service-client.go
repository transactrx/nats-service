package nats_service_client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
)

type Client struct {
	nc                    *nats.Conn
	maxSizeBeforeCompress int
	maxSizeBeforeChunk    int
	debug                 bool
}

type NatsResponseMessage struct {
	Data   []byte
	Header Header
}

var DefaultMaxSizeBeforeCompress int = 1024 * 2
var DefaultMaxSizeBeforeChunk int = 1024 * 8

func NewClient() (*Client, error) {
	natsUrl := os.Getenv("NATS_URL")
	natsToken := os.Getenv("NATS_JWT")
	natsKey := os.Getenv("NATS_KEY")
	natsDebug := os.Getenv("NATS_DEBUG")
	MAX_SIZE_BEFORE_COMPRESS := os.Getenv("MAX_SIZE_BEFORE_COMPRESS")
	MAX_SIZE_BEFORE_CHUNK := os.Getenv("MAX_SIZE_BEFORE_CHUNK")
	maxSizeBeforeCompress := DefaultMaxSizeBeforeCompress
	maxSizeBeforeChunk := DefaultMaxSizeBeforeChunk
	if MAX_SIZE_BEFORE_COMPRESS != "" {
		maxSizeBeforeCompress, _ = strconv.Atoi(MAX_SIZE_BEFORE_COMPRESS)
	}
	if MAX_SIZE_BEFORE_CHUNK != "" {
		maxSizeBeforeChunk, _ = strconv.Atoi(MAX_SIZE_BEFORE_CHUNK)
	}

	if natsUrl == "" {
		return nil, fmt.Errorf("environment variable NATS_URL is missing: %w", nats_service.ConfigError)
	}

	// These are only required when using authentication.
	// if natsToken == "" {
	// 	return nil, fmt.Errorf("environment variable NATS_JWT is missing: %w", nats_service.ConfigError)
	// }

	// if natsKey == "" {
	// 	return nil, fmt.Errorf("environment variable NATS_KEY is missing: %w", nats_service.ConfigError)
	// }

	debug, _ := strconv.ParseBool(natsDebug)

	return NewLowLevelClientWithChunkingAndCompressionDebug(natsUrl, maxSizeBeforeCompress, maxSizeBeforeChunk, natsToken, natsKey, debug)
}

func NewLowLevelClient(natsUrl string, natsToken, natsKey string) (*Client, error) {

	return NewLowLevelClientWithChunkingAndCompression(natsUrl, DefaultMaxSizeBeforeCompress, DefaultMaxSizeBeforeChunk, natsToken, natsKey)

}

func NewLowLevelClientWithChunkingAndCompression(natsUrl string, maxSizeBeforeCompress, maxSizeBeforeChunk int, natsToken, natsKey string) (*Client, error) {

	natsDebug := os.Getenv("NATS_DEBUG")
	debug, _ := strconv.ParseBool(natsDebug)

	return NewLowLevelClientWithChunkingAndCompressionDebug(natsUrl, maxSizeBeforeCompress, maxSizeBeforeChunk, natsToken, natsKey, debug)

}

func NewLowLevelClientWithChunkingAndCompressionDebug(natsUrl string, maxSizeBeforeCompress, maxSizeBeforeChunk int, natsToken, natsKey string, debug bool) (*Client, error) {
	var opts []nats.Option
	if (natsToken != "" && natsKey != "") {
		// Set up with authentication
		opts = []nats.Option{nats.UserJWTAndSeed(natsToken, natsKey)}
	}
	opts = setupConnOptions(opts)
	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		log.Printf("%s Connect failed error: %s", time.Now(), err)
		return nil, err
	}
	log.Printf("%s Connect CONNECTED to %s SUCCESS ", time.Now(), natsUrl)

	client := Client{
		nc:                    nc,
		maxSizeBeforeChunk:    maxSizeBeforeChunk,
		maxSizeBeforeCompress: maxSizeBeforeCompress,
		debug:                 debug,
	}

	return &client, nil
}

func (cl *Client) DoRequest(correlationId, subject string, header Header, data []byte, timeout time.Duration) (*NatsResponseMessage, *nats_service.NatsServiceError, error) {
	requestMsg := nats.Msg{}

	if strings.Trim(correlationId, " ") == "" {
		correlationId = uuid.New().String()
	}
	if cl.debug {
		log.Printf("corrolationId: %s", correlationId)
	}

	requestMsg.Header = nats.Header{}
	requestMsg.Header.Set(nats_service_common.MESSAGE_ID, correlationId)
	requestMsg.Header.Set(nats_service_common.USER_ID, cl.nc.Opts.User)

	if len(data) > cl.maxSizeBeforeCompress {
		data = nats_service_common.GZipBytes(data)
		requestMsg.Header.Set(nats_service_common.COMPRESSED_HEADER, nats_service_common.GZIP_COMPRESSION_TYPE)
	}

	if header != nil && len(header) > 0 {
		for key, values := range header {
			for _, value := range values {
				requestMsg.Header.Add(key, value)
			}
		}
	}

	logger := log.New(os.Stdout, correlationId+" - ", log.Ltime|log.Ldate|log.Lshortfile|log.Lmsgprefix)
	startTime := time.Now().UnixMicro()
	var reqErr error
	var msg *nats.Msg

	requestMsg.Subject = subject
	requestMsg.Data = data
	if cl.debug {
		logger.Printf("Sending request to %s", subject)
	}

	msg, reqErr = cl.nc.RequestMsg(&requestMsg, timeout)

	var natsError *nats_service.NatsServiceError
	var natsResultMsg *NatsResponseMessage

	if reqErr == nil {
		var status string
		if msg.Header != nil {
			status = msg.Header.Get(nats_service_common.STATUS)
		}

		if status == "200" {
			//	success

			respData, decompressionError := cl.decompressIfCompressionUsed(msg, reqErr, logger)
			if decompressionError != nil {
				reqErr = fmt.Errorf("invalid response from service:%w", decompressionError)
				logger.Printf("Request failed in %dμs with error: %v", time.Now().UnixMicro()-startTime, reqErr)
			} else {

				natsResultMsg = &NatsResponseMessage{
					Data:   respData,
					Header: convertNatsHeaderToHeader(msg.Header),
				}
				if cl.debug {
					logger.Printf("Request successfully completed in %dμs", time.Now().UnixMicro()-startTime)
				}
			}

		} else {
			//	error condition
			natsError = &nats_service.NatsServiceError{}
			parsError := json.Unmarshal(msg.Data, natsError)
			if parsError != nil {
				reqErr = fmt.Errorf("invalid response from service: %s, %w", msg.Data, parsError)
			}
			logger.Printf("Request failed in %dμs with error: %v", time.Now().UnixMicro()-startTime, natsError)
		}
	} else {
		logger.Printf("Request failed to start in %dμs with error: %v", time.Now().UnixMicro()-startTime, reqErr)
	}

	return natsResultMsg, natsError, reqErr
}

func (cl *Client) decompressIfCompressionUsed(msg *nats.Msg, err error, logger *log.Logger) ([]byte, error) {
	var respData []byte
	var rawData []byte

	//check if chunked
	if msg.Header.Get(nats_service_common.CHUNKED_SUBJECT) != "" {
		//data is chunked.
		chunkSubject := msg.Header.Get(nats_service_common.CHUNKED_SUBJECT)
		chunksId := msg.Header.Get(nats_service_common.CHUNKS_ID)
		chunksCount := msg.Header.Get(nats_service_common.CHUNKED_LENGTH)
		msgId := msg.Header.Get(nats_service_common.MESSAGE_ID)
		//ChunkLength
		//ChunkId

		rawData, err = cl.downloadChunks(chunkSubject, msgId, cl.nc.Opts.User, chunksId, chunksCount, logger)
		if err != nil {
			return nil, err
		}

	} else {
		rawData = msg.Data
	}

	if msg.Header.Get(nats_service_common.COMPRESSED_HEADER) == nats_service_common.GZIP_COMPRESSION_TYPE {
		respData, err = nats_service_common.GUnzipBytes(rawData)
		if err != nil {
			logger.Printf("Error decompressing response: %s", err)
			return nil, err
		}
	} else {
		respData = msg.Data
	}
	return respData, nil
}
func (cl *Client) GetNatsClient() *nats.Conn {
	return cl.nc
}
func (cl *Client) downloadChunks(subject, messageId, userId, chunksId, count string, logger *log.Logger) ([]byte, error) {

	chunksCount, err := strconv.Atoi(count)
	if err != nil {
		logger.Printf("Error parsing chunks count: %s", err)
		return nil, err
	}

	buff := bytes.NewBuffer(nil)

	for i := 0; i < chunksCount; i++ {

		request := nats.Msg{
			Subject: subject,
			Header:  nats.Header{},
		}

		request.Header.Set(nats_service_common.CHUNKS_ID, chunksId)
		request.Header.Set(nats_service_common.CHUNK_INDEX, strconv.Itoa(i))
		request.Header.Set(nats_service_common.MESSAGE_ID, messageId)
		request.Header.Set(nats_service_common.USER_ID, userId)

		msg, err := cl.nc.RequestMsg(&request, 30*time.Second)
		if err != nil {
			logger.Printf("Error downloading chunk: %s", err)
			return nil, err
		}

		if msg.Header.Get(nats_service_common.STATUS) == "200" {
			buff.Write(msg.Data)
		} else {
			natsError := &nats_service.NatsServiceError{}
			parsError := json.Unmarshal(msg.Data, natsError)
			if parsError != nil {
				err = fmt.Errorf("invalid response from service: %s, %w", msg.Data, parsError)
				return nil, err
			}

			err = fmt.Errorf("unable to retrieve chunk, error: %v", natsError)
			return nil, err
		}
	}

	return buff.Bytes(), nil

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
