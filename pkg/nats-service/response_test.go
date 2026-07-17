package nats_service

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestRespondToRequestSkipsFireAndForgetMessage(t *testing.T) {
	service := &NatService{maxRespSizeToChunk: 1024}
	request := &nats.Msg{}
	response := &nats.Msg{Header: nats.Header{}, Data: []byte("processed")}

	if err := service.respondToRequest(request, response); err != nil {
		t.Fatalf("fire-and-forget message should not attempt a reply: %v", err)
	}
}

func TestRespondToRequestPreservesRequestReplyPath(t *testing.T) {
	service := &NatService{maxRespSizeToChunk: 1024}
	request := &nats.Msg{Reply: "_INBOX.requester"}
	response := &nats.Msg{Header: nats.Header{}, Data: []byte("processed")}

	// The synthetic message is intentionally not bound to a subscription. A
	// bound-message error proves a reply subject still follows the existing
	// RespondMsg path instead of being treated as fire-and-forget.
	err := service.respondToRequest(request, response)
	if !errors.Is(err, nats.ErrMsgNotBound) {
		t.Fatalf("request/reply message should attempt RespondMsg; got %v", err)
	}
}
