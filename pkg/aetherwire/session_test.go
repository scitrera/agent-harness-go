package aetherwire

import (
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestSessionFramePayloadRoundTrip(t *testing.T) {
	request := spec.NewSessionAttachRequest("session-1", "client-1")
	frame, err := spec.NewSessionFrame(spec.SessionFrameAttachRequest, "attach-1", request)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := SessionFramePayload(frame)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok, err := ParseSessionFrame(payload)
	if err != nil || !ok {
		t.Fatalf("ParseSessionFrame() = %+v, %v, %v", decoded, ok, err)
	}
	requestPayload, err := decoded.DecodeAttachRequest()
	if err != nil || requestPayload == nil || requestPayload.ClientID != "client-1" {
		t.Fatalf("request payload = %+v, %v", requestPayload, err)
	}
}

func TestParseSessionFrameDistinguishesOtherAetherPayloads(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`not json`),
		[]byte(`{"id":"m1","role":"user","content":[]}`),
		[]byte(`{"options":{"chat_stream_event":{"event":"token_delta"}}}`),
	} {
		if _, ok, err := ParseSessionFrame(payload); err != nil || ok {
			t.Fatalf("ParseSessionFrame(%s) = ok %v, err %v", payload, ok, err)
		}
	}
}

func TestParseSessionFrameRecognizesMalformedFrame(t *testing.T) {
	payload := []byte(`{"protocol_version":1,"schema_revision":2,"type":"attach_request","payload":{}}`)
	frame, ok, err := ParseSessionFrame(payload)
	if !ok || err == nil {
		t.Fatalf("ParseSessionFrame() = %+v, %v, %v", frame, ok, err)
	}
}
