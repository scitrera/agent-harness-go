package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

// TestCodecRoundTrip exercises request read plus response/notification/error
// writes over an in-memory buffer, asserting the newline-delimited JSON framing
// and field wiring.
func TestCodecRoundTrip(t *testing.T) {
	// read: a request line parses into method/id/params.
	in := `{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"s1"}}` + "\n"
	rc := newConn(strings.NewReader(in), &bytes.Buffer{})
	req, err := rc.read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if req.Method != "session/prompt" {
		t.Fatalf("method = %q, want session/prompt", req.Method)
	}
	if string(req.ID) != "7" {
		t.Fatalf("id = %q, want 7", req.ID)
	}
	if req.isNotification() {
		t.Fatal("request with id must not be a notification")
	}
	var pp promptParams
	if err := json.Unmarshal(req.Params, &pp); err != nil {
		t.Fatalf("params: %v", err)
	}
	if pp.SessionID != "s1" {
		t.Fatalf("sessionId = %q, want s1", pp.SessionID)
	}

	// read: a message without id is a notification.
	notifIn := `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s1"}}` + "\n"
	nc := newConn(strings.NewReader(notifIn), &bytes.Buffer{})
	nreq, err := nc.read()
	if err != nil {
		t.Fatalf("read notif: %v", err)
	}
	if !nreq.isNotification() {
		t.Fatal("message without id must be a notification")
	}

	// write: response, notification, error each emit exactly one JSON line.
	var buf bytes.Buffer
	wc := newConn(strings.NewReader(""), &buf)
	if err := wc.writeResponse(json.RawMessage("7"), promptResult{StopReason: stopEndTurn}); err != nil {
		t.Fatalf("writeResponse: %v", err)
	}
	if err := wc.notify(methodSessionUpdate, sessionNotification{SessionID: "s1", Update: json.RawMessage(`{"sessionUpdate":"agent_message_chunk"}`)}); err != nil {
		t.Fatalf("notify: %v", err)
	}
	if err := wc.writeError(json.RawMessage("8"), codeMethodNotFound, "nope"); err != nil {
		t.Fatalf("writeError: %v", err)
	}

	lines := splitLines(t, buf.Bytes())
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf.String())
	}

	var resp rpcResponse
	if err := json.Unmarshal(lines[0], &resp); err != nil {
		t.Fatalf("resp decode: %v", err)
	}
	if resp.JSONRPC != jsonRPCVersion || string(resp.ID) != "7" {
		t.Fatalf("resp id/version wrong: %+v", resp)
	}
	var pr promptResult
	if err := json.Unmarshal(resp.Result, &pr); err != nil || pr.StopReason != stopEndTurn {
		t.Fatalf("resp result = %+v (%v)", pr, err)
	}

	var notif rpcNotification
	if err := json.Unmarshal(lines[1], &notif); err != nil {
		t.Fatalf("notif decode: %v", err)
	}
	if notif.Method != methodSessionUpdate {
		t.Fatalf("notification method = %q, want %q", notif.Method, methodSessionUpdate)
	}
	// A notification MUST NOT carry an id member on the wire.
	var idProbe map[string]json.RawMessage
	if err := json.Unmarshal(lines[1], &idProbe); err != nil {
		t.Fatalf("notif probe: %v", err)
	}
	if _, hasID := idProbe["id"]; hasID {
		t.Fatalf("notification carried an id: %s", lines[1])
	}

	var eresp rpcResponse
	if err := json.Unmarshal(lines[2], &eresp); err != nil {
		t.Fatalf("err decode: %v", err)
	}
	if eresp.Error == nil || eresp.Error.Code != codeMethodNotFound {
		t.Fatalf("error response wrong: %s", lines[2])
	}
}

// TestCallRoutesResponse asserts an outbound call() blocks until the read loop
// routes a matching inbound response to it (and does not surface that response
// to dispatch).
func TestCallRoutesResponse(t *testing.T) {
	respR, respW := io.Pipe() // client -> agent (responses)
	reqR, reqW := io.Pipe()   // agent -> client (requests)
	c := newConn(respR, reqW)

	// Run the read loop: it must consume the response internally (deliver -> true)
	// and never return it as a request.
	go func() {
		for {
			if _, err := c.read(); err != nil {
				return
			}
		}
	}()

	type callResult struct {
		res json.RawMessage
		err error
	}
	done := make(chan callResult, 1)
	go func() {
		res, err := c.call(context.Background(), methodRequestPermission, requestPermissionParams{SessionID: "s1"})
		done <- callResult{res, err}
	}()

	// Read the outbound request off the wire — this also guarantees call() has
	// registered its pending entry before we feed the response back.
	br := bufio.NewReader(reqR)
	line, err := br.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read outbound request: %v", err)
	}
	var env rpcEnvelope
	if err := json.Unmarshal(line, &env); err != nil {
		t.Fatalf("decode outbound request: %v", err)
	}
	if env.Method != methodRequestPermission {
		t.Fatalf("outbound method = %q, want %q", env.Method, methodRequestPermission)
	}

	// Echo the id back on a response carrying a result.
	resp := rpcResponse{JSONRPC: jsonRPCVersion, ID: env.ID, Result: json.RawMessage(`{"outcome":{"outcome":"selected","optionId":"session"}}`)}
	rl, _ := json.Marshal(resp)
	if _, err := respW.Write(append(rl, '\n')); err != nil {
		t.Fatalf("write response: %v", err)
	}

	select {
	case cr := <-done:
		if cr.err != nil {
			t.Fatalf("call: %v", cr.err)
		}
		var got requestPermissionResult
		if err := json.Unmarshal(cr.res, &got); err != nil {
			t.Fatalf("decode call result: %v", err)
		}
		if got.Outcome.Outcome != "selected" || got.Outcome.OptionID != "session" {
			t.Fatalf("unexpected outcome: %+v", got.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call did not return after response routed")
	}
}

func splitLines(t *testing.T, b []byte) [][]byte {
	t.Helper()
	raw := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	out := make([][]byte, 0, len(raw))
	for _, l := range raw {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		out = append(out, l)
	}
	return out
}
