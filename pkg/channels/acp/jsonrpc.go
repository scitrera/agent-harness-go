package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// jsonRPCVersion is the only supported JSON-RPC version.
const jsonRPCVersion = "2.0"

// JSON-RPC 2.0 error codes used by the ACP channel.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// rpcRequest is an inbound JSON-RPC request or notification. A notification has
// no id. Params is kept raw for method-specific decoding; ID is kept raw so a
// response echoes the client's exact value (number or string) verbatim.
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// isNotification reports whether the message carries no id (JSON-RPC
// notification: fire-and-forget, no response expected).
func (r rpcRequest) isNotification() bool { return len(r.ID) == 0 }

// rpcError is the JSON-RPC error object.
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// rpcResponse is an outbound JSON-RPC response (Result XOR Error).
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// rpcNotification is an outbound JSON-RPC notification (no id, no response).
type rpcNotification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// conn is a newline-delimited JSON-RPC 2.0 transport over a reader/writer pair
// (ACP's stdio framing: one JSON value per line). Writes are serialized under a
// mutex so notifications emitted from the turn goroutine and responses written
// from the read loop never interleave on the wire. It is hand-rolled — there is
// no official Go ACP SDK and the channel takes no external dependency.
type conn struct {
	r  *bufio.Reader
	w  io.Writer
	mu sync.Mutex
}

func newConn(r io.Reader, w io.Writer) *conn {
	return &conn{r: bufio.NewReaderSize(r, 64*1024), w: w}
}

// read returns the next JSON-RPC message. Blank lines are skipped. It returns
// io.EOF when the stream closes.
func (c *conn) read() (rpcRequest, error) {
	for {
		line, err := c.readLine()
		if err != nil {
			return rpcRequest{}, err
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			return rpcRequest{}, fmt.Errorf("acp: decode jsonrpc: %w", err)
		}
		return req, nil
	}
}

// readLine reads a single '\n'-terminated line. A final line lacking a trailing
// newline (immediately before EOF) is still returned.
func (c *conn) readLine() ([]byte, error) {
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			return line, nil
		}
		return nil, err
	}
	return line, nil
}

// writeResponse marshals result and writes a JSON-RPC response echoing id.
func (c *conn) writeResponse(id json.RawMessage, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("acp: marshal result: %w", err)
	}
	return c.writeValue(rpcResponse{JSONRPC: jsonRPCVersion, ID: normalizeID(id), Result: raw})
}

// writeError writes a JSON-RPC error response.
func (c *conn) writeError(id json.RawMessage, code int, msg string) error {
	return c.writeValue(rpcResponse{JSONRPC: jsonRPCVersion, ID: normalizeID(id), Error: &rpcError{Code: code, Message: msg}})
}

// notify writes a JSON-RPC notification (no id).
func (c *conn) notify(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("acp: marshal params: %w", err)
	}
	return c.writeValue(rpcNotification{JSONRPC: jsonRPCVersion, Method: method, Params: raw})
}

// writeValue serializes v and writes it as one newline-terminated line.
func (c *conn) writeValue(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("acp: marshal message: %w", err)
	}
	data = append(data, '\n')
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = c.w.Write(data)
	return err
}

// normalizeID returns a JSON null id when none was supplied (JSON-RPC requires
// an id member on responses, even for otherwise un-correlatable errors).
func normalizeID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}
