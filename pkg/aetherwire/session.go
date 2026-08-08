package aetherwire

import (
	"encoding/json"
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// ParseSessionFrame recognizes the shared session-protocol frame on a
// multiplexed Aether topic. ok=false means the payload is another message
// shape. A recognized but invalid frame returns ok=true and an error so the
// receiver can return a correlated session error instead of treating it as a
// ChatMessage.
func ParseSessionFrame(payload []byte) (frame spec.SessionFrame, ok bool, err error) {
	var probe struct {
		ProtocolVersion *uint32         `json:"protocol_version"`
		SchemaRevision  *uint32         `json:"schema_revision"`
		Type            string          `json:"type"`
		Payload         json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(payload, &probe) != nil {
		return spec.SessionFrame{}, false, nil
	}
	if probe.ProtocolVersion == nil || probe.SchemaRevision == nil || probe.Type == "" || len(probe.Payload) == 0 {
		return spec.SessionFrame{}, false, nil
	}
	if err := json.Unmarshal(payload, &frame); err != nil {
		return spec.SessionFrame{}, true, fmt.Errorf("aetherwire: decode session frame: %w", err)
	}
	if err := frame.Validate(); err != nil {
		return frame, true, fmt.Errorf("aetherwire: invalid session frame: %w", err)
	}
	return frame, true, nil
}

// SessionFramePayload validates and encodes one shared frame for Aether.
func SessionFramePayload(frame spec.SessionFrame) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, fmt.Errorf("aetherwire: invalid session frame: %w", err)
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("aetherwire: encode session frame: %w", err)
	}
	return payload, nil
}
