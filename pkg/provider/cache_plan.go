package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// PromptCachePlan describes the request-local portions of a provider request
// that are expected to remain stable. It contains digests only; prompt content
// and tool schemas are never copied into telemetry.
type PromptCachePlan struct {
	StablePrefixBytes   int
	StablePromptDigest  string
	DynamicSuffixDigest string
	ToolSchemaDigest    string
}

// BuildPromptCachePlan derives stable prompt and ordered tool-schema digests
// from the exact request that will be sent to the provider. The plan is
// request-local and never mutates transcript messages.
func BuildPromptCachePlan(req ChatRequest) PromptCachePlan {
	var plan PromptCachePlan
	for _, msg := range req.Messages {
		if msg.Role != protocol.RoleSystem {
			continue
		}
		content := messageText(msg)
		boundary, ok := stablePrefixBytes(msg, content)
		if !ok {
			boundary = len(content)
		}
		plan.StablePrefixBytes = boundary
		plan.StablePromptDigest = digestBytes([]byte(content[:boundary]))
		plan.DynamicSuffixDigest = digestBytes([]byte(content[boundary:]))
	}
	if len(req.Tools) > 0 {
		h := sha256.New()
		for _, tool := range req.Tools {
			encoded, err := json.Marshal(tool)
			if err != nil {
				continue
			}
			_, _ = h.Write(encoded)
			_, _ = h.Write([]byte{0})
		}
		plan.ToolSchemaDigest = hex.EncodeToString(h.Sum(nil))
	}
	return plan
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

// stablePrefixBytes returns the declared byte boundary when it is present and
// valid for the supplied UTF-8 content. A boundary in the middle of a UTF-8
// sequence is rejected rather than silently changing the cacheable prompt.
func stablePrefixBytes(msg protocol.ChatMessage, content string) (int, bool) {
	raw, ok := msg.Meta["scitrera"]
	if !ok || len(raw) == 0 {
		return 0, false
	}
	var namespace struct {
		Cache struct {
			StablePrefixChars int `json:"stable_prefix_chars"`
		} `json:"cache"`
	}
	if err := json.Unmarshal(raw, &namespace); err != nil {
		return 0, false
	}
	boundary := namespace.Cache.StablePrefixChars
	if boundary <= 0 || boundary > len(content) {
		return 0, false
	}
	if boundary < len(content) && content[boundary]&0xc0 == 0x80 {
		return 0, false
	}
	return boundary, true
}
