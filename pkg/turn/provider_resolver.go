package turn

import (
	"log/slog"
	"os"
	"sync"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// ProviderResolver maps a per-turn model name to the Provider that should serve
// it, so a single runner can fan out to multiple upstreams (config/models.yaml
// providers). (_, false) means "no per-model provider — use the runner's default
// provider", which is also the answer for a nil resolver, a bare model (no
// provider declared), an unknown provider name, or a construction failure. It
// never fails the turn: a resolution problem always degrades to the default.
type ProviderResolver interface {
	ProviderForModel(model string) (Provider, bool)
}

// NewProviderResolver builds a ProviderResolver over a model.Registry. It returns
// nil when reg is nil (the caller then leaves Config.ProviderResolver unset and
// behavior is unchanged single-provider). def supplies fallback fields (BaseURL,
// Format, and API key/keyenv) for a provider config that omits them; guard is
// applied to every constructed provider (e.g. provider.SandboxGuard). Clients are
// constructed lazily on first use and cached per provider name (thread-safe).
func NewProviderResolver(reg *modelpkg.Registry, def modelpkg.ProviderConfig, guard provider.Guard) ProviderResolver {
	if reg == nil {
		return nil
	}
	return &providerResolver{reg: reg, def: def, guard: guard, cache: map[string]Provider{}}
}

type providerResolver struct {
	reg   *modelpkg.Registry
	def   modelpkg.ProviderConfig
	guard provider.Guard

	mu    sync.Mutex
	cache map[string]Provider
}

func (r *providerResolver) ProviderForModel(model string) (Provider, bool) {
	pc, ok := r.reg.ProviderFor(model)
	if !ok {
		return nil, false // bare/unknown model or unknown provider → default
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, cached := r.cache[pc.Name]; cached {
		if p == nil {
			return nil, false // a prior construction failed; stay on the default
		}
		return p, true
	}
	p, err := r.build(pc)
	if err != nil {
		// Cache the failure (as nil) so we don't retry-and-log every turn, and fall
		// back to the default provider rather than hard-failing the turn.
		slog.Warn("provider resolver: could not build per-model provider; using default",
			slog.String("provider", pc.Name), slog.String("model", model), slog.Any("err", err))
		r.cache[pc.Name] = nil
		return nil, false
	}
	r.cache[pc.Name] = p
	return p, true
}

// build constructs a SidecarClient from the resolved SidecarConfig for pc.
func (r *providerResolver) build(pc modelpkg.ProviderConfig) (Provider, error) {
	return provider.NewSidecarClient(r.sidecarConfig(pc))
}

// sidecarConfig lowers a ProviderConfig to a provider.SidecarConfig, filling
// missing fields from the env default and resolving the API key. Fill order:
// BaseURL/Format from def when pc omits them; the key comes from pc (APIKey, else
// APIKeyEnv) or — when pc names neither — from def; APIKeyEnv is read from the
// environment. Kept pure (no client construction) so it is directly observable in
// tests. AuthHeader is "Bearer <key>" when a key resolves, else "".
func (r *providerResolver) sidecarConfig(pc modelpkg.ProviderConfig) provider.SidecarConfig {
	baseURL := pc.BaseURL
	if baseURL == "" {
		baseURL = r.def.BaseURL
	}
	format := pc.Format
	if format == "" {
		format = r.def.Format
	}
	key, keyEnv := pc.APIKey, pc.APIKeyEnv
	if key == "" && keyEnv == "" {
		key, keyEnv = r.def.APIKey, r.def.APIKeyEnv
	}
	resolved := key
	if resolved == "" && keyEnv != "" {
		resolved = os.Getenv(keyEnv)
	}
	authHeader := ""
	if resolved != "" {
		authHeader = "Bearer " + resolved
	}
	wire := provider.FormatOpenAI
	if format == "native" {
		wire = provider.FormatNative
	}
	return provider.SidecarConfig{
		BaseURL:    baseURL,
		AuthHeader: authHeader,
		Format:     wire,
		Guard:      r.guard,
	}
}
