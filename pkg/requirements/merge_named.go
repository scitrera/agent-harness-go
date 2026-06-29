package requirements

import (
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type namedValue[T any] func(T) string
type namedValidator[T any] func(T) error

func mergeNamed[T any](
	source Source,
	field string,
	values []T,
	nameOf namedValue[T],
	validate namedValidator[T],
	target *map[string]Sourced[T],
) error {
	for _, value := range values {
		name := nameOf(value)
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: %s: %s name required", ErrInvalidLayer, source.display(), field)
		}
		if err := validate(value); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrInvalidLayer, source.display(), err)
		}
		incoming := withSource(value, source)
		existing, ok := (*target)[name]
		if ok && !equal(existing.Value, value) {
			return conflict(field+"."+name, existing.Source, source, "definitions differ")
		}
		if !ok {
			(*target)[name] = incoming
		}
	}
	return nil
}

func sourceRules(rules []Sourced[tools.CommandRule]) []Sourced[tools.CommandRule] {
	out := make([]Sourced[tools.CommandRule], len(rules))
	copy(out, rules)
	return out
}

func serverName(cfg mcp.ServerConfig) string {
	return cfg.Name
}

func validateMCPServer(cfg mcp.ServerConfig) error {
	if strings.TrimSpace(cfg.Command) == "" {
		return fmt.Errorf("mcp server %q command required", cfg.Name)
	}
	return nil
}

func catalogName(source CatalogSource) string {
	return source.Name
}

func validateCatalogSource(source CatalogSource) error {
	switch source.Kind {
	case CatalogPlugin, CatalogAgent, CatalogSkill:
	case "":
		return fmt.Errorf("catalog %q kind required", source.Name)
	default:
		return fmt.Errorf("catalog %q has unsupported kind %q", source.Name, source.Kind)
	}
	if strings.TrimSpace(source.Path) == "" && strings.TrimSpace(source.URI) == "" {
		return fmt.Errorf("catalog %q path or uri required", source.Name)
	}
	return nil
}

func modelRole(choice ModelChoice) string {
	return choice.Role
}

func validateModelChoice(choice ModelChoice) error {
	if strings.TrimSpace(choice.Model) == "" {
		return fmt.Errorf("model %q model required", choice.Role)
	}
	return nil
}

func fileStoreName(path FileStorePath) string {
	return path.Name
}

func validateFileStorePath(path FileStorePath) error {
	if strings.TrimSpace(path.Path) == "" {
		return fmt.Errorf("file store %q path required", path.Name)
	}
	return nil
}

func featureName(gate FeatureGate) string {
	return gate.Name
}

func validateFeatureGate(FeatureGate) error {
	return nil
}
