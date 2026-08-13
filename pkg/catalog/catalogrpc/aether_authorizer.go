package catalogrpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const maxAetherAccessCheckBatch = 100

type batchAccessChecker interface {
	BatchCheckAccess(context.Context, []*pb.ResourceAccessRequest, *pb.AuthorizationContext) ([]*pb.AccessDecisionReceipt, error)
}

// AetherEntryAuthorizer evaluates exact catalog resources under the
// gateway-derived authorization continuation attached to the current request.
// It never falls back to the catalog service's direct ACL identity.
type AetherEntryAuthorizer struct {
	checker batchAccessChecker
}

func NewAetherEntryAuthorizer(checker batchAccessChecker) (*AetherEntryAuthorizer, error) {
	if checker == nil {
		return nil, fmt.Errorf("catalogrpc: Aether access checker is required")
	}
	return &AetherEntryAuthorizer{checker: checker}, nil
}

func (a *AetherEntryAuthorizer) AuthorizeCatalogEntry(ctx context.Context, action string, binding catalog.QueryBinding, entry spec.ToolCatalogEntry) (bool, error) {
	decisions, err := a.AuthorizeCatalogEntries(ctx, action, binding, []spec.ToolCatalogEntry{entry})
	if err != nil {
		return false, err
	}
	return decisions[0], nil
}

func (a *AetherEntryAuthorizer) AuthorizeCatalogEntries(ctx context.Context, action string, binding catalog.QueryBinding, entries []spec.ToolCatalogEntry) ([]bool, error) {
	if len(entries) == 0 {
		return []bool{}, nil
	}
	forwarded, _ := ctx.Value(forwardedAuthorizationContextKey{}).(*pb.ForwardedAuthorization)
	if forwarded == nil || forwarded.GetAuthorization() == nil {
		return nil, fmt.Errorf("catalogrpc: trusted forwarded authorization is missing")
	}
	if binding.AuthorityLineageID == "" || forwarded.GetRootGrantId() != binding.AuthorityLineageID {
		return nil, fmt.Errorf("catalogrpc: forwarded authorization lineage does not match query binding")
	}
	requiredLevel, err := catalogActionAccessLevel(action)
	if err != nil {
		return nil, err
	}

	decisions := make([]bool, len(entries))
	for start := 0; start < len(entries); start += maxAetherAccessCheckBatch {
		end := start + maxAetherAccessCheckBatch
		if end > len(entries) {
			end = len(entries)
		}
		requests := make([]*pb.ResourceAccessRequest, end-start)
		for i := start; i < end; i++ {
			resourceID, resourceErr := catalog.EntryResourceID(binding.Context, entries[i].Ref)
			if resourceErr != nil {
				return nil, fmt.Errorf("catalogrpc: canonical catalog resource: %w", resourceErr)
			}
			requests[i-start] = &pb.ResourceAccessRequest{
				ResourceType: "tool-catalog/entry", ResourceId: resourceID,
				Operation: action, Workspace: binding.Context.WorkspaceID,
				RequiredAccessLevel: requiredLevel, CorrelationId: catalogAccessCorrelation(action, resourceID),
			}
		}
		receipts, checkErr := a.checker.BatchCheckAccess(ctx, requests, forwarded.GetAuthorization())
		if checkErr != nil {
			return nil, fmt.Errorf("catalogrpc: Aether batch access check: %w", checkErr)
		}
		if len(receipts) != len(requests) {
			return nil, fmt.Errorf("catalogrpc: Aether returned %d decisions for %d requests", len(receipts), len(requests))
		}
		for i, receipt := range receipts {
			if err := validateAccessReceipt(receipt, requests[i]); err != nil {
				return nil, err
			}
			decisions[start+i] = receipt.GetAllowed()
		}
	}
	return decisions, nil
}

func catalogAccessCorrelation(action, resourceID string) string {
	digest := sha256.Sum256([]byte(action + "\x00" + resourceID))
	return "catalog.access." + hex.EncodeToString(digest[:])
}

func catalogActionAccessLevel(action string) (int32, error) {
	switch action {
	case catalog.CatalogActionDiscover, catalog.CatalogActionDescribe, catalog.CatalogActionInvokeRead:
		return 10, nil
	case catalog.CatalogActionInvokeWrite, catalog.CatalogActionInvokeExecute,
		catalog.CatalogActionInvokeExternal, catalog.CatalogActionInvokeInteraction:
		return 20, nil
	default:
		return 0, fmt.Errorf("catalogrpc: unsupported catalog authorization action %q", action)
	}
}

func validateAccessReceipt(receipt *pb.AccessDecisionReceipt, request *pb.ResourceAccessRequest) error {
	if receipt == nil || receipt.GetRequest() == nil {
		return fmt.Errorf("catalogrpc: Aether returned a malformed access decision")
	}
	actual := receipt.GetRequest()
	if actual.GetResourceType() != request.GetResourceType() ||
		actual.GetResourceId() != request.GetResourceId() ||
		actual.GetOperation() != request.GetOperation() ||
		actual.GetWorkspace() != request.GetWorkspace() ||
		actual.GetRequiredAccessLevel() != request.GetRequiredAccessLevel() {
		return fmt.Errorf("catalogrpc: Aether access decision does not match its request")
	}
	return nil
}

var _ catalog.EntryAuthorizer = (*AetherEntryAuthorizer)(nil)
var _ catalog.BatchEntryAuthorizer = (*AetherEntryAuthorizer)(nil)
