// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"testing"
	"time"

	"github.com/pilot-protocol/common/decision"
)

// TestGovernedPublicationReplayGuard pins SECURITY_REVIEW_v1.14 finding M5 for
// the event broker: a verified governed intent may fan out at most once; a
// replay within its validity window is rejected, and distinct intents pass.
func TestGovernedPublicationReplayGuard(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	clock := base
	g := newGovernedReplayGuard()
	g.now = func() time.Time { return clock }

	intent := decision.Intent{ID: "evt-1", TenantID: "t", AgentID: "pub", ExpiresAt: base.Add(2 * time.Minute).Unix()}
	if err := g.admit(intent); err != nil {
		t.Fatalf("first publication rejected: %v", err)
	}
	if err := g.admit(intent); err == nil {
		t.Fatal("replayed governed publication was ACCEPTED")
	}
	if err := g.admit(decision.Intent{ID: "evt-2", TenantID: "t", AgentID: "pub", ExpiresAt: clock.Add(2 * time.Minute).Unix()}); err != nil {
		t.Fatalf("distinct intent rejected: %v", err)
	}
	clock = base.Add(3 * time.Minute)
	if err := g.admit(decision.Intent{ID: "evt-1", TenantID: "t", AgentID: "pub", ExpiresAt: clock.Add(2 * time.Minute).Unix()}); err != nil {
		t.Fatalf("post-expiry reuse rejected: %v", err)
	}
}
