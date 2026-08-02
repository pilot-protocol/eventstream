// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"fmt"
	"sync"
	"time"

	"github.com/pilot-protocol/common/decision"
)

// maxGovernedReplayEntries bounds the receiver-side replay cache; entries
// expire at the intent's own ExpiresAt (<= 5-minute MaxIntentTTL) so the cache
// drains under legitimate signed traffic. On overflow a publication is refused
// (fail closed) rather than admitted un-deduplicated.
const maxGovernedReplayEntries = 1 << 20

// governedReplayGuard rejects a second fan-out of the same signed governed
// event. Without it a verified GovernedEvent is a bearer capability: any peer
// that observed one could re-publish the exact bytes within the intent TTL,
// producing duplicate authorized publications and re-charging the signing
// agent's quota. Dedup is keyed on the signature-authenticated
// (tenant, agent, intent id); a legitimate re-publish must carry a fresh
// intent.
type governedReplayGuard struct {
	mu   sync.Mutex
	seen map[string]int64
	now  func() time.Time
}

func newGovernedReplayGuard() *governedReplayGuard {
	return &governedReplayGuard{seen: make(map[string]int64), now: time.Now}
}

func (g *governedReplayGuard) admit(intent decision.Intent) error {
	if intent.ID == "" {
		return nil
	}
	now := g.now().Unix()
	key := intent.TenantID + "\x1f" + intent.AgentID + "\x1f" + intent.ID
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, exp := range g.seen {
		if exp <= now {
			delete(g.seen, k)
		}
	}
	if exp, ok := g.seen[key]; ok && exp > now {
		return fmt.Errorf("governed event already published (replay rejected)")
	}
	if len(g.seen) >= maxGovernedReplayEntries {
		return fmt.Errorf("governed replay cache saturated")
	}
	expiresAt := intent.ExpiresAt
	if expiresAt <= now {
		expiresAt = now + int64(decision.MaxIntentTTL/time.Second)
	}
	g.seen[key] = expiresAt
	return nil
}
