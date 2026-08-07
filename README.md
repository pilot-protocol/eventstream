# eventstream

[![ci](https://github.com/pilot-protocol/eventstream/actions/workflows/ci.yml/badge.svg)](https://github.com/pilot-protocol/eventstream/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/pilot-protocol/eventstream/branch/main/graph/badge.svg)](https://codecov.io/gh/pilot-protocol/eventstream)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

Pub/sub event delivery plugin for the Pilot Protocol daemon. Binds
service port 1002 and ships events between Pilot peers over the
daemon's reliable stream transport.

## Install

```go
import "github.com/pilot-protocol/eventstream"
```

## Usage

```go
// Register as a plugin on the daemon runtime:
rt.Register(eventstream.NewService())

// Or use the client directly:
c, err := eventstream.Dial(ctx, peerAddr)
if err != nil {
    return err
}
defer c.Close()
if err := c.Subscribe("ticker.btcusd"); err != nil {
    return err
}
for {
    ev, err := c.Read()
    if err != nil {
        return err
    }
    handle(ev)
}
```

From `pilotctl`:

```bash
pilotctl subscribe <peer-address> ticker.btcusd
pilotctl publish   <peer-address> ticker.btcusd --data '...'
```

## Layout

| File | What it does |
|---|---|
| `eventstream.go` | Wire format: `Event{Topic, Payload}`, subscription policy interface, and length-prefixed framing. `WriteEvent` / `ReadEvent`. |
| `client.go` | Subscriber side: `Client.Dial`, `.Subscribe(topic)`, `.Read`, `.Close`. |
| `governed.go` | Signed publication envelope, broker-side verifier, and enforceable topic/payload constraints. |
| `server.go` | Publisher side: `Server` accepts inbound stream connections and broadcasts. |
| `service.go` | `*Service` — `coreapi.Service` adapter, binds port 1002. Build tag `!no_eventstream`. |
| `service_disabled.go` | Stub when `-tags no_eventstream` is set. |
| `examples/main.go` | Minimal publisher + subscriber example. |

## Build tags

| Tag | Effect |
|---|---|
| `no_eventstream` | Compiles a no-op stub service. |

## Governed publication

An enterprise broker can call `SetGovernedPublication` before `Start` with a
`DecisionEventVerifier` (or an equivalent local verifier). Its
`GovernedTopic` transport envelope binds a publication topic and exact bytes to
a signed `decision.Intent` and `decision.Decision`. The broker verifies the
tenant authority state, local deterministic ceiling, exact broker resource,
and understood constraints before it forwards the original event. Subscribers
receive the original topic and payload, never the envelope.

After all publishers have been upgraded, set `require` to `true`; unsigned
legacy publications are then dropped at the broker. This is intentionally a
publication control: subscription access remains governed by the existing
`TopicPolicy`. A workflow-approved publication uses the same short-lived
execution Decision as an ordinary allowed publication, rather than a reusable
workflow token.

For a typed-disclosure profile, use `PublishGovernedWithDisclosure` and bind
the canonical `decision.DisclosureBinding` hash into the signed Intent. A
`DecisionEventVerifier` with `RequireDisclosure` rejects otherwise-valid
governed publications that do not carry matching content metadata.
With a receipt recorder configured, typed publications require V2
disclosure-evidence support before broker fanout; the signed receipt contains
only the disclosure hash, not event plaintext.

For auditable enterprise publishing, also call `SetGovernedReceiptRecorder`
before `Start` and require it. The broker appends evidence for the exact signed
Intent and Decision before fanout; a recorder failure denies the publication,
so subscribers never receive an unreceipted governed event.

### Local content inspection

Call `SetGovernedContentInspector` before `Start` to inspect verified governed
event bytes locally before broker fan-out. `RequireGovernedContentInspection`
makes a missing hook a startup failure, while a detector error rejects only the
publication. `decision.PresidioInspector` provides a bounded text/structured-
text adapter for a tenant-local OSS Presidio service; unsupported binary types
are rejected rather than bypassing inspection. This hook is not invoked by the
central decision authority.

Typed disclosure binding V2 can carry a signed `retention_class`, which a
policy may restrict before fan-out. This binds metadata for downstream
retention operations; it is not a substitute for a retention executor.

### Per-agent publication quotas

`SetGovernedTransferQuota` applies a bounded local byte/action budget to each
verified publisher `Intent.AgentID`. The budget is charged after signature and
local-policy verification, never from a network address or caller-supplied
identity, and covers admitted attempts that later fail local inspection or
receipt recording.

## License

AGPL-3.0-or-later. See [LICENSE](LICENSE).
