# eventstream

Pilot Protocol eventstream plugin — service port 1002. Provides
pub/sub event delivery between Pilot peers using the daemon's
reliable stream transport.

## Files

| File | What it does |
|---|---|
| `eventstream.go` | Wire format: `Event{Topic, Payload}`, length-prefixed framing. `WriteEvent` / `ReadEvent`. |
| `client.go` | Subscriber side: `Client.Dial`, `.Subscribe(topic)`, `.Read`, `.Close`. |
| `server.go` | Publisher side: `Server` accepts inbound stream connections and broadcasts. |
| `service.go` | `*Service` — `coreapi.Service` adapter, binds port 1002. Build tag `!no_eventstream`. |
| `service_disabled.go` | Stub when `-tags no_eventstream` is set. |
| `examples/main.go` | Minimal publisher + subscriber example. |

## Daemon wiring

```go
import "github.com/pilot-protocol/eventstream"

rt.Register(eventstream.NewService())
```

## CLI use (via the protocol repo's `pilotctl`)

```bash
pilotctl subscribe <peer-address> ticker.btcusd     # listen for events
pilotctl publish   <peer-address> ticker.btcusd --data '...'
```

## Disabling

`go build -tags no_eventstream` → stub service.
