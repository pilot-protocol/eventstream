# eventstream

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
| `eventstream.go` | Wire format: `Event{Topic, Payload}`, length-prefixed framing. `WriteEvent` / `ReadEvent`. |
| `client.go` | Subscriber side: `Client.Dial`, `.Subscribe(topic)`, `.Read`, `.Close`. |
| `server.go` | Publisher side: `Server` accepts inbound stream connections and broadcasts. |
| `service.go` | `*Service` — `coreapi.Service` adapter, binds port 1002. Build tag `!no_eventstream`. |
| `service_disabled.go` | Stub when `-tags no_eventstream` is set. |
| `examples/main.go` | Minimal publisher + subscriber example. |

## Build tags

| Tag | Effect |
|---|---|
| `no_eventstream` | Compiles a no-op stub service. |
