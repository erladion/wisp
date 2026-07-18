# Wisp wire protocol

The contract between brokers, clients, and bindings. Everything here is
observable on the wire; internals (queues, threads, timeouts that only affect
one side) are out of scope. Source of truth for the encodings:
[common/wireframe.h](common/wireframe.h) and
[common/proto/broker.proto](common/proto/broker.proto).

## Transport and framing

Clients connect a ZeroMQ **DEALER** socket to the broker's **ROUTER** endpoint
(any ZeroMQ transport; the stock broker binds `tcp://*:5555` and
`ipc:///tmp/broker.sock`). The DEALER's ZeroMQ routing id is the client's
identity — set it to a unique, stable client id.

A message is one ZeroMQ multipart group of up to two application frames:

| Frame | Content |
|---|---|
| header | 1 format byte + encoded `broker.MessageHeader` (see below) |
| payload | opaque bytes; **omitted entirely** when the payload is empty |

On the ROUTER side ZeroMQ prepends the sender's identity frame; DEALER-side
traffic carries no identity frame. Frames beyond the payload are discarded.
Frames larger than **16 MiB** are rejected at the transport (`ZMQ_MAXMSGSIZE`);
both sides set this.

## Header frame

Byte 0 identifies the header encoding; the rest of the frame is that codec's
output. A receiver that does not recognize the format byte drops the message.

| Format byte | Encoding |
|---|---|
| `1` | serialized `broker.MessageHeader` (protobuf) — the default |
| `2` | reserved: fixed binary layout (experimental, `header-codec` branch) |

`broker.MessageHeader`:

```proto
message MessageHeader {
  string handler_key = 1;      // control key or application handler name
  string sender_id = 2;        // client id of the original sender
  string topic = 3;            // routing topic; "" in a SUBSCRIBE = wildcard
  string origin_broker_id = 4; // stamped by the first broker; loop detection
  bytes  message_uuid = 5;     // stamped by the first broker; 16 raw UUIDv4 bytes
  string reply_topic = 6;      // request/reply: where to publish the answer
}
```

Senders leave `message_uuid` and `origin_broker_id` empty; the first broker to
route a message stamps both. Any non-empty `message_uuid` value is treated as
opaque for dedup, so its exact form is not part of the contract.

## Payload frame

The broker never parses the payload. The C++/binding client library uses these
conventions (a custom client may use anything):

- Protobuf messages travel packed in a `google.protobuf.Any`
  (`type.googleapis.com/<full.type.name>` type url), so receivers can verify
  the type before parsing.
- Strings and raw bytes travel as-is.
- Trivially-copyable structs travel as raw memory (same-ABI endpoints only).

## Session lifecycle

**Handshake.** The broker consumes the *first* envelope it sees from an
unknown identity — whatever it is — and answers with a `__RESET__` message.
The client must respond by sending `__CONNECT__` and re-sending its
`__SUBSCRIBE__`s. Because the first envelope is sacrificed, clients must not
lead with application data; the stock client library holds data messages until
the connection is online and leads with control traffic.

`__RESET__` can arrive again at any time (e.g. after a broker restart or after
the broker timed the client out); the reaction is the same: re-subscribe
everything. Re-subscribing is idempotent.

**Liveness.** Clients send `__HEARTBEAT__` on an interval (default 3 s; keep
it under the broker's timeout) and the broker answers `__HEARTBEAT_ACK__`.
The broker forgets clients that have been silent for **10 s** (checked every
2 s); a forgotten client is re-treated as unknown on its next message (see
handshake). Any received traffic counts as liveness in both directions.

**Goodbye.** `__DISCONNECT__` removes the client's state immediately;
otherwise the zombie timeout cleans up.

## Control keys

Control messages use reserved `handler_key` values; the broker acts on them
and never routes them to subscribers. Sent by the client unless noted:

| Key | Meaning |
|---|---|
| `__CONNECT__` | presence announcement; no payload |
| `__DISCONNECT__` | immediate goodbye |
| `__HEARTBEAT__` | liveness probe; broker answers `__HEARTBEAT_ACK__` |
| `__HEARTBEAT_ACK__` | broker → client answer |
| `__RESET__` | broker → client: your state is gone; send `__CONNECT__` and every `__SUBSCRIBE__` again |
| `__SUBSCRIBE__` | subscribe to `topic`; `topic = ""` is the wildcard (every topic) |
| `__UNSUBSCRIBE__` | remove one subscription |
| `__SET_CLUSTER__` | payload = new discovery cluster name (1–64 bytes, no `\|`); handled by the receiving broker only |
| `__SYS_STATS__` | broker → subscribers, every 1 s: payload is an Any-packed `broker.SystemStats` |

Stats are delivered only to clients that subscribed to `__SYS_STATS__`
*exactly* — wildcard subscribers do not receive them (this keeps per-broker
stats off the peer mesh).

## Routing

A non-control message is delivered, one copy per client, to every subscriber
of its exact `topic` plus every wildcard (`""`) subscriber — but never echoed
back to its local sender. Delivery is **best-effort**: there are no acks, no
retries, and a receiver that falls far enough behind loses messages. Ordering
is ZeroMQ's per-connection FIFO; there is no cross-client ordering guarantee.

**Request/reply** is a convention on top of pub/sub: the requester subscribes
to a freshly generated unique topic, sends its request with `reply_topic` set
to that topic, and the responder publishes its answer there. The broker is not
involved beyond normal routing.

## Broker meshing

Brokers link to each other over the same DEALER/ROUTER protocol: a peer link
is a client whose identity starts with `BrokerLink-` and whose only
subscription is the wildcard. Each broker floods every routed message to all
of its peer links.

Loop protection: the stamped `message_uuid` is remembered by each broker
(roughly the last 10–20K ids) and repeats are dropped, so a message crosses
each broker once regardless of mesh shape. Control keys arriving over a peer
link (`__RESET__`, `__HEARTBEAT__`, `__HEARTBEAT_ACK__`, `__SET_CLUSTER__`)
are ignored.

## Discovery

Brokers find each other with UDP broadcast beacons on port **5670** (default),
sent every **1 s**:

```
WISP|1|<cluster>|<uuid>|<router_port>
```

`1` is the beacon format version. Receivers ignore beacons from other
clusters and their own uuid. For each same-cluster pair, exactly one side
dials: the broker with the **smaller uuid** (string comparison) connects to
`tcp://<beacon sender ip>:<router_port>`. A dialed peer whose beacons stop
for **5 s** is dropped. Cluster names must not contain `|` and fit within the
beacon's 512-byte read buffer.

A running broker leaves its mesh and joins another when it receives
`__SET_CLUSTER__`: beacons switch immediately and links *it* dialed drop at
once, but links dialed *by* old-mesh peers persist until those peers' 5 s
beacon timeout — a few seconds of cross-mesh traffic is expected during a
swap.

## Inspector tap

The broker republishes every message it processes — control traffic included —
on a PUB socket at `ipc:///tmp/broker_inspector.sock`, as the same
header + payload frames (no identity frame). Subscribe with an empty
subscription to see everything. The tap is lossy by design: a slow inspector
drops messages rather than slowing the broker.

## Compatibility rules

- New `MessageHeader` fields are appended with fresh field numbers; receivers
  ignore unknown fields (protobuf semantics). Removing or renumbering fields
  is a breaking change.
- New header encodings take a fresh format byte; every previously shipped
  encoding stays decodable.
- New control keys follow the `__NAME__` shape — but beware: a broker that
  does not recognize a handler key treats the message as application traffic
  and routes it by topic. Introducing a new control key is only safe once
  every broker in the mesh understands it.
