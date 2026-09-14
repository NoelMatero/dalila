The idea

A single-binary cluster agent, written in Rust.

You install it on a set of machines and point one at another. They form a cluster and keep track of which members are alive. On top of that, you get load balancing / proxying across the healthy machines.

The pitch is that it should be easy: download the CLI, run it, and a handful of machines start behaving like one pool. No config files listing every backend, no Kubernetes.

Status

Concept stage. Nothing built yet, nothing decided yet — scope, architecture, and naming are all open.

Notes
The interesting problem is probably not the proxying, but membership: knowing which machines are actually alive, and distinguishing "dead" from "slow" from "unreachable from this particular node."
Gossip-based membership protocols (SWIM and its relatives) are the usual approach and worth reading up on. Rust already has libraries here — foca, memberlist-rs, chitchat — so worth checking what exists before deciding whether to build or reuse.
Possible later direction: doing more with the cluster once membership works.
