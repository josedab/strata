# ADR-0001: Raft Consensus for Metadata

## Status

Accepted

## Context

Strata is a distributed file system that must maintain consistent metadata across multiple nodes. File system metadata operations (creating files, renaming directories, updating permissions) require strong consistency guarantees:

- **Atomicity**: Directory operations must complete fully or not at all
- **Ordering**: Operations must be applied in a consistent order across all nodes
- **Durability**: Committed operations must survive node failures
- **Availability**: The system must continue operating when minority nodes fail

We evaluated several approaches for distributed metadata management:

1. **Single-node metadata server**: Simple but creates a single point of failure
2. **Eventual consistency (CRDTs)**: High availability but allows conflicting file system states
3. **Two-phase commit**: Strong consistency but blocks on any node failure
4. **Consensus protocols (Paxos, Raft, Viewstamped Replication)**: Strong consistency with minority failure tolerance

File systems have particularly strict requirements compared to other distributed systems. A user creating `/tmp/foo/bar` expects that path to exist immediately and consistently. Eventual consistency would allow scenarios where one node sees the directory while another doesn't, leading to confusing failures.

## Decision

We will use the **Raft consensus algorithm** for metadata cluster coordination.

Specifically:
- Metadata operations are submitted as commands to the Raft log
- A leader node coordinates all writes and replicates to followers
- Reads can be served by the leader (for linearizability) or followers (for throughput)
- The metadata state machine applies committed log entries to update file system state
- Snapshots compact the log to bound storage growth

We chose Raft over Paxos for its understandability and the availability of well-tested implementations. The protocol provides:
- Leader election with randomized timeouts
- Log replication with consistency checks
- Membership changes (planned for future implementation)

Implementation is in `src/raft/` with key components:
- `node.rs`: Core Raft node logic (leader/follower/candidate states)
- `state.rs`: State machine interface
- `log.rs`: Replicated log with persistence
- `rpc.rs`: Inter-node communication types

## Consequences

### Positive

- **Linearizable consistency**: All clients see the same file system state
- **Automatic failover**: Leader election completes in milliseconds when nodes fail
- **Split-brain prevention**: Only one leader can exist per term
- **Proven correctness**: Raft has formal proofs and extensive industry deployment
- **Understandable**: Easier to debug and reason about than Paxos

### Negative

- **Write latency**: Every write requires majority acknowledgment (typically 2 of 3 or 3 of 5 nodes)
- **Leader bottleneck**: All writes funnel through a single leader
- **Implementation complexity**: Raft is simpler than Paxos but still requires careful implementation
- **Network sensitivity**: Performance degrades with network latency between nodes

### Implications

- Metadata cluster should be deployed in low-latency network (same datacenter/region)
- Odd number of nodes recommended (3 or 5) for optimal fault tolerance
- Read-heavy workloads can scale by adding follower reads (with staleness tradeoff)
- Future work needed for snapshot streaming and dynamic membership changes

## References

- Ongaro, D., & Ousterhout, J. (2014). "In Search of an Understandable Consensus Algorithm"
- `src/raft/mod.rs` - Module entry point
- `src/metadata/state_machine.rs` - File system state machine
