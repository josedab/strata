# ADR-0001: Use Raft Consensus for Metadata Cluster

## Status

**Accepted**

## Context

Strata is a distributed file system that must maintain consistent metadata (file names, directory structures, permissions, chunk locations) across multiple nodes. When a client creates a file or modifies metadata, all nodes must agree on the outcome to prevent data corruption or loss.

We needed a consensus mechanism that could:
- Guarantee strong consistency (linearizability) for all metadata operations
- Handle leader failures with automatic failover
- Support cluster membership changes without downtime
- Be understandable and debuggable by the team

Alternatives considered:
1. **Paxos** - Theoretically equivalent but notoriously difficult to implement correctly and reason about
2. **ZooKeeper/etcd as external dependency** - Adds operational complexity, another system to maintain, potential version conflicts
3. **Gossip-based eventual consistency** - Insufficient for file system semantics where reads must see prior writes
4. **Single-node metadata** - No fault tolerance, single point of failure

## Decision

We implemented a custom Raft consensus protocol for the metadata cluster with the following components:

```
src/raft/
├── mod.rs          # StateMachine trait, public exports
├── node.rs         # RaftNode - leader election, log replication
├── state.rs        # RaftState - leader/follower/candidate transitions
├── log.rs          # RaftLog - append-only replicated log
├── storage.rs      # Persistent storage for log and snapshots
└── rpc.rs          # RPC message types
```

Key implementation choices:

1. **Custom implementation over library**: We chose to implement Raft ourselves rather than use a library like `raft-rs` to:
   - Have full control over the state machine interface
   - Avoid dependency on external crate's release schedule
   - Enable tight integration with our metadata operations

2. **State machine snapshots**: The `StateMachine` trait requires `snapshot()` and `restore()` methods:
   ```rust
   pub trait StateMachine: Send + Sync {
       fn apply(&mut self, entry: &LogEntry) -> Result<Vec<u8>>;
       fn snapshot(&self) -> Result<Vec<u8>>;
       fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
   }
   ```

3. **Configurable timeouts**: Election and heartbeat timeouts are configurable for different network conditions:
   ```rust
   pub struct RaftConfig {
       pub election_timeout_min: Duration,  // Default: 150ms
       pub election_timeout_max: Duration,  // Default: 300ms
       pub heartbeat_interval: Duration,    // Default: 50ms
       pub snapshot_threshold: usize,       // Default: 10,000 entries
   }
   ```

4. **Snapshot threshold**: Automatic snapshotting after 10,000 log entries prevents unbounded log growth.

## Consequences

### Positive

- **Strong consistency**: All metadata operations are linearizable. A successful write is guaranteed to be visible to subsequent reads
- **Automatic failover**: Leader failures trigger election within ~300ms, minimal disruption to clients
- **No external dependencies**: Entire consensus mechanism is self-contained, simplifying deployment
- **Debuggability**: Team has full visibility into consensus state, can add logging/metrics as needed
- **Snapshot support**: Bounded memory usage, fast node recovery from snapshots

### Negative

- **Implementation complexity**: ~3,000 lines of consensus code that must be correct. Bugs in Raft implementation can cause data loss
- **Write latency**: Every metadata write requires majority acknowledgment (typically 2 of 3 nodes), adding network round-trip latency
- **Minimum cluster size**: Requires 3+ nodes for fault tolerance (2-node cluster has no fault tolerance)
- **Split-brain risk**: Network partitions can cause temporary unavailability until partition heals

### Operational Implications

- **Odd cluster sizes**: Deploy 3, 5, or 7 metadata nodes (even numbers provide no additional fault tolerance)
- **Network latency sensitivity**: Cross-datacenter deployments need tuned election timeouts
- **Monitoring requirements**: Must monitor Raft term, commit index, and leader status

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - "In Search of an Understandable Consensus Algorithm"
- `src/raft/node.rs` - Core RaftNode implementation
- `src/metadata/state_machine.rs` - MetadataStateMachine implementing StateMachine trait
