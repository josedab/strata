# Strata Architecture Overview

This document provides a comprehensive overview of Strata's distributed file system architecture.

## System Architecture

Strata is designed as a distributed file system with three main layers: Access, Metadata, and Data.

```mermaid
graph TB
    subgraph "Access Layer"
        FUSE[FUSE Client]
        S3[S3 Gateway]
        CLI[CLI Tools]
        SDK[Native SDK]
    end

    subgraph "Metadata Cluster"
        M1[Metadata Node 1<br/>Leader]
        M2[Metadata Node 2<br/>Follower]
        M3[Metadata Node 3<br/>Follower]
        M1 <-->|Raft| M2
        M2 <-->|Raft| M3
        M1 <-->|Raft| M3
    end

    subgraph "Data Layer"
        D1[Data Server 1]
        D2[Data Server 2]
        D3[Data Server 3]
        D4[Data Server N]
    end

    FUSE --> M1
    S3 --> M1
    CLI --> M1
    SDK --> M1

    M1 -.->|Placement Info| D1
    M1 -.->|Placement Info| D2
    M1 -.->|Placement Info| D3
    M1 -.->|Placement Info| D4

    FUSE -->|Read/Write| D1
    FUSE -->|Read/Write| D2
    S3 -->|Read/Write| D3
    SDK -->|Read/Write| D4
```

## Component Overview

### Access Layer

The access layer provides multiple interfaces for clients to interact with Strata:

| Component | Protocol | Use Case |
|-----------|----------|----------|
| **FUSE Client** | POSIX | Mount as local filesystem |
| **S3 Gateway** | HTTP/S3 | Cloud-native applications |
| **CLI Tools** | gRPC | Administration and scripting |
| **Native SDK** | gRPC | Application integration |

### Metadata Cluster

The metadata cluster manages the file system namespace and coordinates data placement.

```mermaid
graph LR
    subgraph "Metadata Node"
        RN[Raft Node]
        SM[State Machine]
        OP[Operations Handler]

        RN -->|Apply| SM
        OP -->|Propose| RN
        SM -->|Query| OP
    end

    subgraph "Persistent Storage"
        RL[Raft Log]
        DB[(RocksDB)]
    end

    RN -->|Persist| RL
    SM -->|Store| DB
```

**Key responsibilities:**
- File and directory metadata (inodes, directories)
- Namespace operations (create, delete, rename)
- Chunk placement tracking
- Lease management
- Quota enforcement

### Data Layer

Data servers store the actual file content as chunks with erasure coding.

```mermaid
graph TB
    subgraph "Data Server"
        CS[Chunk Storage]
        EC[Erasure Encoder]
        CA[Cache Layer]

        EC -->|Encode| CS
        CS -->|Cache| CA
    end

    subgraph "Storage Backend"
        L1[Local Disk]
        L2[SSD Cache]
    end

    CS --> L1
    CA --> L2
```

**Key responsibilities:**
- Chunk storage and retrieval
- Erasure encoding/decoding
- Caching hot data
- Integrity verification (checksums)
- Background scrubbing

## Data Flow

### Write Path

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Metadata
    participant D1 as Data Server 1
    participant D2 as Data Server 2
    participant D3 as Data Server 3

    C->>M: CreateFile("/path/file")
    M->>M: Allocate inode
    M->>M: Select placement
    M-->>C: OK (inode, placement)

    C->>C: Erasure encode data

    par Write shards
        C->>D1: WriteChunk(shard1)
        C->>D2: WriteChunk(shard2)
        C->>D3: WriteChunk(shard3)
    end

    D1-->>C: OK
    D2-->>C: OK
    D3-->>C: OK

    C->>M: CommitWrite(inode, chunks)
    M->>M: Raft replicate
    M-->>C: OK
```

### Read Path

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Metadata
    participant D1 as Data Server 1
    participant D2 as Data Server 2

    C->>M: Lookup("/path/file")
    M-->>C: Inode(chunks, locations)

    par Read shards
        C->>D1: ReadChunk(chunk1)
        C->>D2: ReadChunk(chunk2)
    end

    D1-->>C: shard1
    D2-->>C: shard2

    C->>C: Erasure decode
    C->>C: Return data
```

## Erasure Coding

Strata uses Reed-Solomon erasure coding for data durability with configurable data and parity shards.

```mermaid
graph LR
    subgraph "Original Data"
        OD[64MB Chunk]
    end

    subgraph "Erasure Encoded (4+2)"
        S1[Shard 1<br/>16MB]
        S2[Shard 2<br/>16MB]
        S3[Shard 3<br/>16MB]
        S4[Shard 4<br/>16MB]
        P1[Parity 1<br/>16MB]
        P2[Parity 2<br/>16MB]
    end

    OD -->|Encode| S1
    OD -->|Encode| S2
    OD -->|Encode| S3
    OD -->|Encode| S4
    OD -->|Encode| P1
    OD -->|Encode| P2
```

### Configuration Presets

| Preset | Data | Parity | Overhead | Fault Tolerance |
|--------|------|--------|----------|-----------------|
| Small Cluster | 2 | 1 | 1.5x | 1 node |
| Default | 4 | 2 | 1.5x | 2 nodes |
| Large Cluster | 8 | 4 | 1.5x | 4 nodes |
| Cost Optimized | 10 | 4 | 1.4x | 4 nodes |

## Cluster Management

### Failure Detection

Strata uses the Phi Accrual failure detector for accurate node health monitoring:

```mermaid
graph TB
    subgraph "Failure Detection"
        HB[Heartbeat Monitor]
        PH[Phi Calculator]
        TH[Threshold Check]

        HB -->|Timestamps| PH
        PH -->|Phi Value| TH
        TH -->|Suspected| FH[Failure Handler]
    end
```

### Self-Healing

When failures are detected, Strata automatically recovers:

```mermaid
stateDiagram-v2
    [*] --> Healthy
    Healthy --> Suspected: Phi > threshold
    Suspected --> Failed: Timeout
    Suspected --> Healthy: Heartbeat received
    Failed --> Recovering: Initiate recovery
    Recovering --> Healthy: Recovery complete
    Failed --> [*]: Node removed
```

**Recovery process:**
1. Detect failed node
2. Identify under-replicated chunks
3. Read available shards from healthy nodes
4. Reconstruct missing shards
5. Place reconstructed shards on healthy nodes
6. Update metadata

## Node Roles

Strata supports flexible node deployment:

| Role | Metadata | Data | Use Case |
|------|----------|------|----------|
| **Combined** | Yes | Yes | Small deployments |
| **Metadata** | Yes | No | Dedicated namespace |
| **Data** | No | Yes | Storage scaling |

## Network Ports

| Port | Service | Protocol |
|------|---------|----------|
| 9000 | Metadata/Raft | gRPC |
| 9001 | Data Server | gRPC |
| 9002 | S3 Gateway | HTTP |
| 9090 | Metrics | HTTP/Prometheus |

## Security Model

```mermaid
graph TB
    subgraph "Authentication"
        JWT[JWT Tokens]
        MTLS[mTLS]
    end

    subgraph "Authorization"
        ACL[POSIX ACLs]
        RBAC[Role-Based Access]
    end

    subgraph "Encryption"
        TLS[TLS in Transit]
        AES[AES-256-GCM at Rest]
    end

    JWT --> ACL
    MTLS --> RBAC
    ACL --> TLS
    RBAC --> AES
```

## See Also

- [Raft Consensus](raft.md) - Deep dive into Raft implementation
- [Data Storage](data-storage.md) - Chunk storage details
- [Cluster Management](cluster.md) - Node management and rebalancing
