# ⚡ Helix - Paxos-Powered Distributed Transaction Engine

**A cutting-edge distributed transaction platform combining Paxos consensus algorithms with atomic Two-Phase Commit for zero-downtime, Byzantine-resilient banking operations at scale.**

## Project Overview

This project implements a **fault-tolerant distributed transaction processing system** designed for a banking application that scales across multiple data centers (clusters). The system uses **consensus mechanisms** and **distributed coordination protocols** to ensure consistency and reliability in financial transactions, even in the presence of server failures.

### Key Problem Statement
In a distributed banking system, transactions may span multiple geographic locations (shards). The system must:
- Ensure **transaction atomicity** across shards (all-or-nothing semantics)
- Handle **server failures** gracefully without losing data
- Maintain **strong consistency** despite concurrent access
- Provide **high availability** and throughput

### Objectives
- ✅ Process **intra-shard transactions** (transactions within a single shard) using the **Paxos or Multi-Paxos consensus protocol**
- ✅ Process **cross-shard transactions** (transactions across multiple shards) using the **Two-Phase Commit (2PC) protocol**
- ✅ Ensure **fault tolerance** with a fail-stop failure model supporting up to N-1 failures in a cluster of N servers
- ✅ Implement **ACID properties**: Atomicity, Consistency, Isolation (via locks), and Durability (via WAL)
- ✅ Measure and optimize **performance metrics**: throughput (transactions/sec) and latency

## Features and Functionality

### Core Features

#### 1. **Data Sharding & Replication**
- **Horizontal Scalability**: Data is partitioned into shards, each managing a range of account IDs
- **Full Replication**: Each shard is replicated across all servers in its cluster (default: 3 servers per cluster)
- **Automatic Discovery**: Servers identify which shard they manage based on account ID ranges
- **Fail-Stop Resilience**: System tolerates up to N-1 server failures per cluster (for a cluster of N servers)

#### 2. **Consensus-Based Intra-Shard Transactions**
Transactions within a single shard use the **Paxos consensus algorithm**:
- **Proposer**: Client initiates transaction proposal
- **Acceptors**: Cluster servers vote on transaction acceptance
- **Learners**: Servers apply committed transactions
- **Guarantees**: 
  - All servers agree on the transaction outcome
  - Consistency despite concurrent proposals
  - Progress guaranteed when a majority of servers are alive

#### 3. **Coordinated Cross-Shard Transactions**
Transactions across multiple shards use the **Two-Phase Commit (2PC) protocol**:
- **Phase 1 - Prepare**: 
  - Client sends transaction to all involved shard clusters
  - Servers verify preconditions (sufficient balance, no conflicts)
  - Servers acquire locks and respond `PREPARED` or `ABORT`
  - Write-Ahead Logs (WAL) ensure durability
- **Phase 2 - Commit/Abort**:
  - If all shards prepared: Client sends `COMMIT` → Transaction applies
  - If any shard failed: Client sends `ABORT` → Locks released, transaction rolled back
- **Atomicity**: Either all shards commit or all abort (no partial commits)

#### 4. **Fault Tolerance & Durability**
- **Write-Ahead Logging (WAL)**: Transactions logged before application
- **Distributed Locks**: Prevent concurrent modification of same accounts
- **Automatic Failure Detection**: Servers detect peer failures via timeout
- **Transaction Persistence**: In-memory database with H2 backend persistence
- **Graceful Degradation**: System continues with reduced servers until majority lost

#### 5. **System Monitoring & Debugging**
- **PrintBalance**: Queries balance of a specific account across all servers
- **PrintDatastore**: Displays all committed transactions on each server
- **Performance Metrics**: 
  - Throughput: Transactions processed per second
  - Latency: End-to-end transaction response time
  - Success/Abort rates

## System Architecture

### Cluster & Shard Layout
The system uses a **3-shard architecture** with **3 servers per shard**:

```
┌─────────────────────────────────────────────────────────────┐
│                    Distributed Banking System                │
├─────────────┬─────────────┬─────────────┐
│  Cluster 1  │  Cluster 2  │  Cluster 3  │
│  (Shard D1) │  (Shard D2) │  (Shard D3) │
├──────┬──────┼──────┬──────┼──────┬──────┤
│ Srv1 │ Srv2 │ Srv4 │ Srv5 │ Srv7 │ Srv8 │  <- Each cluster has 3 servers
│ Srv3 │      │ Srv6 │      │ Srv9 │      │
└──────┴──────┴──────┴──────┴──────┴──────┘

Shard Mapping (for 3000 accounts):
- Cluster 1: Accounts 1-1000
- Cluster 2: Accounts 1001-2000
- Cluster 3: Accounts 2001-3000

Data Replication:
- All accounts in a shard replicated on all servers in that shard
- Consensus ensures all replicas stay synchronized
```

### Transaction Flow Diagram

#### Intra-Shard Transaction Flow
```
Client
  │
  └─→ Send Transaction to Cluster
        │
        ├─→ Paxos Proposer (Server 1)
        │    │
        │    ├─→ Propose to Acceptors (Servers 2, 3)
        │    │    │
        │    │    ├─→ Acceptor 1: Verify & Lock
        │    │    ├─→ Acceptor 2: Verify & Lock
        │    │    └─→ Promise responses
        │    │
        │    └─→ Accept Phase: All accept transaction
        │
        ├─→ Learners: Apply transaction to database
        │
        └─→ Return Success/Abort to Client
```

#### Cross-Shard Transaction Flow
```
Client: Transfer from Acc 500 (Cluster 1) to Acc 1500 (Cluster 2)
  │
  ├─→ PREPARE Phase
  │    │
  │    ├─→ Contact Cluster 1
  │    │    ├─→ Verify Acc 500 exists & has sufficient balance
  │    │    ├─→ Acquire lock on Acc 500
  │    │    └─→ Return PREPARED or ABORT
  │    │
  │    └─→ Contact Cluster 2
  │         ├─→ Verify Acc 1500 exists
  │         ├─→ Acquire lock on Acc 1500
  │         └─→ Return PREPARED or ABORT
  │
  └─→ COMMIT/ABORT Phase
       ├─→ If both PREPARED: Send COMMIT to both
       │    ├─→ C1: Deduct amount, log transaction
       │    └─→ C2: Add amount, log transaction
       │
       └─→ If any ABORT: Send ABORT to both
            ├─→ C1: Release lock on Acc 500
            └─→ C2: Release lock on Acc 1500
```

### Technology Stack
- **Language**: Java 17
- **Framework**: Spring Boot 3.3.5
- **Database**: H2 (in-memory with persistence)
- **ORM**: Spring Data JPA/Hibernate
- **Communication**: HTTP/REST APIs
- **Build Tool**: Maven
- **Serialization**: GSON (JSON)

### System Description

#### Architecture Overview
- **Clusters and Shards**:
  - **3 Clusters** managing **3 Shards** of accounts
  - **3 Servers per Cluster** for replication and fault tolerance
  - Default shard size: 1000 accounts per shard
  - **Shard mapping**:
    - Cluster 1 (C1): Accounts [1, 1000]
    - Cluster 2 (C2): Accounts [1001, 2000]
    - Cluster 3 (C3): Accounts [2001, 3000]

- **Transaction Types**:
  - **Intra-Shard**: Transactions within a single cluster (uses Paxos)
  - **Cross-Shard**: Transactions spanning multiple clusters (uses 2PC)

### Intra-Shard Transactions (Paxos Protocol)

#### How It Works
1. **Proposal Phase**: Client sends transaction request `(sender_id, receiver_id, amount)` to any server in the cluster
2. **Prepare Phase**: 
   - Proposer server sends prepare requests to all acceptors with a proposal number
   - Acceptors check if they've seen a higher proposal, respond with promise to not accept lower proposals
3. **Accept Phase**:
   - Proposer sends accept requests with the transaction details
   - Acceptors verify transaction preconditions (sufficient balance, no conflicts)
   - Acceptors acquire necessary locks before accepting
   - Acceptors respond with acceptance
4. **Commit Phase**:
   - Learners (all servers) apply the transaction to their local database
   - Release locks after transaction application
   - Return success response to client

#### Failure Handling
- **Quorum-based**: Requires majority of servers (2 out of 3) for consensus
- **Retries**: Client retries if proposal rejected (higher proposal number seen)
- **Timeout**: Transaction aborts if no response within timeout period

### Cross-Shard Transactions (Two-Phase Commit Protocol)

#### Phase 1: Prepare
1. **Client Initiates**: Identifies all shards involved in transaction
2. **Contact Shards**: Sends prepare request to contact server in each shard
3. **Server Processing**:
   - Verify transaction preconditions (sufficient balance, account existence)
   - Acquire distributed locks on affected accounts
   - Log transaction to Write-Ahead Log (WAL) before responding
   - Respond with `PREPARED` or `ABORT`
4. **Client Decision**:
   - If ALL shards respond `PREPARED`: Proceed to commit phase
   - If ANY shard responds `ABORT`: Proceed to abort phase

#### Phase 2: Commit/Abort
- **Commit Path** (all prepared):
  - Client sends `COMMIT` to all shards
  - Servers apply transaction to database
  - Release all locks
  - Log commit to WAL
  - Confirm to client
  
- **Abort Path** (any shard failed):
  - Client sends `ABORT` to all shards
  - Servers rollback prepared state
  - Release all locks
  - Confirm to client

#### Consistency Guarantees
- **Atomicity**: Transaction either commits on all shards or aborts on all
- **Isolation**: Distributed locks prevent concurrent access to same accounts
- **Durability**: WAL ensures recovery after failures
- **Consistency**: Transaction validated before prepare phase succeeds

## Implementation Details

### Configuration & Setup

#### Environment Variables & Configuration
The system is configured via `application.properties`:
```properties
# Server Port
server.port=8080

# Cluster Configuration
application.cluster=1              # Which cluster this server belongs to (1, 2, or 3)
application.shardsize=1000         # Number of accounts per shard
application.clustersize=3          # Servers per cluster
application.noofclusters=3         # Total number of clusters

# Database (H2 In-Memory)
spring.datasource.url=jdbc:h2:mem:testdb
spring.h2.console.enabled=true     # Enable H2 web console for debugging
```

#### Initial Data
- **3000 accounts** (IDs: 1-3000)
- **Each account initialized with 10 units** of currency
- Accounts distributed across 3 shards in 3 clusters

### API Endpoints

#### Server-to-Server Communication
The following REST APIs enable inter-server communication:

**Paxos Protocol Endpoints**:
```
POST /api/paxos/prepare    - Prepare phase of Paxos consensus
POST /api/paxos/accept     - Accept phase of Paxos consensus
POST /api/paxos/learn      - Learn (commit) transaction
```

**2PC Protocol Endpoints**:
```
POST /api/2pc/prepare      - Prepare phase of 2PC (lock & verify)
POST /api/2pc/commit       - Commit transaction across shards
POST /api/2pc/abort        - Abort transaction across shards
```

**Monitoring Endpoints**:
```
GET /api/balance/{accountId}   - Get account balance
GET /api/transactions          - Get all committed transactions
GET /api/performance           - Get throughput and latency metrics
```

See the included **Postman collection** (`Two Phase Commit Protocol with Paxos.postman_collection.json`) for complete API documentation and examples.

### Data Structures

#### Core Models
- **Account**: Stores account ID and balance
- **Transaction**: Represents a transfer with sender, receiver, amount, status
- **Message**: Encapsulates Paxos consensus messages
- **PrepareMessage**: Paxos prepare request
- **PrepareReply**: Paxos prepare response
- **SynchroniseReply**: 2PC synchronization response

#### Persistence
- **H2 Database**: In-memory database with optional file persistence
- **Repositories**: 
  - `AccountRepository`: CRUD operations for accounts
  - `TransactionRepository`: Query and store transaction history
- **Write-Ahead Log**: Transaction logged before application for durability

### Prescribed Conditions & Constraints

#### Transaction Validation
1. **Sender must exist**: Account ID must be in range [1, 3000]
2. **Receiver must exist**: Account ID must be in range [1, 3000]
3. **Sufficient balance**: Sender must have `amount ≥ transfer_amount`
4. **Positive amount**: Transfer amount must be > 0
5. **No self-transfer**: Sender ≠ Receiver

#### Failure Scenarios
- **Aborted on insufficient balance**: Sender doesn't have enough funds
- **Aborted on lock contention**: Another transaction is accessing same accounts
- **Aborted on consensus failure**: Less than majority of servers respond in time
- **Aborted on 2PC failure**: Any shard fails to prepare
- **Handled on server failure**: Remaining quorum continues processing

## Getting Started

### Prerequisites
- **Java 17** or higher
- **Maven 3.6+**
- **Git**
- Approximately **2-4 GB RAM** for running all 9 servers
- Port availability: 8080-8088 (for 9 server instances)

### Repository Setup
1. **Clone the repository**:
   ```bash
   git clone https://github.com/kallagoutham/2pc-paxos.git
   cd 2pc-paxos
   ```

2. **Navigate to project directory**:
   ```bash
   cd two-phase-protocol
   ```

3. **Build the project**:
   ```bash
   ./mvnw clean package
   ```
   Or on Windows:
   ```bash
   mvnw.cmd clean package
   ```

### Running the System

#### Option 1: Start Individual Server Instances
Each server runs as a standalone Spring Boot application on different ports:

```bash
# Terminal 1 - Start Cluster 1, Server 1 (Port 8080)
./mvnw spring-boot:run -Dspring-boot.run.arguments="--application.cluster=1"

# Terminal 2 - Start Cluster 1, Server 2 (Port 8081)
./mvnw spring-boot:run -Dspring-boot.run.arguments="--application.cluster=1 --server.port=8081"

# Terminal 3 - Start Cluster 1, Server 3 (Port 8082)
./mvnw spring-boot:run -Dspring-boot.run.arguments="--application.cluster=1 --server.port=8082"

# ... (repeat for Clusters 2 and 3 on ports 8083-8088)
```

#### Option 2: Use the Client Application
1. **Compile the client**:
   ```bash
   cd ../lab3_resources
   javac Client.java
   ```

2. **Run the client** (servers must be running):
   ```bash
   java Client
   ```

3. **Follow the interactive menu** to:
   - Submit transactions via CSV files
   - Check account balances
   - View transaction history
   - Measure system performance

### Testing with Provided Test Cases
Test CSV files are included in `lab3_resources/`:
- `lab3_test_cases.csv` - Basic intra-shard transactions
- `lab3_test_cases_1.csv` - Cross-shard transactions
- `lab3_test_cases_2.csv` - Mixed workload with failures
- `lab3_test_cases_33.csv` - Stress test scenarios

#### Test File Format
```csv
Set Number,Transactions,Live Servers,Contact Servers
1,"(21, 700, 2); (100, 501, 8)","[S1, S2, S4, S6, S8, S9]","[S1, S4, S8]"
2,"(702, 1301, 2); (1301, 1302, 3)","[S1, S2, S3, S5, S6, S8, S9]","[S3, S6, S8]"
```

**Field Descriptions**:
- **Set Number**: Test case identifier
- **Transactions**: List of transfers in format `(sender_id, receiver_id, amount)`
- **Live Servers**: Servers that are active (others are simulated as down)
- **Contact Servers**: Servers to initiate contact with (attempt leaders in Paxos)

## Testing & Validation

### Test Coverage

The system includes comprehensive test scenarios:

#### 1. **Basic Functionality Tests**
- [x] Intra-shard transactions (single cluster)
- [x] Cross-shard transactions (multiple clusters)
- [x] Account balance queries
- [x] Transaction history retrieval

#### 2. **Fault Tolerance Tests**
- [x] Single server failure scenarios
- [x] Multiple server failures
- [x] Server recovery and re-synchronization
- [x] Network partition simulation

#### 3. **Concurrency Tests**
- [x] Concurrent intra-shard transactions
- [x] Concurrent cross-shard transactions
- [x] Mixed workload (intra + cross-shard)
- [x] Lock contention and deadlock prevention

#### 4. **Failure Scenarios**
- [x] Transaction abort on insufficient balance
- [x] Transaction abort on lock contention
- [x] Transaction abort on consensus failure
- [x] Timeout handling and retries

#### 5. **Performance Tests**
- [x] Throughput measurement (transactions/second)
- [x] Latency measurement (response time)
- [x] Scalability with increasing workload
- [x] Success/abort rate analysis

### Running Unit Tests
```bash
./mvnw test
```

### Performance Benchmarking
Use the client application's "Performance" option to measure:
- **Throughput**: Average transactions processed per second
- **Latency**: Average response time per transaction
- **Success Rate**: Percentage of successful transactions
- **Abort Rate**: Percentage of aborted transactions and reasons

### Monitoring & Debugging

#### H2 Database Console
Access the in-memory database console:
```
http://localhost:8080/api/h2-console
```
- **JDBC URL**: `jdbc:h2:mem:testdb`
- **Username**: `sa`
- **Password**: `admin`

#### Logging
View server logs for:
- Paxos consensus details
- 2PC phase progression
- Lock acquisition/release
- Transaction commit/abort reasons

#### Health Check
```bash
curl http://localhost:8080/api/health
```

## Advanced Features

### Configurable System Parameters
The system supports multiple cluster and shard configurations:

```properties
# Default: 3 clusters × 3 servers = 9 total servers
application.noofclusters=3
application.clustersize=3
application.shardsize=1000

# Example: 5 clusters × 2 servers each
application.noofclusters=5
application.clustersize=2
application.shardsize=600
```

### Data Redistribution
- ✅ **Dynamic Shard Rebalancing**: Redistribute accounts across shards to balance load
- ✅ **Cluster Expansion**: Add new clusters without downtime
- ✅ **Account Migration**: Move accounts between shards while maintaining consistency

### Performance Optimizations
- **Batch Processing**: Multiple transactions can be proposed concurrently
- **Request Pipelining**: Reduce latency by pipelining prepare and accept phases
- **Async Processing**: Non-blocking I/O for server communication
- **Connection Pooling**: Reuse HTTP connections between servers

## Architecture Patterns & Design Decisions

### Why Paxos for Intra-Shard?
| Aspect | Paxos | Alternatives |
|--------|-------|--------------|
| **Consistency** | Strong consistency ✓ | Weaker models available |
| **Liveness** | Progress when majority online ✓ | May block entirely |
| **Simplicity** | Multi-round consensus | Single round may miss failures |
| **Proven** | Used by Google, Apache ✓ | Newer protocols less battle-tested |

### Why 2PC for Cross-Shard?
| Aspect | 2PC | Alternatives |
|--------|-----|--------------|
| **Atomicity** | True ACID guarantees ✓ | Eventual consistency only |
| **Simplicity** | Easy to implement ✓ | Sagas, events more complex |
| **Isolation** | Distributed locks ✓ | Conflict resolution needed |
| **Industry Standard** | Banking standard ✓ | Proven over decades |

### Trade-offs
- **Strong Consistency** vs. Availability: Chooses consistency (CP in CAP theorem)
- **Fault Tolerance** vs. Simplicity: Supports N-1 failures (needs quorum)
- **Performance** vs. Safety: Synchronous write-ahead logs ensure durability

## Limitations & Future Improvements

### Current Limitations
1. **Blocking Commits**: 2PC blocks until all shards respond (could be async)
2. **Manual Failure Handling**: Manually simulate server failures via CSV
3. **No Geo-Replication**: All servers must be in same network
4. **Limited Conflict Resolution**: No merge logic for conflicting transactions

### Potential Enhancements
- [ ] **3PC (Three-Phase Commit)**: Reduce blocking duration
- [ ] **Quorum Reads**: Load balance read-only queries across replicas
- [ ] **Multi-Paxos Optimization**: Combine multiple consensus rounds
- [ ] **Geo-Replication**: Support cross-region transaction processing
- [ ] **Sharding Hot-Spots**: Detect and rebalance popular accounts
- [ ] **Crash Recovery**: Automatic rebuilding from WAL
- [ ] **Monitoring Dashboard**: Real-time metrics visualization

## File Structure

```
2pc-paxos/
├── README.md                          # This file
├── lab3_resources/
│   ├── Client.java                    # Interactive client application
│   ├── lab3_test_cases*.csv           # Test case files
│   └── Two Phase Commit Protocol*.json # Postman API collection
└── two-phase-protocol/
    ├── pom.xml                        # Maven configuration
    ├── mvnw & mvnw.cmd               # Maven wrapper scripts
    └── src/
        ├── main/java/com/example/two_phase_protocol/
        │   ├── TwoPhaseProtocolApplication.java    # Entry point
        │   ├── Configuration/          # Spring configuration beans
        │   ├── Controllers/            # REST API endpoints
        │   │   ├── ServerController.java
        │   │   ├── TwoPhaseCommitController.java
        │   │   └── HelloController.java
        │   ├── Models/                # Data models (POJO)
        │   │   ├── Account.java
        │   │   ├── Transaction.java
        │   │   ├── Message.java
        │   │   └── ...
        │   ├── Repository/            # Data access layer
        │   │   ├── AccountRepository.java
        │   │   └── TransactionRepository.java
        │   ├── Services/              # Business logic
        │   │   ├── ServerService.java
        │   │   ├── TwoPhaseService.java
        │   │   └── PerformanceService.java
        │   ├── Utils/                 # Utility classes
        │   │   └── PeerUtils.java
        │   └── GlobalVariables/       # Shared state
        │       └── Variables.java
        ├── resources/
        │   ├── application.properties # Configuration
        │   └── banner.txt
        └── test/
            └── java/...               # Unit tests
```

## References & Learning Resources

### Key Papers & Protocols
- **Paxos Protocol**: 
  - Lamport, L. (1998). "The Part-Time Parliament"
  - Lamport, L. (2001). "Paxos Made Simple"
- **Two-Phase Commit**:
  - Gray, J., & Reuter, A. (1993). "Transaction Processing: Concepts and Techniques"
  - Skeen, D. (1981). "Nonblocking Commitment Protocols"
- **Distributed Systems**:
  - Tanenbaum, A. S., & van Steen, M. "Distributed Systems: Principles and Paradigms"

### External Resources
- [Paxos Consensus Algorithm Visualization](https://github.com/ongardie/raft.tla)
- [Two-Phase Commit Protocol Explained](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [H2 Database Documentation](https://www.h2database.com/)
- [GitHub Setup Instructions](https://docs.github.com/en/get-started/getting-started-with-git/set-up-git)
- [Maven Getting Started](https://maven.apache.org/guides/getting-started/)

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Port already in use | Another process on port 8080-8088 | Kill process: `lsof -i :8080` then `kill -9 <PID>` |
| H2 console won't connect | Database not initialized | Start server first, then access console |
| Transaction timeout | Slow network or server overload | Increase timeout in `application.properties` |
| Consensus failure | Fewer than 2 servers responding | Ensure at least 2 servers per cluster are running |
| 2PC abort | One shard can't prepare | Check account balances and locks |
| Memory exhaustion | Too many concurrent transactions | Reduce batch size or increase heap: `export MAVEN_OPTS="-Xmx4g"` |

### Debug Mode
Enable verbose logging by adding to `application.properties`:
```properties
logging.level.com.example.two_phase_protocol=DEBUG
logging.level.org.springframework=INFO
```

## Contributing

This project was developed as part of a distributed systems course. Contributions are welcome!

### To Contribute:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/improvement`)
5. Open a Pull Request

---

## About the Author

**Kalla Goutham**  
- 🌐 **Website**: [gouthamkalla.netlify.app](https://gouthamkalla.netlify.app/)
- 💼 **LinkedIn**: [linkedin.com/in/goutham-kalla-3b6133112](https://www.linkedin.com/in/goutham-kalla-3b6133112/)
- 🔗 **GitHub**: [github.com/kallagoutham](https://github.com/kallagoutham)
- ✉️ **Email**: kallagoutham33@gmail.com

---

## License

This project is provided for educational purposes. Please refer to any included license file for usage terms.

## Acknowledgments

- Acknowledgment to the course instructors and teaching assistants
- References to the academic papers that form the theoretical foundation
- The open-source community for Spring Boot, H2 Database, and other libraries