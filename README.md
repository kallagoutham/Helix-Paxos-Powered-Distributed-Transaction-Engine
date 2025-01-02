# Fault-Tolerant Distributed Transaction Processing

## Project Overview
This project involves implementing a fault-tolerant distributed transaction processing system that supports intra-shard and cross-shard transactions for a simple banking application. The system partitions servers into clusters, where each cluster manages a shard of data replicated across its servers.

### Objectives
- Process intra-shard transactions using a modified Paxos or Multi-Paxos protocol.
- Process cross-shard transactions using the two-phase commit (2PC) protocol.
- Ensure fault tolerance and consistency despite server failures.
- Optimize performance in terms of throughput and latency.

## Features and Functionality

### Core Features
1. **Shard Management**:
   - Data is divided into shards, each managed by a cluster of servers.
   - Each shard is replicated across all servers in its cluster for fault tolerance.
2. **Intra-Shard Transactions**:
   - Transactions within a shard are processed using a consensus mechanism.
   - Modified Paxos or Multi-Paxos is used to achieve agreement.
3. **Cross-Shard Transactions**:
   - Transactions across shards use the two-phase commit protocol.
   - Clients act as coordinators for the protocol.
4. **Fault Tolerance**:
   - Servers adhere to a fail-stop failure model.
   - Transactions are aborted in case of insufficient balance, lock contention, or consensus failure.

### Additional Functions
- **PrintBalance**: Retrieves the balance of a specified client across all servers.
- **PrintDatastore**: Outputs the set of committed transactions on each server.
- **Performance Metrics**: Measures throughput (transactions per second) and latency.

## System Description

### Architecture
- **Clusters and Shards**:
  - Data is divided into three shards: D1, D2, and D3.
  - Each shard is managed by a cluster of three servers.
  - Shard mapping:
    - C1: [1, 2, ..., 1000]
    - C2: [1001, ..., 2000]
    - C3: [2001, ..., 3000]
- **Transaction Types**:
  - **Intra-Shard Transactions**: Access data within a single shard.
  - **Cross-Shard Transactions**: Access data across multiple shards.

### Intra-Shard Transactions
- A client sends a transaction request `(x, y, amt)` to a server in the relevant cluster.
- The server initiates consensus using the modified Paxos or Multi-Paxos protocol.
- Consensus ensures that all servers in the cluster agree on the transaction.
- Locks are acquired during the accept phase, and transactions are committed if conditions are met.

### Cross-Shard Transactions
- A client sends the request to the contact servers of the involved shards.
- Two-phase commit protocol:
  1. **Prepare Phase**:
     - Contact servers verify conditions and acquire locks.
     - Servers send `PREPARED` or `ABORT` messages to the client.
  2. **Commit Phase**:
     - If all shards prepare successfully, the client sends `COMMIT` messages.
     - Otherwise, the client sends `ABORT` messages.
- Write-ahead logs (WAL) are maintained to support undo operations in case of abort.

## Implementation Details

### Prescribed Conditions
- Dataset: 3000 data items, all initialized with 10 units.
- System should:
  - Support intra-shard and cross-shard transactions.
  - Abort transactions when conditions are not met.

### Functions
1. **PrintBalance(Client ID)**: Displays the balance of the specified client.
2. **PrintDatastore()**: Outputs all committed transactions on each server.
3. **Performance()**: Reports throughput and latency.

### Communication Among Servers
  I have used TCP/HTTP for communication among servers. I am providing postman collection file of API endpoints that I developed for the communication among Paxos Servers under lab3_resources.

### Input Format
- Input files should be CSV files with columns:
  1. **Set Number**: Identifier for the test case.
  2. **Transactions**: List of transactions `(Sender, Receiver, Amount)`.
  3. **Live Servers**: Active servers for the test case.
  4. **Contact Servers**: Contact servers attempting to become leaders.

#### Example Input
```csv
Set Number, Transactions, Live Servers, Contact Servers
1, (21, 700, 2); (100, 501, 8), [S1, S2, S4, S6, S8, S9], [S1, S4, S8]
2, (702, 1301, 2); (1301, 1302, 3), [S1, S2, S3, S5, S6, S8, S9], [S3, S6, S8]
```

## Setup Instructions

### Repository Setup
1. Clone the GitHub repository:
   ```bash
   https://github.com/kallagoutham/2pc-paxos.git
   ```
2. Compile Client.java and run Client.class file in lab2_resources. Then a list of options will be appeared select appropriate option to run and evaluate results.

### Running the Program
1. Prepare an input file with the required transaction format.
2. Execute the program to process transactions sequentially.
3. Use functions (`PrintBalance`, `PrintDatastore`) to monitor system states.

## Testing

### Test Cases
1. Valid intra-shard transactions.
2. Valid cross-shard transactions.
3. Concurrent intra-shard and cross-shard transactions.
4. Fault tolerance scenarios.
5. Abort scenarios (e.g., insufficient balance, lock contention).

### Example Input File
```csv
Set Number, Transactions, Live Servers, Contact Servers
1, (21, 700, 2); (100, 501, 8), [S1, S2, S4, S6, S8, S9], [S1, S4, S8]
2, (702, 1301, 2); (1301, 1302, 3), [S1, S2, S3, S5, S6, S8, S9], [S3, S6, S8]
```

## Bonus Features
- [x] **Shard Redistribution**:
   - Dynamically redistribute data items to optimize performance.
- [x] **Configurable Clusters**:
   - Allow custom configurations for the number of clusters and servers per cluster.


## References
- Paxos and Multi-Paxos protocol resources.
- Two-Phase Commit protocol documentation.
- [GitHub Setup Instructions](https://docs.github.com/en/get-started/getting-started-with-git/set-up-git).

👨‍💻 **Kalla Goutham**    
🌐 [Website](https://gouthamkalla.netlify.app/) | [LinkedIn](https://www.linkedin.com/in/goutham-kalla-3b6133112/) | [GitHub](https://github.com/kallagoutham)  
✉️ Reach me at: kallagoutham33@gmail.com
