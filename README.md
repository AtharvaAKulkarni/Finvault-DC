# FinVault

## Design and Implementation of a Fault-Tolerant Distributed Financial Transaction Management System

FinVault is a distributed financial transaction management system designed to demonstrate important Distributed Systems concepts in a finance-oriented environment.

The project uses multiple independent Finance Server nodes communicating through Java RMI. Docker is used to simulate a distributed environment, while Cristian's Algorithm is used for physical clock synchronization and Lamport's Logical Clock is used for maintaining the ordering and causality of distributed events.

The system also demonstrates concurrent transaction processing using Java multithreading and synchronization.

---

## Features

- Distributed Finance Server nodes using Docker
- Java RMI based client-server communication
- Physical clock synchronization using Cristian's Algorithm
- RTT-based network delay estimation
- Logical clock synchronization using Lamport's Algorithm
- Concurrent transaction processing using a thread pool
- Thread-safe financial transfers
- Transaction IDs and synchronized timestamps
- Transaction validation
- Basic fraud-check simulation
- Transaction logging
- Customer notification simulation
- Docker Compose based distributed deployment

---

## Technology Stack

- **Java 17+**
- **Java RMI**
- **Docker**
- **Docker Compose**
- **Java Concurrency (`ExecutorService`)**
- **Lamport Logical Clock**
- **Cristian's Clock Synchronization Algorithm**

Java 17+ and Java RMI are part of the project's planned technology stack, along with concurrent utilities and custom logical/physical clock mechanisms. :contentReference[oaicite:1]{index=1}

---

## System Architecture

The current implementation consists of:

- **Clock Master** – provides the reference physical time and maintains its own Lamport clock.
- **Finance Server 1**
- **Finance Server 2**
- **Finance Server 3**
- **Finance Client** – sends financial transaction requests to a Finance Server.

Docker Compose places all services on the same Docker network, allowing the containers to communicate using service names.

Each Finance Server has a deliberately different simulated clock offset so that clock synchronization can be demonstrated.

---

## Clock Synchronization

### Cristian's Algorithm

The Clock Master acts as the reference time server.

Each Finance Server initially has a simulated clock drift. For example, one server may be several seconds ahead while another may be several seconds behind.

The synchronization process is:

1. The Finance Server records `T1` before sending the synchronization request.
2. The request is sent to the Clock Master using Java RMI.
3. The Clock Master records its current physical time and sends it back.
4. The Finance Server records `T2` after receiving the response.
5. Round Trip Time is calculated as:

   `RTT = T2 - T1`

6. The one-way network delay is approximated as:

   `RTT / 2`

7. The estimated current Clock Master time is calculated using the server time plus half of the RTT.
8. The Finance Server calculates the required clock adjustment.
9. The adjustment is applied to its simulated clock.

This allows servers with different initial clock offsets to converge approximately to the master's physical time.

---

## Lamport Logical Clock

Physical clocks are not sufficient for determining the causal ordering of events in a distributed system.

FinVault therefore maintains a Lamport logical clock on the Finance Servers and Clock Master.

The Lamport rules used are:

- **Local event:** increment the logical clock.
- **Send event:** increment the logical clock before sending a message.
- **Receive event:**  
  `L = max(local_clock, received_clock) + 1`

The Lamport timestamp is exchanged as part of the clock synchronization request and response.

This allows distributed events to be ordered logically even when physical clocks are not perfectly synchronized.

---

## RTT-Based Synchronization

Unlike a simple clock synchronization implementation that assumes zero communication delay, FinVault measures the communication delay between the Finance Server and Clock Master.

The RTT measurement makes the synchronization demonstration more realistic because network communication takes time.

The estimated server time is therefore based on:

`Estimated Master Time = Master Time + RTT / 2`

This estimated time is then used to calculate the clock correction.

---

## Financial Transaction Processing

After clock synchronization, Finance Servers can process financial transactions.

A client sends a transaction request through Java RMI containing:

- Customer ID
- Source account
- Destination account
- Transaction amount

The Finance Server:

1. Generates a unique transaction ID.
2. Records the synchronized physical timestamp.
3. Assigns the transaction to a worker thread.
4. Validates the transaction.
5. Performs a fraud-check simulation.
6. Transfers money between accounts.
7. Records the transaction.
8. Sends a notification.
9. Reports transaction completion.

The current server uses a fixed thread pool of five worker threads for concurrent transaction processing. :contentReference[oaicite:2]{index=2}

---

## Concurrency and Thread Safety

Multiple financial transactions can be processed concurrently using Java's `ExecutorService`.

Since multiple transactions may access the same account balances simultaneously, the actual balance transfer operation is synchronized.

This prevents two concurrent transactions from incorrectly modifying shared account state at the same time. :contentReference[oaicite:3]{index=3}

---

## Docker Setup

Docker is used to simulate multiple distributed machines on a single physical computer.

The deployment contains:

- `clock-master`
- `finance-server-1`
- `finance-server-2`
- `finance-server-3`

Each Finance Server receives a different `CLOCK_DRIFT_MS` value through Docker Compose.

Example:

- Server 1: `+5000 ms`
- Server 2: `-3000 ms`
- Server 3: `+8000 ms`

These offsets intentionally make the physical clocks different so that Cristian's Algorithm can be observed in the container logs.

---

## Project Structure

```text
FinVault/
│
├── FinanceClient.java
├── FinanceServer.java
├── FinanceService.java
│
├── ClockService.java
├── ClockMasterServer.java
├── ClockResponse.java
├── LamportClock.java
│
├── Dockerfile
├── docker-compose.yml
│
└── README.md
