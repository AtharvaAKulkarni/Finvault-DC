# FinVault — Experiment 6: Load Balancing Demonstration Guide

This guide walks through the demonstration of **Experiment 6: Implementation of a Load Balancing Algorithm**.

> **Before you begin:** The commands and expected log messages below assume your repository uses the service/container names shown here. Confirm names in `docker-compose.yml` and adjust commands if your setup differs. Expected log lines are examples, not guaranteed exact output.

## Objectives

1. **Write routing:** `processTransaction` requests go to the current `PRIMARY` node.
2. **Read balancing:** `getBalance` requests are distributed across healthy `PRIMARY` and `SECONDARY` nodes.
3. **Client abstraction:** `FinanceClient` connects to the load balancer through RMI on port `8000`, rather than selecting a backend node itself.
4. **Strategy selection:** The load balancer supports `ROUND_ROBIN` and `LEAST_CONNECTIONS`, if implemented in the current build.
5. **Failover:** After primary failure and a successful election, the load balancer routes writes to the newly elected primary.

## Prerequisites

- Docker and Docker Compose installed.
- The FinVault project and its `docker-compose.yml` available locally.
- Java installed if you run `FinanceClient` directly on the host.
- The RMI registry/service configured to be reachable on port `8000`.
- A client command that matches your project’s actual build/run setup.

Check the available Compose services:

```bash
docker compose config --services
```

If your installation uses the older standalone Compose command, use `docker-compose` instead of `docker compose`.

## Step 1 — Build and launch the cluster

From the directory containing `docker-compose.yml`, run:

```bash
docker compose down
docker compose up --build
```

Keep this terminal open to watch startup logs. In a separate terminal, you can check service status:

```bash
docker compose ps
```

Look for the clock/master service, the finance server nodes, the replication manager, and the load balancer. The expected initial roles are:

- `finance-server-1`: `PRIMARY`
- `finance-server-2`: `SECONDARY`
- `load-balancer`: listening for RMI requests on port `8000`

Actual startup order and role assignment depend on your implementation and election logic.

## Step 2 — Open log-monitoring terminals

Open separate terminal tabs/windows and run:

**Terminal 2 — Load balancer**

```bash
docker compose logs -f load-balancer
```

**Terminal 3 — Finance server 1**

```bash
docker compose logs -f finance-server-1
```

**Terminal 4 — Finance server 2**

```bash
docker compose logs -f finance-server-2
```

Optional: monitor the replication manager in another terminal:

```bash
docker compose logs -f finvault-replication-manager
```

If a service name differs, use the exact name shown by `docker compose config --services`.

## Step 3 — Run the client request suite

Run the client using the command supported by your project. If `FinanceClient.class` has already been compiled and its dependencies are available on the classpath, this may be:

```bash
java FinanceClient
```

If the client is part of a Maven/Gradle project or runs inside a container, use the project’s documented command instead. Confirm that the client connects to the load balancer’s RMI endpoint on port `8000`.

## Step 4 — Verify write routing

Run the client suite and locate transaction requests in the load-balancer logs.

Example expected behavior:

```text
[Load Balancer] WRITE -> Routing processTransaction to PRIMARY (Node 1)
[Load Balancer] WRITE -> Routing processTransaction to PRIMARY (Node 1)
[Load Balancer] WRITE -> Routing processTransaction to PRIMARY (Node 1)
```

Verify that the corresponding transaction requests reach `finance-server-1` while it is the primary. The secondary may receive replication traffic, but it should not receive direct client write requests if your architecture requires primary-only writes.

**Explain to the professor:** Transactions modify the ledger, so writes are routed to the current primary to preserve a single authoritative write path. Replication then propagates the committed state to the secondary according to the implementation.

## Step 5 — Verify read balancing

Run the client suite and look at `getBalance` routing in the load-balancer logs.

Example for round-robin routing:

```text
[Load Balancer] READ (ROUND_ROBIN) -> Routing getBalance(ACC101) to Node 1
[Load Balancer] READ (ROUND_ROBIN) -> Routing getBalance(ACC102) to Node 2
[Load Balancer] READ (ROUND_ROBIN) -> Routing getBalance(ACC103) to Node 1
[Load Balancer] READ (ROUND_ROBIN) -> Routing getBalance(ACC104) to Node 2
```

The exact order depends on the round-robin counter’s starting position and the sequence of client calls. Confirm that requests alternate across eligible healthy nodes over repeated calls.

**Explain to the professor:** Balance inquiries are read-only, so they can be spread across healthy nodes to distribute workload. This is safe only if the replica’s data freshness is acceptable for the demonstration.

## Step 6 — Switch the balancing strategy

Open `docker-compose.yml` and set the load balancer’s environment variable to the strategy your code supports:

```yaml
services:
  load-balancer:
    environment:
      - LB_STRATEGY=LEAST_CONNECTIONS
```

Preserve any existing environment variables and indentation in your file. If `LB_STRATEGY` already exists, change its value rather than adding a duplicate.

Recreate the load-balancer container so the updated environment is applied:

```bash
docker compose up -d --force-recreate load-balancer
docker compose logs -f load-balancer
```

Run the client suite again and inspect the logs. You may see output such as:

```text
[Load Balancer] Strategy: LEAST_CONNECTIONS
[Load Balancer] READ (LEAST_CONNECTIONS) -> Routing getBalance(ACC101) to Node 2
```

The selected node is implementation- and load-dependent. Do not expect Node 2 specifically unless the current connection counts and selection logic make that result deterministic.

**Important:** Least-connections is meaningful only if the load balancer tracks active/in-flight connections or requests. If the implementation merely picks a node without tracking load, verify or implement that behavior before claiming the algorithm is demonstrated.

To return to round-robin, change the setting back:

```yaml
- LB_STRATEGY=ROUND_ROBIN
```

Then recreate the load-balancer container again.

## Step 7 — Demonstrate primary failure and failover

> **Caution:** This step intentionally stops a server. Use a test environment and avoid doing this during any demonstration involving real data.

First confirm that `finance-server-1` is the current primary. Then stop its container:

```bash
docker stop finvault-server-1
```

The name passed to `docker stop` must be the actual **container name**, which may differ from the Compose service name. Check with:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Watch the replication-manager and surviving-server logs. Confirm that the election process detects the failure and promotes `finance-server-2` to `PRIMARY`. Do not assume promotion succeeded until the logs or status output confirm it.

Now run the client request suite again. Example expected routing:

```text
[Load Balancer] WRITE -> Routing processTransaction to PRIMARY (Node 2)
[Load Balancer] READ (ROUND_ROBIN) -> Routing getBalance(ACC101) to Node 2
```

If the load balancer does not discover the new primary automatically, inspect its discovery/health-check mechanism and refresh interval. The failover claim is verified only when a client write succeeds against the new primary without requiring the client to change its endpoint.

Restore the cluster after the demonstration:

```bash
docker compose up -d
docker compose ps
```

Check the logs to confirm the intended roles and replication state have recovered.

## Troubleshooting

- **Load balancer cannot find a backend:** Check service names, network configuration, backend hostnames, and RMI registry bindings.
- **Client cannot connect to port 8000:** Confirm the load balancer is running, the port is published if the client runs on the host, and RMI host/port settings are correct.
- **No read distribution:** Confirm `getBalance` is routed through the load balancer and both nodes are healthy/eligible.
- **Least-connections always picks one node:** Check whether active request counts are tracked and decremented after requests complete.
- **Failover does not occur:** Inspect election and heartbeat logs; verify that the surviving node is promoted and that the load balancer refreshes primary information.
- **Command says service/container not found:** Compare the service names from `docker compose config --services` with container names from `docker ps`.

## Professor evaluation checklist

| Objective | What to demonstrate | Verified |
|---|---|---|
| Client abstraction | Client calls the load balancer via RMI on port `8000` | [ ] |
| Write consistency | `processTransaction` routes to the current primary | [ ] |
| Read balancing | `getBalance` requests use eligible healthy nodes | [ ] |
| Algorithm selection | Round-robin and least-connections work as implemented | [ ] |
| Fault tolerance | Writes move to the elected primary after failover | [ ] |
| Recovery | Cluster is restored and roles/replication are checked | [ ] |

## Short viva explanation

“FinVault uses a load balancer as the client-facing RMI endpoint. Write operations are routed to the current primary to maintain one authoritative write path, while read operations can be distributed across healthy nodes using a configurable balancing strategy. When the primary fails, the election mechanism selects a new primary, and the load balancer must refresh its routing information so the client can continue using the same endpoint.”

---

**Final note:** Treat the log lines in this guide as illustrative. During the demonstration, point to the actual logs from your running implementation as evidence.
