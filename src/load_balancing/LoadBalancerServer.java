package load_balancing;

import replication.NodeRole;
import replication.ReplicationService;

import java.lang.reflect.Method;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancerServer
        extends UnicastRemoteObject
        implements LoadBalancerService {

    private static final int PORT = 8000;

    private final Map<Integer, String> nodes = new LinkedHashMap<>();
    private final Map<Integer, AtomicInteger> activeConnections = new ConcurrentHashMap<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private LoadBalancingStrategy strategy;

    public LoadBalancerServer() throws RemoteException {
        super(PORT);

        // Register cluster nodes from environment variables
        nodes.put(1, System.getenv().getOrDefault("NODE1_HOST", "finance-server-1"));
        nodes.put(2, System.getenv().getOrDefault("NODE2_HOST", "finance-server-2"));
        nodes.put(3, System.getenv().getOrDefault("NODE3_HOST", "finance-server-3"));
        nodes.put(4, System.getenv().getOrDefault("NODE4_HOST", "finance-server-4"));

        for (int id : nodes.keySet()) {
            activeConnections.put(id, new AtomicInteger(0));
        }

        String strategyEnv = System.getenv().getOrDefault("LB_STRATEGY", "ROUND_ROBIN");
        try {
            strategy = LoadBalancingStrategy.valueOf(strategyEnv.toUpperCase());
        } catch (IllegalArgumentException e) {
            strategy = LoadBalancingStrategy.ROUND_ROBIN;
        }

        System.out.println();
        System.out.println("============================================");
        System.out.println("      FINVAULT LOAD BALANCER SERVER        ");
        System.out.println("============================================");
        System.out.println("[Load Balancer] Strategy: " + strategy);
        System.out.println("[Load Balancer] Registered Nodes: " + nodes);
    }

    @Override
    public String createAccount(String accountNumber, double initialBalance) throws RemoteException {
        int primaryNodeId = getPrimaryNodeId();
        if (primaryNodeId == -1) {
            return "Creation rejected: No active Primary available.";
        }

        System.out.println("[Load Balancer] WRITE -> Routing createAccount to PRIMARY (Node " + primaryNodeId + ")");

        activeConnections.get(primaryNodeId).incrementAndGet();
        try {
            Remote stub = getFinanceServerStub(primaryNodeId);
            Method method = stub.getClass().getMethod("createAccount", String.class, double.class);
            return (String) method.invoke(stub, accountNumber, initialBalance);
        } catch (Exception e) {
            throw new RemoteException("Load Balancer failed write query on Node " + primaryNodeId + ": " + e.getMessage(), e);
        } finally {
            activeConnections.get(primaryNodeId).decrementAndGet();
        }
    }


    /*
     * ===================================================
     * WRITE TRANSACTION -> Always routed to PRIMARY
     * ===================================================
     */
    @Override
    public String processTransaction(
            String customerId,
            String fromAccount,
            String toAccount,
            double amount)
            throws RemoteException {

        int primaryNodeId = getPrimaryNodeId();
        if (primaryNodeId == -1) {
            return "Transaction rejected: No active Primary available.";
        }

        System.out.println("[Load Balancer] WRITE -> Routing processTransaction to PRIMARY (Node " + primaryNodeId + ")");

        activeConnections.get(primaryNodeId).incrementAndGet();
        try {
            Remote stub = getFinanceServerStub(primaryNodeId);
            Method method = stub.getClass().getMethod(
                    "processTransaction", String.class, String.class, String.class, double.class);
            return (String) method.invoke(stub, customerId, fromAccount, toAccount, amount);
        } catch (Exception e) {
            throw new RemoteException("Load Balancer failed write query on Node " + primaryNodeId + ": " + e.getMessage(), e);
        } finally {
            activeConnections.get(primaryNodeId).decrementAndGet();
        }
    }

    /*
     * ===================================================
     * READ QUERY -> Balanced across healthy nodes
     * ===================================================
     */
    @Override
    public double getBalance(String accountNumber) throws RemoteException {
        int targetNodeId = (strategy == LoadBalancingStrategy.LEAST_CONNECTIONS)
                ? selectNodeLeastConnections()
                : selectNodeRoundRobin();

        if (targetNodeId == -1) {
            throw new RemoteException("No healthy Finance Servers available for read query.");
        }

        System.out.println("[Load Balancer] READ (" + strategy + ") -> Routing getBalance(" + accountNumber + ") to Node " + targetNodeId);

        activeConnections.get(targetNodeId).incrementAndGet();
        try {
            Remote stub = getFinanceServerStub(targetNodeId);
            Method method = stub.getClass().getMethod("getBalance", String.class);
            return (Double) method.invoke(stub, accountNumber);
        } catch (Exception e) {
            System.out.println("[Load Balancer] Read query failed on Node " + targetNodeId + ": " + e.getMessage());
            throw new RemoteException("Read failed on Node " + targetNodeId + ": " + e.getMessage(), e);
        } finally {
            activeConnections.get(targetNodeId).decrementAndGet();
        }
    }

    /*
     * ---------------------------------------------------
     * ROUND-ROBIN ALGORITHM
     * ---------------------------------------------------
     */
    private int selectNodeRoundRobin() {
        List<Integer> healthyNodes = getHealthyNodeIds();
        if (healthyNodes.isEmpty()) return -1;

        int index = Math.abs(roundRobinIndex.getAndIncrement() % healthyNodes.size());
        return healthyNodes.get(index);
    }

    /*
     * ---------------------------------------------------
     * LEAST CONNECTIONS ALGORITHM
     * ---------------------------------------------------
     */
    private int selectNodeLeastConnections() {
        List<Integer> healthyNodes = getHealthyNodeIds();
        if (healthyNodes.isEmpty()) return -1;

        int selectedNode = healthyNodes.get(0);
        int minConnections = activeConnections.get(selectedNode).get();

        for (int nodeId : healthyNodes) {
            int conns = activeConnections.get(nodeId).get();
            if (conns < minConnections) {
                minConnections = conns;
                selectedNode = nodeId;
            }
        }
        return selectedNode;
    }

    /*
     * ---------------------------------------------------
     * CLUSTER HEALTH & PRIMARY DISCOVERY
     * ---------------------------------------------------
     */
    private List<Integer> getHealthyNodeIds() {
        List<Integer> healthy = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : nodes.entrySet()) {
            try {
                ReplicationService service = getReplicationService(entry.getKey());
                if (service.ping()) {
                    healthy.add(entry.getKey());
                }
            } catch (Exception ignored) {
                // Node unavailable
            }
        }
        return healthy;
    }

    private int getPrimaryNodeId() {
        for (int nodeId : nodes.keySet()) {
            try {
                ReplicationService server = getReplicationService(nodeId);
                if (server.getRole() == NodeRole.PRIMARY) {
                    return nodeId;
                }
            } catch (Exception ignored) {}
        }
        return -1;
    }

    private Remote getFinanceServerStub(int nodeId) throws Exception {
        String host = nodes.get(nodeId);
        return Naming.lookup("rmi://" + host + ":1234/FinanceServer");
    }

    private ReplicationService getReplicationService(int nodeId) throws Exception {
        String host = nodes.get(nodeId);
        return (ReplicationService) Naming.lookup("rmi://" + host + ":1234/replication.ReplicationService");
    }

    /*
     * ---------------------------------------------------
     * MAIN METHOD
     * ---------------------------------------------------
     */
    public static void main(String[] args) {
        try {
            System.setProperty(
                    "java.rmi.server.hostname",
                    System.getenv().getOrDefault("RMI_HOSTNAME", "load-balancer")
            );

            LoadBalancerServer lb = new LoadBalancerServer();
            Registry registry = LocateRegistry.createRegistry(PORT);

            registry.rebind("LoadBalancer", lb);

            System.out.println("Load Balancer RMI Registry started on port " + PORT + ".");
            System.out.println("Registered as: LoadBalancer");
            System.out.println("Waiting for client requests...");

            Thread.currentThread().join();

        } catch (Exception e) {
            System.out.println("Load Balancer Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}