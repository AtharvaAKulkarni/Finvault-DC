import clock_synchronisation.ClockResponse;
import clock_synchronisation.ClockService;
import clock_synchronisation.LamportClock;
import replication.AccountStateSnapshot;
import replication.NodeRole;
import replication.ReplicationResult;
import replication.ReplicationService;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.Naming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Future;

public class FinanceServer
        extends UnicastRemoteObject
        implements FinanceService, ReplicationService {

    private static final int THREAD_POOL_SIZE = 5;

    private volatile NodeRole role;
    private final int nodeId;
    private volatile int currentPrimaryId;
    private volatile boolean electionInProgress = false;

    private final long clockDrift;
    private long clockOffset = 0;
    private final LamportClock lamportClock = new LamportClock();

    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private final AtomicLong transactionCounter = new AtomicLong(1000);
    private final Map<String, Double> accountBalances = new ConcurrentHashMap<>();
    private final Object replicationLock = new Object();
    private final String replicationManagerHost;

    private String dbUrl;

    /*
     * ===================================================
     * DATABASE INITIALISATION
     * ===================================================
     */
    private void initDatabase() {
        // Explicitly register SQLite JDBC driver
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.err.println("[SQLite Error] SQLite JDBC Driver not found on classpath: " + e.getMessage());
            return;
        }
        this.dbUrl = "jdbc:sqlite:finance_node_" + nodeId + ".db";
        System.out.println("[SQLite] Initializing local database: finance_node_" + nodeId + ".db");

        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE IF NOT EXISTS accounts (" +
                    "account_number TEXT PRIMARY KEY, " +
                    "balance REAL NOT NULL)");

            stmt.execute("CREATE TABLE IF NOT EXISTS transaction_history (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "customer_id TEXT, " +
                    "from_acc TEXT, " +
                    "to_acc TEXT, " +
                    "amount REAL, " +
                    "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM accounts");
            if (rs.next() && rs.getInt(1) == 0) {
                stmt.execute("INSERT INTO accounts VALUES ('ACC101', 50000.0)");
                stmt.execute("INSERT INTO accounts VALUES ('ACC102', 35000.0)");
                stmt.execute("INSERT INTO accounts VALUES ('ACC103', 70000.0)");
                stmt.execute("INSERT INTO accounts VALUES ('ACC104', 45000.0)");
                System.out.println("[SQLite] Initial accounts seeded successfully.");
            }

            // Sync in-memory map from SQLite DB state
            ResultSet rsAll = stmt.executeQuery("SELECT account_number, balance FROM accounts");
            while (rsAll.next()) {
                accountBalances.put(rsAll.getString("account_number"), rsAll.getDouble("balance"));
            }

        } catch (SQLException e) {
            System.err.println("[SQLite Error] Failed to initialize database: " + e.getMessage());
        }
    }

    /*
    *  ==================================================
    *  CREATE ACCOUNT
    *  ==================================================
    * */
    /*
     * ===================================================
     * CREATE NEW ACCOUNT (PRIMARY)
     * ===================================================
     */
    @Override
    public synchronized String createAccount(String accountNumber, double initialBalance) throws RemoteException {
        if (role != NodeRole.PRIMARY) {
            return "Creation rejected: Node " + nodeId + " is SECONDARY.";
        }

        if (accountBalances.containsKey(accountNumber)) {
            return "Creation failed: Account " + accountNumber + " already exists.";
        }

        if (initialBalance < 0) {
            return "Creation failed: Initial balance cannot be negative.";
        }

        // 1. Write to Primary SQLite DB
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO accounts (account_number, balance) VALUES (?, ?)")) {
                pstmt.setString(1, accountNumber);
                pstmt.setDouble(2, initialBalance);
                pstmt.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            return "Creation failed (SQLite): " + e.getMessage();
        }

        // 2. Update Primary in-memory map
        accountBalances.put(accountNumber, initialBalance);
        System.out.println("[SQLite & Memory] Account created locally on Node " + nodeId + ": " + accountNumber + " (Rs. " + initialBalance + ")");

        // 3. Replicate across cluster via Replication Manager
        try {
            ReplicationService manager = (ReplicationService) Naming.lookup(
                    "rmi://" + replicationManagerHost + ":7000/ReplicationManager"
            );
            manager.replicateAccountCreation(accountNumber, initialBalance);
            System.out.println("[Replication] Account " + accountNumber + " successfully replicated across cluster.");
        } catch (Exception e) {
            System.err.println("[Replication Error] Failed to replicate account: " + e.getMessage());
        }

        return "SUCCESS: Account " + accountNumber + " created with balance Rs. " + initialBalance;
    }

    /*
     * ===================================================
     * APPLY REPLICATED ACCOUNT (SECONDARY)
     * ===================================================
     */
    @Override
    public synchronized ReplicationResult replicateAccountCreation(String accountNumber, double initialBalance) throws RemoteException {
        System.out.println("[Secondary] Replication request received: Create Account " + accountNumber + " (Rs. " + initialBalance + ")");

        // 1. Write to Secondary SQLite DB
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT OR REPLACE INTO accounts (account_number, balance) VALUES (?, ?)")) {
                pstmt.setString(1, accountNumber);
                pstmt.setDouble(2, initialBalance);
                pstmt.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            return new ReplicationResult(false, 0, "Secondary SQLite account creation failed: " + e.getMessage());
        }

        // 2. Update Secondary in-memory map
        accountBalances.put(accountNumber, initialBalance);
        System.out.println("[Secondary] Account " + accountNumber + " applied to SQLite & Memory on Node " + nodeId);

        return new ReplicationResult(true, 0, "Account creation applied on Secondary Node " + nodeId);
    }

    /*
     * ===================================================
     * CONSTRUCTOR
     * ===================================================
     */
    public FinanceServer() throws RemoteException {
        super();

        String drift = System.getenv().getOrDefault("CLOCK_DRIFT_MS", "0");
        clockDrift = Long.parseLong(drift);

        String nodeIdValue = System.getenv().getOrDefault("NODE_ID", "1");
        nodeId = Integer.parseInt(nodeIdValue);

        currentPrimaryId = Integer.parseInt(System.getenv().getOrDefault("PRIMARY_ID", "1"));

        String roleValue = System.getenv().getOrDefault("NODE_ROLE", "PRIMARY");

        try {
            role = NodeRole.valueOf(roleValue.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.out.println("[Replication] Invalid NODE_ROLE='" + roleValue + "'. Defaulting to PRIMARY.");
            role = NodeRole.PRIMARY;
        }

        replicationManagerHost = System.getenv().getOrDefault("REPLICATION_MANAGER_HOST", "replication-manager");

        // Initialize SQLite database and sync initial account state
        initDatabase();

        System.out.println("[Finance Server] Thread pool created with " + THREAD_POOL_SIZE + " threads.");
        System.out.println("[Clock] Simulated clock drift: " + clockDrift + " ms");
        System.out.println("[Replication] Node Role: " + role);
        System.out.println("[Replication] Manager Host: " + replicationManagerHost);
    }

    /*
     * ===================================================
     * CLIENT TRANSACTION PROCESSING
     * ===================================================
     */
    @Override
    public String processTransaction(String customerId, String fromAccount, String toAccount, double amount)
            throws RemoteException {

        if (role != NodeRole.PRIMARY) {
            return "Transaction rejected: Finance Server is currently SECONDARY.";
        }

        long transactionLamport = lamportClock.tick();
        long transactionId = transactionCounter.incrementAndGet();
        long transactionTimestamp = getSynchronizedTime();

        System.out.println();
        System.out.println("--------------------------------------------");
        System.out.println("[TRANSACTION] ID=TXN" + transactionId + " | Physical Time=" + transactionTimestamp + " | Lamport Time=" + transactionLamport);
        System.out.println("[Finance Server] New transaction received");
        System.out.println("Transaction ID : TXN" + transactionId);
        System.out.println("Customer       : " + customerId);
        System.out.println("From Account   : " + fromAccount);
        System.out.println("To Account     : " + toAccount);
        System.out.println("Amount         : Rs. " + amount);
        System.out.println("Timestamp      : " + transactionTimestamp);

        executorService.submit(() -> {
            long threadId = Thread.currentThread().getId();
            String threadName = Thread.currentThread().getName();

            System.out.println("[Thread " + threadId + "] " + threadName + " started processing TXN" + transactionId);

            try {
                validateTransaction(fromAccount, toAccount, amount);
                fraudCheck(customerId, amount);

                synchronized (replicationLock) {
                    double oldFromBalance = accountBalances.get(fromAccount);
                    double oldToBalance = accountBalances.get(toAccount);

                    transferMoney(fromAccount, toAccount, amount);

                    ReplicationResult replicationResult = replicateToManager(
                            transactionId, customerId, fromAccount, toAccount, amount, transactionTimestamp, transactionLamport
                    );

                    if (!replicationResult.isSuccess()) {
                        // Rollback in memory
                        accountBalances.put(fromAccount, oldFromBalance);
                        accountBalances.put(toAccount, oldToBalance);

                        // Rollback in SQLite DB
                        try (Connection conn = DriverManager.getConnection(dbUrl)) {
                            try (PreparedStatement p1 = conn.prepareStatement("UPDATE accounts SET balance = ? WHERE account_number = ?")) {
                                p1.setDouble(1, oldFromBalance);
                                p1.setString(2, fromAccount);
                                p1.executeUpdate();
                                p1.setDouble(1, oldToBalance);
                                p1.setString(2, toAccount);
                                p1.executeUpdate();
                            }
                        }

                        System.out.println("[Replication] TXN" + transactionId + " rolled back on Primary.");
                        throw new Exception("Synchronous replication failed: " + replicationResult.getMessage());
                    }

                    System.out.println("[Replication] TXN" + transactionId + " successfully replicated.");
                    recordTransaction(transactionId, customerId, amount);
                    sendNotification(customerId, transactionId);
                }

                System.out.println("[Thread " + threadId + "] TXN" + transactionId + " completed successfully.");
                return "SUCCESS: TXN" + transactionId;

            } catch (Exception e) {
                System.out.println("[Thread " + threadId + "] TXN" + transactionId + " failed: " + e.getMessage());
                return "FAILED: TXN" + transactionId + " - " + e.getMessage();
            }
        });

        return "Transaction accepted and assigned to a worker thread. TXN" + transactionId;
    }

    private ReplicationResult replicateToManager(
            long transactionId, String customerId, String fromAccount, String toAccount, double amount, long physicalTimestamp, long lamportTimestamp) {

        try {
            System.out.println();
            System.out.println("[Replication] Primary TXN" + transactionId + " -> Replication Manager");

            long replicationLamport = lamportClock.sendEvent();

            ReplicationService manager = (ReplicationService) Naming.lookup(
                    "rmi://" + replicationManagerHost + ":7000/ReplicationManager"
            );

            ReplicationResult result = manager.replicateTransaction(
                    transactionId, customerId, fromAccount, toAccount, amount, physicalTimestamp, Math.max(lamportTimestamp, replicationLamport)
            );

            if (result.isSuccess()) {
                System.out.println("[Replication] Manager ACK received for TXN" + transactionId);
            } else {
                System.out.println("[Replication] Manager rejected TXN" + transactionId);
            }

            return result;

        } catch (Exception e) {
            System.out.println("[Replication] Unable to contact Replication Manager: " + e.getMessage());
            return new ReplicationResult(false, transactionId, "Replication Manager unavailable: " + e.getMessage());
        }
    }

    private void validateTransaction(String fromAccount, String toAccount, double amount) throws Exception {
        if (!accountBalances.containsKey(fromAccount)) {
            throw new Exception("Source account does not exist.");
        }
        if (!accountBalances.containsKey(toAccount)) {
            throw new Exception("Destination account does not exist.");
        }
        if (fromAccount.equals(toAccount)) {
            throw new Exception("Source and destination accounts must differ.");
        }
        if (amount <= 0) {
            throw new Exception("Transaction amount must be positive.");
        }
    }

    private void fraudCheck(String customerId, double amount) throws InterruptedException {
        System.out.println("[Finance Server] Fraud check for " + customerId + " : PASSED (Rs. " + amount + ")");
        Thread.sleep(300);
    }

    private synchronized void transferMoney(String fromAccount, String toAccount, double amount) throws Exception {
        Double sourceBalance = accountBalances.get(fromAccount);
        Double destinationBalance = accountBalances.get(toAccount);

        if (sourceBalance == null || destinationBalance == null) {
            throw new Exception("Account does not exist.");
        }
        if (sourceBalance < amount) {
            throw new Exception("Insufficient balance.");
        }

        // 1. Update SQLite DB locally
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);

            try (PreparedStatement deduct = conn.prepareStatement("UPDATE accounts SET balance = balance - ? WHERE account_number = ?")) {
                deduct.setDouble(1, amount);
                deduct.setString(2, fromAccount);
                deduct.executeUpdate();
            }

            try (PreparedStatement credit = conn.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE account_number = ?")) {
                credit.setDouble(1, amount);
                credit.setString(2, toAccount);
                credit.executeUpdate();
            }

            try (PreparedStatement log = conn.prepareStatement("INSERT INTO transaction_history (customer_id, from_acc, to_acc, amount) VALUES (?, ?, ?, ?)")) {
                log.setString(1, "LOCAL");
                log.setString(2, fromAccount);
                log.setString(3, toAccount);
                log.setDouble(4, amount);
                log.executeUpdate();
            }

            conn.commit();
        } catch (SQLException e) {
            throw new Exception("SQLite persistence failed: " + e.getMessage());
        }

        // 2. Update in-memory state
        accountBalances.put(fromAccount, sourceBalance - amount);
        accountBalances.put(toAccount, destinationBalance + amount);

        System.out.println("[SQLite & Memory] Transfer completed on Node " + nodeId + ": Rs. " + amount + " from " + fromAccount + " to " + toAccount);
    }

    private void recordTransaction(long transactionId, String customerId, double amount) throws InterruptedException {
        System.out.println("[Finance Server] TXN" + transactionId + " recorded in transaction log for " + customerId);
        Thread.sleep(200);
    }

    private void sendNotification(String customerId, long transactionId) {
        System.out.println("[Finance Server] Notification sent to " + customerId + " for TXN" + transactionId);
    }

    @Override
    public synchronized double getBalance(String accountNumber) throws RemoteException {
        String sql = "SELECT balance FROM accounts WHERE account_number = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, accountNumber);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getDouble("balance");
            }
        } catch (SQLException e) {
            System.err.println("[SQLite Error] getBalance failed: " + e.getMessage());
        }

        Double balance = accountBalances.get(accountNumber);
        return balance == null ? -1 : balance;
    }

    /*
     * ===================================================
     * REPLICATION SERVICE: SECONDARY RECEIVES TRANSACTION
     * ===================================================
     */
    @Override
    public synchronized ReplicationResult replicateTransaction(
            long transactionId, String customerId, String fromAccount, String toAccount, double amount, long physicalTimestamp, long lamportTimestamp)
            throws RemoteException {

        System.out.println();
        System.out.println("[Secondary] Replication request received.");
        System.out.println("[Secondary] TXN" + transactionId);
        System.out.println("[Secondary] " + fromAccount + " -> " + toAccount + " | Rs. " + amount);

        if (role != NodeRole.SECONDARY) {
            return new ReplicationResult(false, transactionId, "Node is not SECONDARY.");
        }

        try {
            long updatedLamport = lamportClock.receiveEvent(lamportTimestamp);
            System.out.println("[Secondary] Lamport Time = " + updatedLamport);

            if (!accountBalances.containsKey(fromAccount) || !accountBalances.containsKey(toAccount)) {
                return new ReplicationResult(false, transactionId, "Account does not exist.");
            }

            double sourceBalance = accountBalances.get(fromAccount);
            if (sourceBalance < amount) {
                return new ReplicationResult(false, transactionId, "Insufficient balance on Secondary.");
            }

            double destinationBalance = accountBalances.get(toAccount);

            // Update SQLite DB on Secondary
            try (Connection conn = DriverManager.getConnection(dbUrl)) {
                conn.setAutoCommit(false);
                try (PreparedStatement deduct = conn.prepareStatement("UPDATE accounts SET balance = balance - ? WHERE account_number = ?")) {
                    deduct.setDouble(1, amount);
                    deduct.setString(2, fromAccount);
                    deduct.executeUpdate();
                }
                try (PreparedStatement credit = conn.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE account_number = ?")) {
                    credit.setDouble(1, amount);
                    credit.setString(2, toAccount);
                    credit.executeUpdate();
                }
                conn.commit();
            }

            // Update in-memory state on Secondary
            accountBalances.put(fromAccount, sourceBalance - amount);
            accountBalances.put(toAccount, destinationBalance + amount);

            long currentCounter = transactionCounter.get();
            if (transactionId > currentCounter) {
                transactionCounter.set(transactionId);
            }

            System.out.println("[Secondary] TXN" + transactionId + " applied successfully to SQLite & Memory.");
            System.out.println("[Secondary] ACK -> Replication Manager");

            return new ReplicationResult(true, transactionId, "Transaction replicated successfully.");

        } catch (Exception e) {
            return new ReplicationResult(false, transactionId, "Secondary replication failed: " + e.getMessage());
        }
    }

    @Override
    public synchronized AccountStateSnapshot getStateSnapshot() throws RemoteException {
        AccountStateSnapshot snapshot = new AccountStateSnapshot(accountBalances, transactionCounter.get());
        System.out.println("[Replication] State snapshot created. Balances: " + accountBalances);
        return snapshot;
    }

    @Override
    public synchronized void applyStateSnapshot(AccountStateSnapshot snapshot) throws RemoteException {
        if (snapshot == null) {
            throw new RemoteException("Snapshot cannot be null.");
        }
        if (role != NodeRole.SECONDARY) {
            throw new RemoteException("Only SECONDARY can apply a replication snapshot.");
        }

        accountBalances.clear();
        accountBalances.putAll(snapshot.getAccountBalances());
        transactionCounter.set(snapshot.getTransactionCounter());

        // Update SQLite DB with full snapshot
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT OR REPLACE INTO accounts (account_number, balance) VALUES (?, ?)")) {
                for (Map.Entry<String, Double> entry : accountBalances.entrySet()) {
                    pstmt.setString(1, entry.getKey());
                    pstmt.setDouble(2, entry.getValue());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            System.err.println("[SQLite Error] Snapshot application failed: " + e.getMessage());
        }

        System.out.println("\n[Replication] FULL SNAPSHOT APPLIED TO SQLITE & MEMORY.");
        System.out.println("[Replication] New balances: " + accountBalances);
    }

    @Override
    public boolean ping() throws RemoteException {
        return true;
    }

    @Override
    public NodeRole getRole() throws RemoteException {
        return role;
    }

    @Override
    public int getNodeId() throws RemoteException {
        return nodeId;
    }

    @Override
    public synchronized void promoteToPrimary() throws RemoteException {
        System.out.println("\n============================================");
        System.out.println("[FAILOVER][Node " + nodeId + "] PROMOTED TO PRIMARY");
        role = NodeRole.PRIMARY;
        currentPrimaryId = nodeId;
        electionInProgress = false;
        System.out.println("[FAILOVER][Node " + nodeId + "] Role = PRIMARY");
        System.out.println("[FAILOVER][Node " + nodeId + "] Replicated SQLite state preserved.");
        System.out.println("============================================");
    }

    public long getLocalTime() {
        return System.currentTimeMillis() + clockDrift;
    }

    public long getSynchronizedTime() {
        return getLocalTime() + clockOffset;
    }

    public void synchronizeClock() {
        try {
            String masterHost = System.getenv().getOrDefault("CLOCK_MASTER_HOST", "clock-master");
            ClockService clockMaster = (ClockService) Naming.lookup("rmi://" + masterHost + ":6000/clock_synchronisation.ClockService");

            long lamportBeforeSend = lamportClock.sendEvent();
            long t1 = getLocalTime();

            ClockResponse response = clockMaster.synchronize(lamportBeforeSend);
            long t2 = getLocalTime();
            long rtt = t2 - t1;
            long estimatedMasterTime = response.getServerTime() + (rtt / 2);
            long adjustment = estimatedMasterTime - t2;

            clockOffset = adjustment;
            long lamportAfterReceive = lamportClock.receiveEvent(response.getLamportTime());

            System.out.println("\n============================================");
            System.out.println("        CLOCK SYNCHRONIZATION");
            System.out.println("============================================");
            System.out.println("T1 (Request Time)      : " + t1);
            System.out.println("Server Time            : " + response.getServerTime());
            System.out.println("T2 (Response Time)     : " + t2);
            System.out.println("RTT                    : " + rtt + " ms");
            System.out.println("Synchronized Time      : " + getSynchronizedTime());
            System.out.println("Lamport After Receive  : " + lamportAfterReceive);
            System.out.println("============================================");

        } catch (Exception e) {
            System.out.println("[Clock] Synchronization failed: " + e.getMessage());
        }
    }

    @Override
    public synchronized void startElection() throws RemoteException {
        if (role == NodeRole.PRIMARY) {
            System.out.println("[Election][Node " + nodeId + "] Already PRIMARY.");
            return;
        }

        if (electionInProgress) {
            return;
        }

        electionInProgress = true;

        System.out.println("\n============================================");
        System.out.println("[Election][Node " + nodeId + "] STARTED");
        System.out.println("[Election][Node " + nodeId + "] Searching for higher-ID nodes.");

        boolean higherNodeAlive = false;

        for (int higherId = nodeId + 1; higherId <= 4; higherId++) {
            String host = System.getenv().get("NODE" + higherId + "_HOST");
            if (host == null) continue;

            try {
                ReplicationService higherNode = (ReplicationService) Naming.lookup("rmi://" + host + ":1234/replication.ReplicationService");
                if (higherNode.ping()) {
                    higherNodeAlive = true;
                    System.out.println("[Election][Node " + nodeId + "] Higher node " + higherId + " is alive.");
                    higherNode.electionMessage(nodeId);
                }
            } catch (Exception e) {
                System.out.println("[Election][Node " + nodeId + "] Node " + higherId + " unavailable.");
            }
        }

        if (!higherNodeAlive) {
            System.out.println("[Election][Node " + nodeId + "] No higher node available.");
            System.out.println("[Election][Node " + nodeId + "] I AM THE WINNER.");

            try {
                ReplicationService manager = (ReplicationService) Naming.lookup("rmi://" + replicationManagerHost + ":7000/ReplicationManager");
                manager.electionWon(nodeId);
            } catch (Exception e) {
                System.out.println("[Election][Node " + nodeId + "] Unable to notify Manager: " + e.getMessage());
            }
        }

        electionInProgress = false;
    }

    @Override
    public synchronized void electionMessage(int candidateId) throws RemoteException {
        System.out.println("[Election][Node " + nodeId + "] Received election message from Node " + candidateId);
        if (nodeId > candidateId) {
            System.out.println("[Election][Node " + nodeId + "] My ID is higher.");
            startElection();
        }
    }

    @Override
    public synchronized void electionWon(int newPrimaryId) throws RemoteException {
        updatePrimary(newPrimaryId);
    }

    @Override
    public synchronized void updatePrimary(int newPrimaryId) throws RemoteException {
        currentPrimaryId = newPrimaryId;
        role = (nodeId == newPrimaryId) ? NodeRole.PRIMARY : NodeRole.SECONDARY;

        System.out.println("[Election][Node " + nodeId + "] Cluster updated.");
        System.out.println("[Election][Node " + nodeId + "] Primary = Node " + newPrimaryId);
        System.out.println("[Election][Node " + nodeId + "] Role = " + role);
    }

    public void shutdown() {
        System.out.println("[Finance Server] Shutting down thread pool...");
        executorService.shutdown();
    }

    public static void main(String[] args) {
        try {
            System.setProperty("java.rmi.server.hostname", System.getenv().getOrDefault("RMI_HOSTNAME", "localhost"));
            System.out.println("Starting FinVault Finance RMI Server...");

            FinanceServer server = new FinanceServer();
            Registry registry = LocateRegistry.createRegistry(1234);

            System.out.println("RMI Registry started on port 1234.");

            registry.rebind("FinanceServer", server);
            registry.rebind("replication.ReplicationService", server);

            System.out.println("Registered as: FinanceServer");
            System.out.println("Registered as: replication.ReplicationService");

            Thread.sleep(2000);
            server.synchronizeClock();

            System.out.println("\n============================================");
            System.out.println("     FINVAULT FINANCE SERVER STARTED");
            System.out.println("============================================");
            System.out.println("Node ID: " + server.nodeId);
            System.out.println("Role: " + server.getRole());
            System.out.println("Replication Manager: " + server.replicationManagerHost);
            System.out.println("Waiting for financial transactions...");
            System.out.println("============================================");

            Thread.currentThread().join();

        } catch (Exception e) {
            System.out.println("Finance Server Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}