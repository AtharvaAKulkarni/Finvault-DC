import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.Naming;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class FinanceServer
        extends UnicastRemoteObject
        implements FinanceService, ReplicationService {

    private static final int THREAD_POOL_SIZE = 5;

    /*
     * ---------------------------------------------------
     * SERVER ROLE
     * ---------------------------------------------------
     *
     * finance-server-1 -> PRIMARY
     * finance-server-2 -> SECONDARY
     *
     * The role can be changed during failover.
     */
    private volatile NodeRole role;


    /*
     * ---------------------------------------------------
     * CLOCK VARIABLES
     * ---------------------------------------------------
     */

    private final long clockDrift;

    private long clockOffset = 0;

    private final LamportClock lamportClock =
            new LamportClock();


    /*
     * ---------------------------------------------------
     * THREAD POOL
     * ---------------------------------------------------
     */

    private final ExecutorService executorService =
            Executors.newFixedThreadPool(
                    THREAD_POOL_SIZE
            );


    /*
     * ---------------------------------------------------
     * FINANCIAL STATE
     * ---------------------------------------------------
     */

    private final AtomicLong transactionCounter =
            new AtomicLong(1000);


    private final Map<String, Double> accountBalances =
            new ConcurrentHashMap<>();


    /*
     * ---------------------------------------------------
     * REPLICATION VARIABLES
     * ---------------------------------------------------
     */

    /*
     * Used to make the financial update + replication
     * one synchronous critical section.
     *
     * This prevents two Primary transactions from
     * changing the Primary state in different orders
     * while being replicated in another order.
     */
    private final Object replicationLock =
            new Object();


    /*
     * Hostname of the Replication Manager.
     */
    private final String replicationManagerHost;


    /*
     * ---------------------------------------------------
     * CONSTRUCTOR
     * ---------------------------------------------------
     */

    public FinanceServer()
            throws RemoteException {

        super();


        /*
         * -----------------------------------------------
         * CLOCK DRIFT
         * -----------------------------------------------
         */

        String drift =
                System.getenv().getOrDefault(
                        "CLOCK_DRIFT_MS",
                        "0"
                );

        clockDrift =
                Long.parseLong(drift);


        /*
         * -----------------------------------------------
         * SERVER ROLE
         * -----------------------------------------------
         */

        String roleValue =
                System.getenv().getOrDefault(
                        "NODE_ROLE",
                        "PRIMARY"
                );


        try {

            role =
                    NodeRole.valueOf(
                            roleValue.toUpperCase()
                    );

        } catch (IllegalArgumentException e) {

            System.out.println(
                    "[Replication] Invalid NODE_ROLE='"
                            + roleValue
                            + "'. Defaulting to PRIMARY."
            );

            role = NodeRole.PRIMARY;
        }


        /*
         * -----------------------------------------------
         * REPLICATION MANAGER HOST
         * -----------------------------------------------
         */

        replicationManagerHost =
                System.getenv().getOrDefault(
                        "REPLICATION_MANAGER_HOST",
                        "replication-manager"
                );


        /*
         * -----------------------------------------------
         * INITIAL ACCOUNT BALANCES
         * -----------------------------------------------
         */

        accountBalances.put(
                "ACC101",
                50000.0
        );

        accountBalances.put(
                "ACC102",
                35000.0
        );

        accountBalances.put(
                "ACC103",
                70000.0
        );

        accountBalances.put(
                "ACC104",
                45000.0
        );


        /*
         * -----------------------------------------------
         * STARTUP INFORMATION
         * -----------------------------------------------
         */

        System.out.println(
                "[Finance Server] Thread pool created "
                        + "with "
                        + THREAD_POOL_SIZE
                        + " threads."
        );

        System.out.println(
                "[Clock] Simulated clock drift: "
                        + clockDrift
                        + " ms"
        );

        System.out.println(
                "[Replication] Node Role: "
                        + role
        );

        System.out.println(
                "[Replication] Manager Host: "
                        + replicationManagerHost
        );
    }


    /*
     * ===================================================
     * CLIENT TRANSACTION PROCESSING
     * ===================================================
     */

    @Override
    public String processTransaction(
            String customerId,
            String fromAccount,
            String toAccount,
            double amount)
            throws RemoteException {


        /*
         * ------------------------------------------------
         * A SECONDARY SHOULD NOT ACCEPT NEW CLIENT
         * TRANSACTIONS.
         * ------------------------------------------------
         */

        if (role != NodeRole.PRIMARY) {

            return "Transaction rejected: "
                    + "Finance Server is currently SECONDARY.";
        }


        /*
         * ------------------------------------------------
         * LAMPORT EVENT
         * ------------------------------------------------
         */

        long transactionLamport =
                lamportClock.tick();


        /*
         * ------------------------------------------------
         * TRANSACTION ID
         * ------------------------------------------------
         */

        long transactionId =
                transactionCounter
                        .incrementAndGet();


        /*
         * ------------------------------------------------
         * SYNCHRONIZED PHYSICAL TIME
         * ------------------------------------------------
         */

        long transactionTimestamp =
                getSynchronizedTime();


        System.out.println();

        System.out.println(
                "--------------------------------------------"
        );

        System.out.println(
                "[TRANSACTION]"
                        + " ID=TXN"
                        + transactionId
                        + " | Physical Time="
                        + transactionTimestamp
                        + " | Lamport Time="
                        + transactionLamport
        );


        System.out.println(
                "[Finance Server] New transaction received"
        );

        System.out.println(
                "Transaction ID : TXN"
                        + transactionId
        );

        System.out.println(
                "Customer       : "
                        + customerId
        );

        System.out.println(
                "From Account   : "
                        + fromAccount
        );

        System.out.println(
                "To Account     : "
                        + toAccount
        );

        System.out.println(
                "Amount         : Rs. "
                        + amount
        );

        System.out.println(
                "Timestamp      : "
                        + transactionTimestamp
        );


        /*
         * ------------------------------------------------
         * SUBMIT WORK TO THREAD POOL
         * ------------------------------------------------
         */

        executorService.submit(() -> {

            long threadId =
                    Thread.currentThread().getId();

            String threadName =
                    Thread.currentThread().getName();


            System.out.println(
                    "[Thread "
                            + threadId
                            + "] "
                            + threadName
                            + " started processing TXN"
                            + transactionId
            );


            try {

                /*
                 * -----------------------------------------
                 * STEP 1: VALIDATION
                 * -----------------------------------------
                 */

                validateTransaction(
                        fromAccount,
                        toAccount,
                        amount
                );


                /*
                 * -----------------------------------------
                 * STEP 2: FRAUD CHECK
                 * -----------------------------------------
                 */

                fraudCheck(
                        customerId,
                        amount
                );


                /*
                 * -----------------------------------------
                 * STEP 3 + 4:
                 *
                 * LOCAL UPDATE AND SYNCHRONOUS
                 * REPLICATION
                 *
                 * These two operations are protected by
                 * replicationLock.
                 * -----------------------------------------
                 */

                synchronized (replicationLock) {


                    /*
                     * Save balances so that we can roll
                     * back if Secondary replication fails.
                     */

                    double oldFromBalance =
                            accountBalances.get(
                                    fromAccount
                            );

                    double oldToBalance =
                            accountBalances.get(
                                    toAccount
                            );


                    /*
                     * -------------------------------------
                     * UPDATE PRIMARY STATE
                     * -------------------------------------
                     */

                    transferMoney(
                            fromAccount,
                            toAccount,
                            amount
                    );


                    /*
                     * -------------------------------------
                     * REPLICATE TO SECONDARY
                     * -------------------------------------
                     */

                    ReplicationResult replicationResult =
                            replicateToManager(
                                    transactionId,
                                    customerId,
                                    fromAccount,
                                    toAccount,
                                    amount,
                                    transactionTimestamp,
                                    transactionLamport
                            );


                    /*
                     * -------------------------------------
                     * CHECK SECONDARY ACK
                     * -------------------------------------
                     */

                    if (!replicationResult.isSuccess()) {

                        /*
                         * Replication failed.
                         *
                         * Roll back Primary state so that
                         * Primary and Secondary do not
                         * permanently diverge.
                         */

                        accountBalances.put(
                                fromAccount,
                                oldFromBalance
                        );

                        accountBalances.put(
                                toAccount,
                                oldToBalance
                        );


                        System.out.println(
                                "[Replication] TXN"
                                        + transactionId
                                        + " rolled back on Primary."
                        );


                        throw new Exception(
                                "Synchronous replication failed: "
                                        + replicationResult
                                        .getMessage()
                        );
                    }


                    /*
                     * -------------------------------------
                     * REPLICATION SUCCESSFUL
                     * -------------------------------------
                     */

                    System.out.println(
                            "[Replication] TXN"
                                    + transactionId
                                    + " successfully replicated."
                    );


                    /*
                     * -------------------------------------
                     * RECORD TRANSACTION
                     * -------------------------------------
                     */

                    recordTransaction(
                            transactionId,
                            customerId,
                            amount
                    );


                    /*
                     * -------------------------------------
                     * NOTIFICATION
                     * -------------------------------------
                     */

                    sendNotification(
                            customerId,
                            transactionId
                    );
                }


                System.out.println(
                        "[Thread "
                                + threadId
                                + "] TXN"
                                + transactionId
                                + " completed successfully."
                );


            } catch (Exception e) {

                System.out.println(
                        "[Thread "
                                + threadId
                                + "] TXN"
                                + transactionId
                                + " failed: "
                                + e.getMessage()
                );
            }
        });


        /*
         * The client receives the transaction ID.
         *
         * The actual financial operation is performed
         * by the worker thread.
         */

        return "Transaction accepted and assigned "
                + "to a worker thread. TXN"
                + transactionId;
    }


    /*
     * ===================================================
     * REPLICATION: PRIMARY -> MANAGER
     * ===================================================
     */

    private ReplicationResult replicateToManager(
            long transactionId,
            String customerId,
            String fromAccount,
            String toAccount,
            double amount,
            long physicalTimestamp,
            long lamportTimestamp) {


        try {


            System.out.println();

            System.out.println(
                    "[Replication] Primary TXN"
                            + transactionId
                            + " -> Replication Manager"
            );


            /*
             * -----------------------------------------
             * LAMPORT SEND EVENT
             * -----------------------------------------
             */

            long replicationLamport =
                    lamportClock.sendEvent();


            /*
             * -----------------------------------------
             * LOOK UP MANAGER
             * -----------------------------------------
             */

            ReplicationService manager =
                    (ReplicationService) Naming.lookup(
                            "rmi://"
                                    + replicationManagerHost
                                    + ":7000/"
                                    + "ReplicationManager"
                    );


            /*
             * -----------------------------------------
             * SEND TRANSACTION
             * -----------------------------------------
             */

            ReplicationResult result =
                    manager.replicateTransaction(
                            transactionId,
                            customerId,
                            fromAccount,
                            toAccount,
                            amount,
                            physicalTimestamp,
                            Math.max(
                                    lamportTimestamp,
                                    replicationLamport
                            )
                    );


            /*
             * -----------------------------------------
             * ACK
             * -----------------------------------------
             */

            if (result.isSuccess()) {

                System.out.println(
                        "[Replication] Manager ACK received "
                                + "for TXN"
                                + transactionId
                );

            } else {

                System.out.println(
                        "[Replication] Manager rejected TXN"
                                + transactionId
                );
            }


            return result;


        } catch (Exception e) {

            System.out.println(
                    "[Replication] Unable to contact "
                            + "Replication Manager: "
                            + e.getMessage()
            );


            return new ReplicationResult(
                    false,
                    transactionId,
                    "Replication Manager unavailable: "
                            + e.getMessage()
            );
        }
    }


    /*
     * ===================================================
     * TRANSACTION VALIDATION
     * ===================================================
     */

    private void validateTransaction(
            String fromAccount,
            String toAccount,
            double amount)
            throws Exception {


        if (!accountBalances.containsKey(
                fromAccount)) {

            throw new Exception(
                    "Source account does not exist."
            );
        }


        if (!accountBalances.containsKey(
                toAccount)) {

            throw new Exception(
                    "Destination account does not exist."
            );
        }


        if (fromAccount.equals(toAccount)) {

            throw new Exception(
                    "Source and destination accounts "
                            + "must differ."
            );
        }


        if (amount <= 0) {

            throw new Exception(
                    "Transaction amount must be positive."
            );
        }
    }


    /*
     * ===================================================
     * FRAUD CHECK
     * ===================================================
     */

    private void fraudCheck(
            String customerId,
            double amount)
            throws InterruptedException {


        System.out.println(
                "[Finance Server] Fraud check for "
                        + customerId
                        + " : PASSED (Rs. "
                        + amount
                        + ")"
        );


        Thread.sleep(300);
    }


    /*
     * ===================================================
     * TRANSFER MONEY
     * ===================================================
     */

    private synchronized void transferMoney(
            String fromAccount,
            String toAccount,
            double amount)
            throws Exception {


        Double sourceBalance =
                accountBalances.get(
                        fromAccount
                );


        Double destinationBalance =
                accountBalances.get(
                        toAccount
                );


        if (sourceBalance == null) {

            throw new Exception(
                    "Source account does not exist."
            );
        }


        if (destinationBalance == null) {

            throw new Exception(
                    "Destination account does not exist."
            );
        }


        if (sourceBalance < amount) {

            throw new Exception(
                    "Insufficient balance."
            );
        }


        /*
         * Debit source account.
         */

        accountBalances.put(
                fromAccount,
                sourceBalance - amount
        );


        /*
         * Credit destination account.
         */

        accountBalances.put(
                toAccount,
                destinationBalance + amount
        );


        System.out.println(
                "[Finance Server] Transfer completed: "
                        + "Rs. "
                        + amount
                        + " from "
                        + fromAccount
                        + " to "
                        + toAccount
        );
    }


    /*
     * ===================================================
     * TRANSACTION RECORD
     * ===================================================
     */

    private void recordTransaction(
            long transactionId,
            String customerId,
            double amount)
            throws InterruptedException {


        System.out.println(
                "[Finance Server] TXN"
                        + transactionId
                        + " recorded in transaction log "
                        + "for "
                        + customerId
        );


        Thread.sleep(200);
    }


    /*
     * ===================================================
     * NOTIFICATION
     * ===================================================
     */

    private void sendNotification(
            String customerId,
            long transactionId) {


        System.out.println(
                "[Finance Server] Notification sent to "
                        + customerId
                        + " for TXN"
                        + transactionId
        );
    }


    /*
     * ===================================================
     * GET BALANCE
     * ===================================================
     */

    @Override
    public double getBalance(
            String accountNumber)
            throws RemoteException {


        Double balance =
                accountBalances.get(
                        accountNumber
                );


        return balance == null
                ? -1
                : balance;
    }


    /*
     * ===================================================
     * REPLICATION SERVICE
     *
     * SECONDARY RECEIVES TRANSACTION
     * ===================================================
     */

    @Override
    public synchronized ReplicationResult
    replicateTransaction(
            long transactionId,
            String customerId,
            String fromAccount,
            String toAccount,
            double amount,
            long physicalTimestamp,
            long lamportTimestamp)
            throws RemoteException {


        System.out.println();

        System.out.println(
                "[Secondary] Replication request received."
        );


        System.out.println(
                "[Secondary] TXN"
                        + transactionId
        );


        System.out.println(
                "[Secondary] "
                        + fromAccount
                        + " -> "
                        + toAccount
                        + " | Rs. "
                        + amount
        );


        /*
         * -----------------------------------------------
         * ONLY SECONDARY ACCEPTS REPLICATION REQUESTS
         * -----------------------------------------------
         */

        if (role != NodeRole.SECONDARY) {

            return new ReplicationResult(
                    false,
                    transactionId,
                    "Node is not SECONDARY."
            );
        }


        try {


            /*
             * -------------------------------------------
             * UPDATE LAMPORT CLOCK
             * -------------------------------------------
             */

            long updatedLamport =
                    lamportClock.receiveEvent(
                            lamportTimestamp
                    );


            System.out.println(
                    "[Secondary] Lamport Time = "
                            + updatedLamport
            );


            /*
             * -------------------------------------------
             * VALIDATE REPLICATED DATA
             * -------------------------------------------
             */

            if (!accountBalances.containsKey(
                    fromAccount)) {

                return new ReplicationResult(
                        false,
                        transactionId,
                        "Source account does not exist."
                );
            }


            if (!accountBalances.containsKey(
                    toAccount)) {

                return new ReplicationResult(
                        false,
                        transactionId,
                        "Destination account does not exist."
                );
            }


            if (fromAccount.equals(toAccount)) {

                return new ReplicationResult(
                        false,
                        transactionId,
                        "Source and destination "
                                + "accounts must differ."
                );
            }


            if (amount <= 0) {

                return new ReplicationResult(
                        false,
                        transactionId,
                        "Transaction amount must be positive."
                );
            }


            /*
             * -------------------------------------------
             * CHECK SECONDARY BALANCE
             * -------------------------------------------
             */

            double sourceBalance =
                    accountBalances.get(
                            fromAccount
                    );


            if (sourceBalance < amount) {

                return new ReplicationResult(
                        false,
                        transactionId,
                        "Insufficient balance on Secondary."
                );
            }


            /*
             * -------------------------------------------
             * APPLY REPLICATED FINANCIAL UPDATE
             * -------------------------------------------
             *
             * IMPORTANT:
             *
             * We do NOT run:
             *
             * validateTransaction()
             * fraudCheck()
             * recordTransaction()
             * sendNotification()
             *
             * again.
             *
             * The Primary already performed those stages.
             * The Secondary only applies the replicated
             * state change.
             */

            double destinationBalance =
                    accountBalances.get(
                            toAccount
                    );


            accountBalances.put(
                    fromAccount,
                    sourceBalance - amount
            );


            accountBalances.put(
                    toAccount,
                    destinationBalance + amount
            );


            /*
             * -------------------------------------------
             * KEEP TRANSACTION COUNTER IN SYNC
             * -------------------------------------------
             */

            long currentCounter =
                    transactionCounter.get();


            if (transactionId > currentCounter) {

                transactionCounter.set(
                        transactionId
                );
            }


            /*
             * -------------------------------------------
             * ACK
             * -------------------------------------------
             */

            System.out.println(
                    "[Secondary] TXN"
                            + transactionId
                            + " applied successfully."
            );


            System.out.println(
                    "[Secondary] ACK -> Replication Manager"
            );


            return new ReplicationResult(
                    true,
                    transactionId,
                    "Transaction replicated successfully."
            );


        } catch (Exception e) {

            return new ReplicationResult(
                    false,
                    transactionId,
                    "Secondary replication failed: "
                            + e.getMessage()
            );
        }
    }


    /*
     * ===================================================
     * CREATE FULL STATE SNAPSHOT
     * ===================================================
     */

    @Override
    public synchronized AccountStateSnapshot
    getStateSnapshot()
            throws RemoteException {


        /*
         * Create a complete copy of the current
         * financial state.
         */

        AccountStateSnapshot snapshot =
                new AccountStateSnapshot(
                        accountBalances,
                        transactionCounter.get()
                );


        System.out.println(
                "[Replication] State snapshot created."
        );


        System.out.println(
                "[Replication] Balances: "
                        + accountBalances
        );


        System.out.println(
                "[Replication] Transaction Counter: "
                        + transactionCounter.get()
        );


        return snapshot;
    }


    /*
     * ===================================================
     * APPLY FULL STATE SNAPSHOT
     * ===================================================
     */

    @Override
    public synchronized void applyStateSnapshot(
            AccountStateSnapshot snapshot)
            throws RemoteException {


        if (snapshot == null) {

            throw new RemoteException(
                    "Snapshot cannot be null."
            );
        }


        /*
         * Only Secondary should normally receive
         * a full-state synchronization.
         */

        if (role != NodeRole.SECONDARY) {

            throw new RemoteException(
                    "Only SECONDARY can apply "
                            + "a replication snapshot."
            );
        }


        /*
         * -------------------------------------------
         * REPLACE ACCOUNT STATE
         * -------------------------------------------
         */

        accountBalances.clear();


        accountBalances.putAll(
                snapshot.getAccountBalances()
        );


        /*
         * -------------------------------------------
         * SYNCHRONIZE TRANSACTION COUNTER
         * -------------------------------------------
         */

        transactionCounter.set(
                snapshot.getTransactionCounter()
        );


        System.out.println();

        System.out.println(
                "[Replication] FULL SNAPSHOT APPLIED."
        );


        System.out.println(
                "[Replication] New balances: "
                        + accountBalances
        );


        System.out.println(
                "[Replication] New transaction counter: "
                        + transactionCounter.get()
        );
    }


    /*
     * ===================================================
     * HEALTH CHECK
     * ===================================================
     */

    @Override
    public boolean ping()
            throws RemoteException {

        return true;
    }


    /*
     * ===================================================
     * GET NODE ROLE
     * ===================================================
     */

    @Override
    public NodeRole getRole()
            throws RemoteException {

        return role;
    }


    /*
     * ===================================================
     * PROMOTE SECONDARY -> PRIMARY
     * ===================================================
     */

    @Override
    public synchronized void promoteToPrimary()
            throws RemoteException {


        if (role == NodeRole.PRIMARY) {

            System.out.println(
                    "[Failover] Node is already PRIMARY."
            );

            return;
        }


        System.out.println();

        System.out.println(
                "============================================"
        );

        System.out.println(
                "          FINVAULT FAILOVER"
        );

        System.out.println(
                "============================================"
        );


        System.out.println(
                "[Failover] Current Role: "
                        + role
        );


        role =
                NodeRole.PRIMARY;


        System.out.println(
                "[Failover] New Role: "
                        + role
        );


        System.out.println(
                "[Failover] Existing replicated "
                        + "financial state preserved."
        );


        System.out.println(
                "============================================"
        );
    }


    /*
     * ===================================================
     * LOCAL CLOCK
     * ===================================================
     */

    public long getLocalTime() {

        return System.currentTimeMillis()
                + clockDrift;
    }


    /*
     * ===================================================
     * SYNCHRONIZED CLOCK
     * ===================================================
     */

    public long getSynchronizedTime() {

        return getLocalTime()
                + clockOffset;
    }


    /*
     * ===================================================
     * CRISTIAN CLOCK SYNCHRONIZATION
     * ===================================================
     */

    public void synchronizeClock() {


        try {


            String masterHost =
                    System.getenv().getOrDefault(
                            "CLOCK_MASTER_HOST",
                            "clock-master"
                    );


            ClockService clockMaster =
                    (ClockService) Naming.lookup(
                            "rmi://"
                                    + masterHost
                                    + ":6000/"
                                    + "ClockService"
                    );


            /*
             * -----------------------------------------
             * LAMPORT SEND EVENT
             * -----------------------------------------
             */

            long lamportBeforeSend =
                    lamportClock.sendEvent();


            /*
             * -----------------------------------------
             * CRISTIAN T1
             * -----------------------------------------
             */

            long t1 =
                    getLocalTime();


            /*
             * -----------------------------------------
             * SEND REQUEST
             * -----------------------------------------
             */

            ClockResponse response =
                    clockMaster.synchronize(
                            lamportBeforeSend
                    );


            /*
             * -----------------------------------------
             * CRISTIAN T2
             * -----------------------------------------
             */

            long t2 =
                    getLocalTime();


            /*
             * -----------------------------------------
             * RTT
             * -----------------------------------------
             */

            long rtt =
                    t2 - t1;


            /*
             * -----------------------------------------
             * ESTIMATED MASTER TIME
             * -----------------------------------------
             */

            long estimatedMasterTime =
                    response.getServerTime()
                            + (rtt / 2);


            /*
             * -----------------------------------------
             * CLOCK ADJUSTMENT
             * -----------------------------------------
             */

            long adjustment =
                    estimatedMasterTime - t2;


            clockOffset =
                    adjustment;


            /*
             * -----------------------------------------
             * LAMPORT RECEIVE EVENT
             * -----------------------------------------
             */

            long lamportAfterReceive =
                    lamportClock.receiveEvent(
                            response.getLamportTime()
                    );


            /*
             * -----------------------------------------
             * DISPLAY RESULTS
             * -----------------------------------------
             */

            System.out.println();

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "        CLOCK SYNCHRONIZATION"
            );

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "T1 (Request Time)      : "
                            + t1
            );

            System.out.println(
                    "Server Time            : "
                            + response.getServerTime()
            );

            System.out.println(
                    "T2 (Response Time)     : "
                            + t2
            );

            System.out.println(
                    "RTT                    : "
                            + rtt
                            + " ms"
            );

            System.out.println(
                    "RTT / 2                : "
                            + (rtt / 2)
                            + " ms"
            );

            System.out.println(
                    "Estimated Server Time  : "
                            + estimatedMasterTime
            );

            System.out.println(
                    "Clock Adjustment       : "
                            + adjustment
                            + " ms"
            );

            System.out.println(
                    "Synchronized Time      : "
                            + getSynchronizedTime()
            );

            System.out.println(
                    "Lamport Sent           : "
                            + lamportBeforeSend
            );

            System.out.println(
                    "Lamport Received       : "
                            + response.getLamportTime()
            );

            System.out.println(
                    "Lamport After Receive  : "
                            + lamportAfterReceive
            );

            System.out.println(
                    "============================================"
            );


        } catch (Exception e) {


            System.out.println(
                    "[Clock] Synchronization failed: "
                            + e.getMessage()
            );
        }
    }


    /*
     * ===================================================
     * SHUTDOWN
     * ===================================================
     */

    public void shutdown() {


        System.out.println(
                "[Finance Server] "
                        + "Shutting down thread pool..."
        );


        executorService.shutdown();
    }


    /*
     * ===================================================
     * MAIN
     * ===================================================
     */

    public static void main(
            String[] args) {


        try {


            /*
             * -----------------------------------------
             * RMI HOSTNAME
             * -----------------------------------------
             */

            System.setProperty(
                    "java.rmi.server.hostname",
                    System.getenv().getOrDefault(
                            "RMI_HOSTNAME",
                            "localhost"
                    )
            );


            System.out.println(
                    "Starting FinVault Finance RMI Server..."
            );


            /*
             * -----------------------------------------
             * CREATE SERVER
             * -----------------------------------------
             */

            FinanceServer server =
                    new FinanceServer();


            /*
             * -----------------------------------------
             * CREATE RMI REGISTRY
             * -----------------------------------------
             */

            Registry registry =
                    LocateRegistry.createRegistry(
                            1234
                    );


            System.out.println(
                    "RMI Registry started on port 1234."
            );


            /*
             * -----------------------------------------
             * CLIENT SERVICE
             * -----------------------------------------
             */

            registry.rebind(
                    "FinanceServer",
                    server
            );


            /*
             * -----------------------------------------
             * REPLICATION SERVICE
             * -----------------------------------------
             */

            registry.rebind(
                    "ReplicationService",
                    server
            );


            System.out.println(
                    "Registered as: FinanceServer"
            );


            System.out.println(
                    "Registered as: ReplicationService"
            );


            /*
             * -----------------------------------------
             * WAIT FOR CLOCK MASTER
             * -----------------------------------------
             */

            Thread.sleep(2000);


            server.synchronizeClock();


            /*
             * -----------------------------------------
             * STARTUP INFORMATION
             * -----------------------------------------
             */

            System.out.println();

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "     FINVAULT FINANCE SERVER STARTED"
            );

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "Role: "
                            + server.getRole()
            );

            System.out.println(
                    "Registered as: FinanceServer"
            );

            System.out.println(
                    "Registered as: ReplicationService"
            );

            System.out.println(
                    "Thread pool size: "
                            + THREAD_POOL_SIZE
            );

            System.out.println(
                    "Replication Manager: "
                            + server.replicationManagerHost
            );

            System.out.println(
                    "Waiting for financial transactions..."
            );

            System.out.println(
                    "============================================"
            );


            /*
             * -----------------------------------------
             * KEEP SERVER ALIVE
             * -----------------------------------------
             */

            Thread.currentThread().join();


        } catch (Exception e) {


            System.out.println(
                    "Finance Server Error: "
                            + e.getMessage()
            );


            e.printStackTrace();
        }
    }
}