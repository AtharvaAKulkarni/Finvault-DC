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

public class FinanceServer extends UnicastRemoteObject implements FinanceService {

    private static final int THREAD_POOL_SIZE = 5;

    private final long clockDrift;
    private long clockOffset = 0;

    private final LamportClock lamportClock = new LamportClock();

    private final ExecutorService executorService =
            Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    private final AtomicLong transactionCounter = new AtomicLong(1000);

    // Shared account data used by concurrent transaction requests.
    private final Map<String, Double> accountBalances =
            new ConcurrentHashMap<>();

    public FinanceServer() throws RemoteException {
        super();
        String drift =
                System.getenv().getOrDefault(
                        "CLOCK_DRIFT_MS",
                        "0"
                );

        clockDrift = Long.parseLong(drift);

        accountBalances.put("ACC101", 50000.0);
        accountBalances.put("ACC102", 35000.0);
        accountBalances.put("ACC103", 70000.0);
        accountBalances.put("ACC104", 45000.0);

        System.out.println(
                "[Finance Server] Thread pool created with 5 threads."
        );

        System.out.println(
                "[Clock] Simulated clock drift: "
                        + clockDrift
                        + " ms"
        );
    }

    @Override
    public String processTransaction(String customerId,
                                     String fromAccount,
                                     String toAccount,
                                     double amount)
            throws RemoteException {
        long transactionLamport = lamportClock.tick();
        long transactionId = transactionCounter.incrementAndGet();
        long transactionTimestamp = getSynchronizedTime(); //SYNCH REQUIRED HERE
        System.out.println(
                "[TRANSACTION]"
                        + " ID=" + transactionId
                        + " | Physical Time="
                        + transactionTimestamp
                        + " | Lamport Time="
                        + transactionLamport
        );

        System.out.println();
        System.out.println("--------------------------------------------");
        System.out.println("[Finance Server] New transaction received");
        System.out.println("Transaction ID : TXN" + transactionId);
        System.out.println("Customer       : " + customerId);
        System.out.println("From Account   : " + fromAccount);
        System.out.println("To Account     : " + toAccount);
        System.out.println("Amount         : Rs. " + amount);
        System.out.println("Timestamp      : " + transactionTimestamp);

        // Submit the actual processing work to the fixed thread pool.
        executorService.submit(() -> {
            long threadId = Thread.currentThread().getId();
            String threadName = Thread.currentThread().getName();

            System.out.println("[Thread " + threadId + "] " + threadName
                    + " started processing TXN" + transactionId);

            try {
                // Simulate distributed financial processing stages.
                validateTransaction(fromAccount, toAccount, amount);
                fraudCheck(customerId, amount);
                transferMoney(fromAccount, toAccount, amount);
                recordTransaction(transactionId, customerId, amount);
                sendNotification(customerId, transactionId);

                System.out.println("[Thread " + threadId + "] "
                        + "TXN" + transactionId + " completed successfully.");

            } catch (Exception e) {
                System.out.println("[Thread " + threadId + "] "
                        + "TXN" + transactionId + " failed: " + e.getMessage());
            }
        });

        // The client receives an immediate response while the worker continues.
        return "Transaction accepted and assigned to a worker thread. TXN"
                + transactionId;
    }

    private void validateTransaction(String fromAccount,
                                     String toAccount,
                                     double amount) throws Exception {
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
        System.out.println("[Finance Server] Fraud check for " + customerId
                + " : PASSED (Rs. " + amount + ")");
        Thread.sleep(300);
    }

    // Synchronization is used so two concurrent transfers cannot update
    // the same shared account state at the same time.
    private synchronized void transferMoney(String fromAccount,
                                             String toAccount,
                                             double amount) throws Exception {
        double sourceBalance = accountBalances.get(fromAccount);

        if (sourceBalance < amount) {
            throw new Exception("Insufficient balance.");
        }

        accountBalances.put(fromAccount, sourceBalance - amount);
        accountBalances.put(toAccount, accountBalances.get(toAccount) + amount);

        System.out.println("[Finance Server] Transfer completed: Rs. " + amount
                + " from " + fromAccount + " to " + toAccount);
    }

    private void recordTransaction(long transactionId,
                                   String customerId,
                                   double amount) throws InterruptedException {
        System.out.println("[Finance Server] TXN" + transactionId
                + " recorded in transaction log for " + customerId);
        Thread.sleep(200);
    }

    private void sendNotification(String customerId,
                                  long transactionId) {
        System.out.println("[Finance Server] Notification sent to " + customerId
                + " for TXN" + transactionId);
    }

    @Override
    public double getBalance(String accountNumber) throws RemoteException {
        Double balance = accountBalances.get(accountNumber);
        return balance == null ? -1 : balance;
    }
    // Returns this server's simulated local time.
    public long getLocalTime() {
        return System.currentTimeMillis() + clockDrift;
    }
    // Returns the synchronized time.
    public long getSynchronizedTime() {
        return getLocalTime() + clockOffset;
    }

    public void synchronizeClock() {

        try {

            String masterHost =
                    System.getenv().getOrDefault(
                            "CLOCK_MASTER_HOST",
                            "clock-master"
                    );

            ClockService clockMaster =
                    (ClockService) Naming.lookup(
                            "rmi://" + masterHost
                                    + ":6000/ClockService"
                    );

            // -----------------------------
            // LAMPORT SEND EVENT
            // -----------------------------
            long lamportBeforeSend =
                    lamportClock.sendEvent();

            // -----------------------------
            // CRISTIAN: T1
            // -----------------------------
            long t1 = getLocalTime();

            // -----------------------------
            // SEND REQUEST
            // -----------------------------
            ClockResponse response =
                    clockMaster.synchronize(
                            lamportBeforeSend
                    );

            // -----------------------------
            // CRISTIAN: T2
            // -----------------------------
            long t2 = getLocalTime();

            // -----------------------------
            // RTT
            // -----------------------------
            long rtt = t2 - t1;

            // -----------------------------
            // ESTIMATED MASTER TIME
            // -----------------------------
            long estimatedMasterTime =
                    response.getServerTime()
                            + (rtt / 2);

            // -----------------------------
            // ADJUSTMENT
            // -----------------------------
            long adjustment =
                    estimatedMasterTime - t2;

            clockOffset = adjustment;

            // -----------------------------
            // LAMPORT RECEIVE EVENT
            // -----------------------------
            long lamportAfterReceive =
                    lamportClock.receiveEvent(
                            response.getLamportTime()
                    );

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
                    "T1 (Request Time)      : " + t1
            );

            System.out.println(
                    "Server Time            : "
                            + response.getServerTime()
            );

            System.out.println(
                    "T2 (Response Time)     : " + t2
            );

            System.out.println(
                    "RTT                    : "
                            + rtt + " ms"
            );

            System.out.println(
                    "RTT / 2                : "
                            + (rtt / 2) + " ms"
            );

            System.out.println(
                    "Estimated Server Time  : "
                            + estimatedMasterTime
            );

            System.out.println(
                    "Clock Adjustment       : "
                            + adjustment + " ms"
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

    public void shutdown() {
        System.out.println("[Finance Server] Shutting down thread pool...");
        executorService.shutdown();
    }

    public static void main(String[] args) {
        try {
            System.setProperty(
                    "java.rmi.server.hostname",
                    System.getenv().getOrDefault(
                            "RMI_HOSTNAME",
                            "localhost"
                    )
            );

            System.out.println("Starting FinVault Finance RMI Server...");

            FinanceServer server = new FinanceServer();

            Registry registry = LocateRegistry.createRegistry(1234);
            System.out.println("RMI Registry started on port 1234.");

            registry.rebind("FinanceServer", server);

            Thread.sleep(2000);

            server.synchronizeClock();

            System.out.println("============================================");
            System.out.println(" FINVAULT FINANCE SERVER STARTED");
            System.out.println("============================================");
            System.out.println("Registered as: FinanceServer");
            System.out.println("Thread pool size: 5");
            System.out.println("Waiting for financial transactions...");
            System.out.println("============================================");

            Thread.currentThread().join();

        } catch (Exception e) {
            System.out.println("Finance Server Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
