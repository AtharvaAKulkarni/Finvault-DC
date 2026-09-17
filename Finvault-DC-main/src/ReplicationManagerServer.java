import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Map;

public class ReplicationManagerServer
        extends UnicastRemoteObject
        implements ReplicationService {

    private static final int RMI_PORT = 7000;

    private volatile String primaryHost;
    private volatile String secondaryHost;

    private static final int HEALTH_CHECK_INTERVAL = 5000;
    private static final int FAILURE_THRESHOLD = 3;

    private volatile boolean failoverCompleted = false;


    public ReplicationManagerServer()
            throws RemoteException {

        super();

        primaryHost =
                System.getenv().getOrDefault(
                        "PRIMARY_HOST",
                        "finance-server-1"
                );

        secondaryHost =
                System.getenv().getOrDefault(
                        "SECONDARY_HOST",
                        "finance-server-2"
                );

        System.out.println(
                "[Replication Manager] Primary   : "
                        + primaryHost
        );

        System.out.println(
                "[Replication Manager] Secondary : "
                        + secondaryHost
        );
    }


    // ---------------------------------------------------------
    // LOOKUP FINANCE SERVER
    // ---------------------------------------------------------

    private ReplicationService getServer(
            String host) throws Exception {

        return (ReplicationService) Naming.lookup(
                "rmi://" + host
                        + ":1234/ReplicationService"
        );
    }


    // ---------------------------------------------------------
    // SYNCHRONOUS TRANSACTION REPLICATION
    // ---------------------------------------------------------

    @Override
    public ReplicationResult replicateTransaction(
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
                "============================================"
        );
        System.out.println(
                "[REPLICATION MANAGER]"
        );
        System.out.println(
                "Replicating TXN" + transactionId
        );
        System.out.println(
                "Primary   : " + primaryHost
        );
        System.out.println(
                "Secondary : " + secondaryHost
        );
        System.out.println(
                "============================================"
        );

        try {

            ReplicationService secondary =
                    getServer(secondaryHost);

            ReplicationResult result =
                    secondary.replicateTransaction(
                            transactionId,
                            customerId,
                            fromAccount,
                            toAccount,
                            amount,
                            physicalTimestamp,
                            lamportTimestamp
                    );

            if (result.isSuccess()) {

                System.out.println(
                        "[Replication Manager] "
                                + "Secondary ACK received."
                );

                System.out.println(
                        "[Replication Manager] "
                                + "TXN" + transactionId
                                + " replicated successfully."
                );

            } else {

                System.out.println(
                        "[Replication Manager] "
                                + "Replication failed: "
                                + result.getMessage()
                );
            }

            return result;

        } catch (Exception e) {

            System.out.println(
                    "[Replication Manager] "
                            + "Replication error: "
                            + e.getMessage()
            );

            return new ReplicationResult(
                    false,
                    "Replication failed: "
                            + e.getMessage()
            );
        }
    }


    // ---------------------------------------------------------
    // GET PRIMARY STATE
    // ---------------------------------------------------------

    @Override
    public AccountStateSnapshot getStateSnapshot()
            throws RemoteException {

        try {

            ReplicationService primary =
                    getServer(primaryHost);

            return primary.getStateSnapshot();

        } catch (Exception e) {

            throw new RemoteException(
                    "Unable to obtain Primary snapshot.",
                    e
            );
        }
    }


    // ---------------------------------------------------------
    // FULL STATE SYNCHRONIZATION
    // ---------------------------------------------------------

    @Override
    public void applyStateSnapshot(
            AccountStateSnapshot snapshot)
            throws RemoteException {

        try {

            ReplicationService secondary =
                    getServer(secondaryHost);

            secondary.applyStateSnapshot(snapshot);

            System.out.println(
                    "[Replication Manager] "
                            + "Full state snapshot applied "
                            + "to Secondary."
            );

        } catch (Exception e) {

            throw new RemoteException(
                    "Snapshot synchronization failed.",
                    e
            );
        }
    }


    // ---------------------------------------------------------
    // SYNCHRONIZE PRIMARY -> SECONDARY
    // ---------------------------------------------------------

    public void synchronizeFullState() {

        try {

            System.out.println();
            System.out.println(
                    "============================================"
            );
            System.out.println(
                    "[FULL STATE SYNCHRONIZATION]"
            );
            System.out.println(
                    "============================================"
            );

            ReplicationService primary =
                    getServer(primaryHost);

            ReplicationService secondary =
                    getServer(secondaryHost);

            AccountStateSnapshot snapshot =
                    primary.getStateSnapshot();

            secondary.applyStateSnapshot(snapshot);

            System.out.println(
                    "[Replication Manager] "
                            + "Primary state copied to Secondary."
            );

            System.out.println(
                    "============================================"
            );

        } catch (Exception e) {

            System.out.println(
                    "[Replication Manager] "
                            + "Full synchronization failed: "
                            + e.getMessage()
            );
        }
    }


    // ---------------------------------------------------------
    // CONSISTENCY CHECK
    // ---------------------------------------------------------

    public boolean checkConsistency() {

        try {

            ReplicationService primary =
                    getServer(primaryHost);

            ReplicationService secondary =
                    getServer(secondaryHost);

            AccountStateSnapshot primarySnapshot =
                    primary.getStateSnapshot();

            AccountStateSnapshot secondarySnapshot =
                    secondary.getStateSnapshot();

            Map<String, Double> primaryBalances =
                    primarySnapshot.getAccountBalances();

            Map<String, Double> secondaryBalances =
                    secondarySnapshot.getAccountBalances();

            boolean balancesMatch =
                    primaryBalances.equals(
                            secondaryBalances
                    );

            boolean counterMatches =
                    primarySnapshot.getTransactionCounter()
                            ==
                    secondarySnapshot.getTransactionCounter();

            boolean consistent =
                    balancesMatch && counterMatches;

            System.out.println();
            System.out.println(
                    "============================================"
            );
            System.out.println(
                    "[CONSISTENCY CHECK]"
            );
            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "Primary balances   : "
                            + primaryBalances
            );

            System.out.println(
                    "Secondary balances : "
                            + secondaryBalances
            );

            System.out.println(
                    "Primary counter    : "
                            + primarySnapshot
                            .getTransactionCounter()
            );

            System.out.println(
                    "Secondary counter  : "
                            + secondarySnapshot
                            .getTransactionCounter()
            );

            if (consistent) {

                System.out.println(
                        "CONSISTENCY: PASS"
                );

            } else {

                System.out.println(
                        "CONSISTENCY: FAIL"
                );
            }

            System.out.println(
                    "============================================"
            );

            return consistent;

        } catch (Exception e) {

            System.out.println(
                    "[Replication Manager] "
                            + "Consistency check failed: "
                            + e.getMessage()
            );

            return false;
        }
    }


    // ---------------------------------------------------------
    // MANAGER HEALTH
    // ---------------------------------------------------------

    @Override
    public boolean ping()
            throws RemoteException {

        return true;
    }


    // ---------------------------------------------------------
    // MANAGER DOES NOT HAVE A FINANCE NODE ROLE
    // ---------------------------------------------------------

    @Override
    public NodeRole getRole()
            throws RemoteException {

        try {

            return getServer(primaryHost)
                    .getRole();

        } catch (Exception e) {

            throw new RemoteException(
                    "Unable to obtain Primary role.",
                    e
            );
        }
    }


    // ---------------------------------------------------------
    // MANUAL FAILOVER ENTRY POINT
    // ---------------------------------------------------------

    @Override
    public void promoteToPrimary()
            throws RemoteException {

        triggerFailover();
    }


    // ---------------------------------------------------------
    // HEALTH CHECK THREAD
    // ---------------------------------------------------------

    private void startHealthMonitor() {

        Thread healthThread =
                new Thread(() -> {

                    int consecutiveFailures = 0;

                    System.out.println(
                            "[Health Monitor] Started."
                    );

                    while (true) {

                        try {

                            Thread.sleep(
                                    HEALTH_CHECK_INTERVAL
                            );

                            if (failoverCompleted) {
                                continue;
                            }

                            try {

                                ReplicationService primary =
                                        getServer(primaryHost);

                                boolean alive =
                                        primary.ping();

                                if (alive) {

                                    consecutiveFailures = 0;

                                    System.out.println(
                                            "[Health Monitor] "
                                                    + "Primary healthy: "
                                                    + primaryHost
                                    );

                                } else {

                                    consecutiveFailures++;
                                }

                            } catch (Exception e) {

                                consecutiveFailures++;

                                System.out.println(
                                        "[Health Monitor] "
                                                + "Primary check failed "
                                                + "("
                                                + consecutiveFailures
                                                + "/"
                                                + FAILURE_THRESHOLD
                                                + ")"
                                );
                            }

                            if (consecutiveFailures
                                    >= FAILURE_THRESHOLD) {

                                System.out.println(
                                        "[Health Monitor] "
                                                + "Primary failure detected."
                                );

                                triggerFailover();

                                consecutiveFailures = 0;
                            }

                        } catch (InterruptedException e) {

                            Thread.currentThread()
                                    .interrupt();

                            break;
                        }
                    }
                });

        healthThread.setDaemon(true);
        healthThread.start();
    }


    // ---------------------------------------------------------
    // FAILOVER
    // ---------------------------------------------------------

    private synchronized void triggerFailover() {

        if (failoverCompleted) {
            return;
        }

        System.out.println();
        System.out.println(
                "============================================"
        );
        System.out.println(
                "[FAILOVER]"
        );
        System.out.println(
                "Primary unavailable: "
                        + primaryHost
        );
        System.out.println(
                "Attempting to promote Secondary: "
                        + secondaryHost
        );
        System.out.println(
                "============================================"
        );

        try {

            ReplicationService secondary =
                    getServer(secondaryHost);

            if (!secondary.ping()) {

                System.out.println(
                        "[FAILOVER] Secondary is unavailable."
                );

                return;
            }

            secondary.promoteToPrimary();

            String oldPrimary =
                    primaryHost;

            primaryHost =
                    secondaryHost;

            secondaryHost =
                    oldPrimary;

            failoverCompleted = true;

            System.out.println(
                    "[FAILOVER] Secondary promoted successfully."
            );

            System.out.println(
                    "[FAILOVER] New Primary: "
                            + primaryHost
            );

            System.out.println(
                    "============================================"
            );

        } catch (Exception e) {

            System.out.println(
                    "[FAILOVER] Failed: "
                            + e.getMessage()
            );
        }
    }


    // ---------------------------------------------------------
    // MAIN
    // ---------------------------------------------------------

    public static void main(String[] args) {

        try {

            System.setProperty(
                    "java.rmi.server.hostname",
                    System.getenv().getOrDefault(
                            "RMI_HOSTNAME",
                            "replication-manager"
                    )
            );

            System.out.println(
                    "============================================"
            );
            System.out.println(
                    "      FINVAULT REPLICATION MANAGER"
            );
            System.out.println(
                    "============================================"
            );

            ReplicationManagerServer manager =
                    new ReplicationManagerServer();

            Registry registry =
                    LocateRegistry.createRegistry(
                            RMI_PORT
                    );

            registry.rebind(
                    "ReplicationManager",
                    manager
            );

            System.out.println(
                    "Replication Manager RMI Registry "
                            + "started on port "
                            + RMI_PORT
            );

            System.out.println(
                    "Waiting for Finance Servers..."
            );

            manager.startHealthMonitor();

            Thread.currentThread().join();

        } catch (Exception e) {

            System.out.println(
                    "Replication Manager Error: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}