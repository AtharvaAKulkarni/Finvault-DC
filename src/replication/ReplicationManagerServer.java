package replication;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.LinkedHashMap;
import java.util.Map;

public class ReplicationManagerServer
        extends UnicastRemoteObject
        implements ReplicationService {

    private static final int RMI_PORT = 7000;

    private static final int HEALTH_CHECK_INTERVAL = 5000;

    private static final int FAILURE_THRESHOLD = 3;

    /*
     * =====================================================
     * CLUSTER NODES
     * =====================================================
     *
     * Node 1 -> finance-server-1
     * Node 2 -> finance-server-2
     * Node 3 -> finance-server-3
     * Node 4 -> finance-server-4
     */

    private final Map<Integer, String> nodes =
            new LinkedHashMap<>();

    /*
     * Current Primary node ID.
     */
    private volatile int primaryId;


    /*
     * Prevent two elections from running together.
     */
    private volatile boolean electionInProgress = false;


    /*
     * =====================================================
     * CONSTRUCTOR
     * =====================================================
     */

    public ReplicationManagerServer()
            throws RemoteException {

        super();

        loadNodes();

        primaryId =
                Integer.parseInt(
                        System.getenv().getOrDefault(
                                "PRIMARY_ID",
                                "1"
                        )
                );

        System.out.println();
        System.out.println(
                "============================================"
        );

        System.out.println(
                "      FINVAULT REPLICATION MANAGER"
        );

        System.out.println(
                "============================================"
        );

        System.out.println(
                "[Cluster] Node 1 = "
                        + nodes.get(1)
        );

        System.out.println(
                "[Cluster] Node 2 = "
                        + nodes.get(2)
        );

        System.out.println(
                "[Cluster] Node 3 = "
                        + nodes.get(3)
        );

        System.out.println(
                "[Cluster] Node 4 = "
                        + nodes.get(4)
        );

        System.out.println(
                "[Cluster] Current Primary = Node "
                        + primaryId
        );
    }


    /*
     * =====================================================
     * LOAD NODE CONFIGURATION
     * =====================================================
     */

    private void loadNodes() {

        nodes.put(
                1,
                System.getenv().getOrDefault(
                        "NODE1_HOST",
                        "finance-server-1"
                )
        );

        nodes.put(
                2,
                System.getenv().getOrDefault(
                        "NODE2_HOST",
                        "finance-server-2"
                )
        );

        nodes.put(
                3,
                System.getenv().getOrDefault(
                        "NODE3_HOST",
                        "finance-server-3"
                )
        );

        nodes.put(
                4,
                System.getenv().getOrDefault(
                        "NODE4_HOST",
                        "finance-server-4"
                )
        );
    }


    /*
     * =====================================================
     * LOOK UP FINANCE SERVER
     * =====================================================
     */

    private ReplicationService getServer(
            int nodeId)
            throws Exception {

        String host =
                nodes.get(nodeId);

        if (host == null) {

            throw new Exception(
                    "Unknown node ID: "
                            + nodeId
            );
        }

        return (ReplicationService)
                Naming.lookup(
                        "rmi://"
                                + host
                                + ":1234/"
                                + "replication.ReplicationService"
                );
    }


    /*
     * =====================================================
     * SYNCHRONOUS TRANSACTION REPLICATION
     * =====================================================
     */

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
                "[Replication Manager]"
        );

        System.out.println(
                "Replicating TXN"
                        + transactionId
        );

        System.out.println(
                "Primary Node : "
                        + primaryId
        );

        System.out.println(
                "============================================"
        );

        /*
         * Find a Secondary.
         *
         * Start with the lowest node ID that
         * is not the Primary.
         */

        for (int nodeId : nodes.keySet()) {

            if (nodeId == primaryId) {
                continue;
            }

            try {

                ReplicationService secondary =
                        getServer(nodeId);

                if (secondary.getRole()
                        != NodeRole.SECONDARY) {

                    continue;
                }

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
                                    + "ACK received from Node "
                                    + nodeId
                    );

                    return result;
                }

                System.out.println(
                        "[Replication Manager] "
                                + "Node "
                                + nodeId
                                + " rejected replication: "
                                + result.getMessage()
                );

            } catch (Exception e) {

                System.out.println(
                        "[Replication Manager] "
                                + "Node "
                                + nodeId
                                + " unavailable: "
                                + e.getMessage()
                );
            }
        }

        return new ReplicationResult(
                false,
                transactionId,
                "No available Secondary."
        );
    }


    /*
     * =====================================================
     * GET PRIMARY SNAPSHOT
     * =====================================================
     */

    @Override
    public AccountStateSnapshot getStateSnapshot()
            throws RemoteException {

        try {

            ReplicationService primary =
                    getServer(primaryId);

            return primary.getStateSnapshot();

        } catch (Exception e) {

            throw new RemoteException(
                    "Unable to obtain Primary snapshot.",
                    e
            );
        }
    }


    /*
     * =====================================================
     * APPLY SNAPSHOT
     * =====================================================
     */

    @Override
    public void applyStateSnapshot(
            AccountStateSnapshot snapshot)
            throws RemoteException {

        boolean applied = false;

        for (int nodeId : nodes.keySet()) {

            if (nodeId == primaryId) {
                continue;
            }

            try {

                ReplicationService server =
                        getServer(nodeId);

                if (server.getRole()
                        != NodeRole.SECONDARY) {

                    continue;
                }

                server.applyStateSnapshot(
                        snapshot
                );

                System.out.println(
                        "[Replication Manager] "
                                + "Snapshot applied to Node "
                                + nodeId
                );

                applied = true;

            } catch (Exception e) {

                System.out.println(
                        "[Replication Manager] "
                                + "Snapshot failed for Node "
                                + nodeId
                );
            }
        }

        if (!applied) {

            throw new RemoteException(
                    "No Secondary available."
            );
        }
    }


    /*
     * =====================================================
     * FULL STATE SYNCHRONIZATION
     * =====================================================
     */

    public void synchronizeFullState() {

        try {

            ReplicationService primary =
                    getServer(primaryId);

            AccountStateSnapshot snapshot =
                    primary.getStateSnapshot();

            System.out.println();
            System.out.println(
                    "[FULL STATE SYNCHRONIZATION]"
            );

            for (int nodeId : nodes.keySet()) {

                if (nodeId == primaryId) {
                    continue;
                }

                try {

                    ReplicationService secondary =
                            getServer(nodeId);

                    if (secondary.getRole()
                            == NodeRole.SECONDARY) {

                        secondary.applyStateSnapshot(
                                snapshot
                        );

                        System.out.println(
                                "[Replication Manager] "
                                        + "Node "
                                        + nodeId
                                        + " synchronized."
                        );
                    }

                } catch (Exception e) {

                    System.out.println(
                            "[Replication Manager] "
                                    + "Unable to synchronize Node "
                                    + nodeId
                    );
                }
            }

        } catch (Exception e) {

            System.out.println(
                    "[Replication Manager] "
                            + "Full synchronization failed: "
                            + e.getMessage()
            );
        }
    }


    /*
     * =====================================================
     * CONSISTENCY CHECK
     * =====================================================
     */

    public boolean checkConsistency() {

        try {

            ReplicationService primary =
                    getServer(primaryId);

            AccountStateSnapshot primarySnapshot =
                    primary.getStateSnapshot();

            boolean consistent = true;

            System.out.println();
            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "[CONSISTENCY CHECK]"
            );

            System.out.println(
                    "Primary Node = "
                            + primaryId
            );

            for (int nodeId : nodes.keySet()) {

                if (nodeId == primaryId) {
                    continue;
                }

                try {

                    ReplicationService secondary =
                            getServer(nodeId);

                    if (secondary.getRole()
                            != NodeRole.SECONDARY) {

                        continue;
                    }

                    AccountStateSnapshot secondarySnapshot =
                            secondary.getStateSnapshot();

                    boolean balancesMatch =
                            primarySnapshot
                                    .getAccountBalances()
                                    .equals(
                                            secondarySnapshot
                                                    .getAccountBalances()
                                    );

                    boolean counterMatches =
                            primarySnapshot
                                    .getTransactionCounter()
                                    ==
                                    secondarySnapshot
                                            .getTransactionCounter();

                    boolean nodeConsistent =
                            balancesMatch
                                    && counterMatches;

                    System.out.println(
                            "Node "
                                    + nodeId
                                    + " consistency = "
                                    + nodeConsistent
                    );

                    if (!nodeConsistent) {
                        consistent = false;
                    }

                } catch (Exception e) {

                    consistent = false;

                    System.out.println(
                            "Node "
                                    + nodeId
                                    + " unavailable."
                    );
                }
            }

            System.out.println(
                    "OVERALL CONSISTENCY = "
                            + consistent
            );

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


    /*
     * =====================================================
     * MANAGER PING
     * =====================================================
     */

    @Override
    public boolean ping()
            throws RemoteException {

        return true;
    }


    /*
     * =====================================================
     * GET PRIMARY ROLE
     * =====================================================
     */

    @Override
    public NodeRole getRole()
            throws RemoteException {

        try {

            return getServer(primaryId)
                    .getRole();

        } catch (Exception e) {

            throw new RemoteException(
                    "Unable to obtain Primary role.",
                    e
            );
        }
    }


    /*
     * =====================================================
     * GET NODE ID
     * =====================================================
     */

    @Override
    public int getNodeId()
            throws RemoteException {

        return 0;
    }


    /*
     * =====================================================
     * MANUAL PROMOTION
     * =====================================================
     */

    @Override
    public void promoteToPrimary()
            throws RemoteException {

        startElection();
    }


    /*
     * =====================================================
     * START BULLY ELECTION
     * =====================================================
     */

    @Override
    public synchronized void startElection()
            throws RemoteException {

        if (electionInProgress) {

            System.out.println(
                    "[Election] Election already running."
            );

            return;
        }

        electionInProgress = true;

        System.out.println();
        System.out.println(
                "============================================"
        );

        System.out.println(
                "[BULLY ELECTION]"
        );

        System.out.println(
                "Current Primary Node "
                        + primaryId
                        + " is unavailable."
        );

        System.out.println(
                "Searching for highest available node..."
        );

        System.out.println(
                "============================================"
        );

        int winner = findHighestAliveNode();

        if (winner == -1) {

            System.out.println(
                    "[Election] No Finance Server is available."
            );

            electionInProgress = false;

            return;
        }

        System.out.println(
                "[Election] Winner = Node "
                        + winner
        );

        announceWinner(winner);

        electionInProgress = false;
    }


    /*
     * =====================================================
     * FIND HIGHEST AVAILABLE NODE
     * =====================================================
     */

    private int findHighestAliveNode() {

        /*
         * Bully rule:
         *
         * Highest node ID that is alive wins.
         */

        for (int nodeId = 4;
             nodeId >= 1;
             nodeId--) {

            if (nodeId == primaryId) {
                continue;
            }

            try {

                ReplicationService server =
                        getServer(nodeId);

                if (server.ping()) {

                    System.out.println(
                            "[Election] Node "
                                    + nodeId
                                    + " is alive."
                    );

                    return nodeId;
                }

            } catch (Exception e) {

                System.out.println(
                        "[Election] Node "
                                + nodeId
                                + " is unavailable."
                );
            }
        }

        return -1;
    }


    /*
     * =====================================================
     * ELECTION MESSAGE
     * =====================================================
     */

    @Override
    public void electionMessage(
            int candidateId)
            throws RemoteException {

        System.out.println(
                "[Election] Candidate Node "
                        + candidateId
                        + " requested election."
        );

        if (candidateId > primaryId) {

            primaryId = candidateId;

            announceWinner(candidateId);
        }
    }


    /*
     * =====================================================
     * ELECTION WINNER
     * =====================================================
     */

    @Override
    public synchronized void electionWon(
            int newPrimaryId)
            throws RemoteException {

        announceWinner(newPrimaryId);
    }


    /*
     * =====================================================
     * ANNOUNCE WINNER
     * =====================================================
     */

    private synchronized void announceWinner(
            int newPrimaryId) {

        primaryId = newPrimaryId;

        System.out.println();
        System.out.println(
                "============================================"
        );

        System.out.println(
                "[BULLY ELECTION RESULT]"
        );

        System.out.println(
                "NEW PRIMARY = Node "
                        + newPrimaryId
        );

        System.out.println(
                "============================================"
        );

        /*
         * Tell every Finance Server about
         * the new Primary.
         */

        for (int nodeId : nodes.keySet()) {

            try {

                ReplicationService server =
                        getServer(nodeId);

                server.updatePrimary(
                        newPrimaryId
                );

            } catch (Exception e) {

                System.out.println(
                        "[Election] Unable to update Node "
                                + nodeId
                );
            }
        }

        /*
         * Explicitly promote the winner.
         */

        try {

            ReplicationService winner =
                    getServer(newPrimaryId);

            winner.promoteToPrimary();

        } catch (Exception e) {

            System.out.println(
                    "[Election] Winner promotion failed: "
                            + e.getMessage()
            );
        }
    }


    /*
     * =====================================================
     * UPDATE PRIMARY
     * =====================================================
     */

    @Override
    public void updatePrimary(
            int newPrimaryId)
            throws RemoteException {

        primaryId = newPrimaryId;

        System.out.println(
                "[Manager] Primary updated to Node "
                        + newPrimaryId
        );
    }


    /*
     * =====================================================
     * HEALTH MONITOR
     * =====================================================
     */

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

                            try {

                                ReplicationService primary =
                                        getServer(primaryId);

                                boolean alive =
                                        primary.ping();

                                if (alive) {

                                    consecutiveFailures = 0;

                                    System.out.println(
                                            "[Health Monitor] "
                                                    + "Primary Node "
                                                    + primaryId
                                                    + " healthy."
                                    );

                                } else {

                                    consecutiveFailures++;
                                }

                            } catch (Exception e) {

                                consecutiveFailures++;

                                System.out.println(
                                        "[Health Monitor] "
                                                + "Primary Node "
                                                + primaryId
                                                + " failed "
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
                                                + "PRIMARY FAILURE DETECTED."
                                );

                                startElection();

                                consecutiveFailures = 0;
                            }

                        } catch (
                                InterruptedException e) {

                            Thread.currentThread()
                                    .interrupt();

                            break;
                        }
                    }
                });

        healthThread.setDaemon(true);

        healthThread.start();
    }


    /*
     * =====================================================
     * MAIN
     * =====================================================
     */

    public static void main(
            String[] args) {

        try {

            System.setProperty(
                    "java.rmi.server.hostname",
                    System.getenv().getOrDefault(
                            "RMI_HOSTNAME",
                            "replication-manager"
                    )
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

            manager.synchronizeFullState();

            manager.startHealthMonitor();

            System.out.println(
                    "Replication Manager is running."
            );

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