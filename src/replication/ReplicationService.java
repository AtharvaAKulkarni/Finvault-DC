package replication;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ReplicationService extends Remote {

    /*
     * =====================================================
     * SYNCHRONOUS TRANSACTION REPLICATION
     * =====================================================
     */

    ReplicationResult replicateTransaction(
            long transactionId,
            String customerId,
            String fromAccount,
            String toAccount,
            double amount,
            long physicalTimestamp,
            long lamportTimestamp)
            throws RemoteException;


    /*
     * =====================================================
     * FULL STATE SNAPSHOT
     * =====================================================
     */

    AccountStateSnapshot getStateSnapshot()
            throws RemoteException;


    void applyStateSnapshot(
            AccountStateSnapshot snapshot)
            throws RemoteException;


    /*
     * =====================================================
     * HEALTH CHECK
     * =====================================================
     */

    boolean ping()
            throws RemoteException;


    /*
     * =====================================================
     * NODE INFORMATION
     * =====================================================
     */

    NodeRole getRole()
            throws RemoteException;

    int getNodeId()
            throws RemoteException;


    /*
     * =====================================================
     * FAILOVER
     * =====================================================
     */

    void promoteToPrimary()
            throws RemoteException;


    /*
     * =====================================================
     * BULLY ELECTION
     * =====================================================
     */

    void startElection()
            throws RemoteException;

    void electionMessage(
            int candidateId)
            throws RemoteException;

    void electionWon(
            int newPrimaryId)
            throws RemoteException;

    void updatePrimary(
            int newPrimaryId)
            throws RemoteException;
}