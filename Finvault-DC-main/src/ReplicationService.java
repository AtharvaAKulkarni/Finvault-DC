import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ReplicationService extends Remote {

    /*
     * Apply an already-approved financial transaction
     * on a replica.
     *
     * This method is NOT normal client transaction processing.
     * It is used only for replication.
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
     * Return the complete current financial state
     * of this Finance Server.
     */
    AccountStateSnapshot getStateSnapshot()
            throws RemoteException;


    /*
     * Replace the local financial state with
     * the supplied snapshot.
     */
    void applyStateSnapshot(
            AccountStateSnapshot snapshot)
            throws RemoteException;


    /*
     * Used by the Replication Manager
     * for health checking.
     */
    boolean ping()
            throws RemoteException;


    /*
     * Return the current role of this node.
     */
    NodeRole getRole()
            throws RemoteException;


    /*
     * Promote a Secondary to Primary
     * during failover.
     */
    void promoteToPrimary()
            throws RemoteException;
}