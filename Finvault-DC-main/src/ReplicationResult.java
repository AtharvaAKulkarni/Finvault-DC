import java.io.Serializable;

public class ReplicationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean success;
    private final long transactionId;
    private final String message;


    /*
     * ---------------------------------------------------
     * 3-ARGUMENT CONSTRUCTOR
     *
     * Used by FinanceServer and ReplicationManagerServer
     *
     * Example:
     *
     * new ReplicationResult(
     *     true,
     *     1001,
     *     "Transaction replicated successfully."
     * );
     * ---------------------------------------------------
     */

    public ReplicationResult(
            boolean success,
            long transactionId,
            String message) {

        this.success = success;
        this.transactionId = transactionId;
        this.message = message;
    }


    /*
     * ---------------------------------------------------
     * 2-ARGUMENT CONSTRUCTOR
     *
     * Kept for compatibility in case another class
     * in your project uses the older format.
     *
     * Example:
     *
     * new ReplicationResult(
     *     true,
     *     "Replication successful"
     * );
     * ---------------------------------------------------
     */

    public ReplicationResult(
            boolean success,
            String message) {

        this.success = success;
        this.transactionId = -1;
        this.message = message;
    }


    /*
     * ---------------------------------------------------
     * GET SUCCESS STATUS
     * ---------------------------------------------------
     */

    public boolean isSuccess() {
        return success;
    }


    /*
     * ---------------------------------------------------
     * GET TRANSACTION ID
     * ---------------------------------------------------
     */

    public long getTransactionId() {
        return transactionId;
    }


    /*
     * ---------------------------------------------------
     * GET MESSAGE
     * ---------------------------------------------------
     */

    public String getMessage() {
        return message;
    }


    /*
     * ---------------------------------------------------
     * TO STRING
     * ---------------------------------------------------
     */

    @Override
    public String toString() {

        return "ReplicationResult{" +
                "success=" + success +
                ", transactionId=" + transactionId +
                ", message='" + message + '\'' +
                '}';
    }
}