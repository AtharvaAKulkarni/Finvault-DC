import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AccountStateSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, Double> accountBalances;
    private final long transactionCounter;

    public AccountStateSnapshot(
            Map<String, Double> accountBalances,
            long transactionCounter) {

        // Create a copy so the snapshot is independent
        // of the original server map.
        this.accountBalances =
                new HashMap<>(accountBalances);

        this.transactionCounter = transactionCounter;
    }

    public Map<String, Double> getAccountBalances() {
        return new HashMap<>(accountBalances);
    }

    public long getTransactionCounter() {
        return transactionCounter;
    }

    @Override
    public String toString() {
        return "AccountStateSnapshot{" +
                "accountBalances=" + accountBalances +
                ", transactionCounter=" + transactionCounter +
                '}';
    }
}