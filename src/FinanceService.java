import java.rmi.Remote;
import java.rmi.RemoteException;

public interface FinanceService extends Remote {
    String processTransaction(String customerId, String fromAccount,
                              String toAccount, double amount)
            throws RemoteException;

    double getBalance(String accountNumber) throws RemoteException;
}
