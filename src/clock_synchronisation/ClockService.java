package clock_synchronisation;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClockService extends Remote {
    ClockResponse synchronize(long workerLamport) throws RemoteException;
}