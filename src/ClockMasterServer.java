import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ClockMasterServer
        extends UnicastRemoteObject
        implements ClockService {
    private final LamportClock lamportClock = new LamportClock();
    public ClockMasterServer() throws RemoteException {
        super();
    }

    @Override
    public ClockResponse synchronize(long workerLamport)
            throws RemoteException {

        // Receive event at master
        long masterLamport =
                lamportClock.receiveEvent(workerLamport);

        long serverTime = System.currentTimeMillis();

        System.out.println(
                "[CLOCK MASTER] Request received"
                        + " | Worker L=" + workerLamport
                        + " | Master L=" + masterLamport
                        + " | Physical Time=" + serverTime
        );

        // Sending response is another Lamport event
        long responseLamport =
                lamportClock.sendEvent();

        System.out.println(
                "[CLOCK MASTER] Sending response"
                        + " | Master L=" + responseLamport
        );

        return new ClockResponse(
                serverTime,
                responseLamport
        );
    }

    public static void main(String[] args) {

        try {
            System.setProperty(
                    "java.rmi.server.hostname",
                    "clock-master"
            );

            System.out.println("============================================");
            System.out.println("       FINVAULT CLOCK MASTER");
            System.out.println("============================================");

            ClockMasterServer clockMaster =
                    new ClockMasterServer();

            Registry registry =
                    LocateRegistry.createRegistry(6000);

            registry.rebind("ClockService", clockMaster);

            System.out.println(
                    "Clock Master RMI Registry started on port 6000."
            );

            System.out.println(
                    "Reference Physical Time: "
                            + System.currentTimeMillis()
            );

            System.out.println(
                    "Waiting for clock synchronization requests..."
            );

            Thread.currentThread().join();

        } catch (Exception e) {

            System.out.println(
                    "Clock Master Error: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}