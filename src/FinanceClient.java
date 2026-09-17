import java.rmi.Naming;

public class FinanceClient {

    private static final String RMI_HOST =
            System.getenv().getOrDefault(
                    "FINANCE_SERVER_HOST",
                    "localhost"
            );

    private static final String RMI_URL =
            "rmi://" + RMI_HOST + ":1234/FinanceServer";

    public static void main(String[] args) {
        try {
            System.out.println("============================================");
            System.out.println(" FINVAULT FINANCE CLIENT");
            System.out.println("============================================");

            FinanceService finance =
                    (FinanceService) Naming.lookup(RMI_URL);

            System.out.println("RMI Lookup Successful.");
            System.out.println("Connected to FinVault Finance Server.");

            // Three simulated customers generate requests concurrently.
            Thread customer1 = new Thread(() -> {
                try {
                    System.out.println("[Customer 1] Initiating transfer from ACC101");
                    String response = finance.processTransaction(
                            "CUST001", "ACC101", "ACC102", 5000);
                    System.out.println("[Customer 1] Server Response: " + response);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            Thread customer2 = new Thread(() -> {
                try {
                    System.out.println("[Customer 2] Initiating transfer from ACC103");
                    String response = finance.processTransaction(
                            "CUST002", "ACC103", "ACC104", 8000);
                    System.out.println("[Customer 2] Server Response: " + response);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            Thread customer3 = new Thread(() -> {
                try {
                    System.out.println("[Customer 3] Initiating transfer from ACC102");
                    String response = finance.processTransaction(
                            "CUST003", "ACC102", "ACC103", 3000);
                    System.out.println("[Customer 3] Server Response: " + response);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            customer1.start();
            customer2.start();
            customer3.start();

            customer1.join();
            customer2.join();
            customer3.join();

            System.out.println();
            System.out.println("============================================");
            System.out.println("All customer transactions have been submitted.");
            System.out.println("============================================");

            // Give background worker threads time to finish for demonstration.
            Thread.sleep(2500);

            System.out.println("\nFinal account balances:");
            System.out.println("ACC101 : Rs. " + finance.getBalance("ACC101"));
            System.out.println("ACC102 : Rs. " + finance.getBalance("ACC102"));
            System.out.println("ACC103 : Rs. " + finance.getBalance("ACC103"));
            System.out.println("ACC104 : Rs. " + finance.getBalance("ACC104"));

        } catch (Exception e) {
            System.out.println("Client Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
