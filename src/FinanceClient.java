import load_balancing.LoadBalancerService;

import java.rmi.Naming;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.Arrays;

public class FinanceClient {

    private static final String RMI_HOST =
            System.getenv().getOrDefault("LOAD_BALANCER_HOST", "localhost");

    private static final String RMI_URL =
            "rmi://" + RMI_HOST + ":8000/LoadBalancer";

    // Track initial accounts + dynamically added accounts
    private static final Set<String> knownAccounts = new TreeSet<>(
            Arrays.asList("ACC101", "ACC102", "ACC103", "ACC104")
    );

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("            FINVAULT BANKING CLIENT              ");
        System.out.println("=================================================");
        System.out.println("Connecting to Load Balancer at: " + RMI_URL);

        LoadBalancerService lbService = null;
        try {
            lbService = (LoadBalancerService) Naming.lookup(RMI_URL);
            System.out.println("[SUCCESS] Connected to FinVault Cluster!\n");
        } catch (Exception e) {
            System.err.println("[ERROR] Could not connect to Load Balancer at " + RMI_URL);
            System.err.println("Ensure the FinVault cluster is running properly.");
            System.exit(1);
        }

        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            printMenu();
            System.out.print("Select Option (0-7): ");
            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    createAccount(lbService, scanner);
                    break;
                case "2":
                    executeTransaction(lbService, scanner);
                    break;
                case "3":
                    showLoadBalancing(lbService);
                    break;
                case "4":
                    showReplication(lbService);
                    break;
                case "5":
                    showMultithreading(lbService);
                    break;
                case "6":
                    showFaultTolerance();
                    break;
                case "7":
                    runSystemDiagnostic(lbService);
                    break;
                case "0":
                    running = false;
                    System.out.println("Exiting FinVault Client. Goodbye!");
                    break;
                default:
                    System.out.println("Invalid option. Please choose between 0 and 7.");
            }

            if (running) {
                System.out.println("\nPress Enter to return to the main menu...");
                scanner.nextLine();
            }
        }
        scanner.close();
    }

    private static void printMenu() {
        System.out.println("\n=================================================");
        System.out.println("                  MAIN MENU                      ");
        System.out.println("=================================================");
        System.out.println("1. Create Account");
        System.out.println("2. Execute Transaction");
        System.out.println("3. Show Load Balancing");
        System.out.println("4. Show Replication");
        System.out.println("5. Show Multithreading");
        System.out.println("6. Show Fault Tolerance");
        System.out.println("7. Run Full System Diagnostic");
        System.out.println("0. Exit Client");
        System.out.println("=================================================");
    }

    /* -------------------------------------------------------------------
     * 1. CREATE ACCOUNT
     * ------------------------------------------------------------------- */
    private static void createAccount(LoadBalancerService lb, Scanner scanner) {
        System.out.println("\n--- [Create New Account] ---");
        try {
            System.out.print("Enter Account Number (default ACC105): ");
            String accNum = scanner.nextLine().trim();
            if (accNum.isEmpty()) accNum = "ACC105";

            System.out.print("Enter Initial Balance (default 25000): ");
            String balStr = scanner.nextLine().trim();
            double initialBal = balStr.isEmpty() ? 25000.0 : Double.parseDouble(balStr);

            System.out.println("\nSending account creation request...");
            String response = lb.createAccount(accNum, initialBal);
            System.out.println("Server Response: " + response);

            if (response.startsWith("SUCCESS")) {
                knownAccounts.add(accNum); // Add newly created account dynamically
            }

            double balance = lb.getBalance(accNum);
            System.out.println("Verified Balance for " + accNum + ": Rs. " + balance);
        } catch (Exception e) {
            System.err.println("Account Creation Error: " + e.getMessage());
        }
    }

    /* -------------------------------------------------------------------
     * 2. EXECUTE TRANSACTION
     * ------------------------------------------------------------------- */
    private static void executeTransaction(LoadBalancerService lb, Scanner scanner) {
        System.out.println("\n--- [Execute Transaction] ---");
        try {
            System.out.print("Enter Customer ID (default CUST101): ");
            String custId = scanner.nextLine().trim();
            if (custId.isEmpty()) custId = "CUST101";

            System.out.print("Enter Source Account (default ACC101): ");
            String from = scanner.nextLine().trim();
            if (from.isEmpty()) from = "ACC101";

            System.out.print("Enter Destination Account (default ACC102): ");
            String to = scanner.nextLine().trim();
            if (to.isEmpty()) to = "ACC102";

            System.out.print("Enter Transfer Amount (default 500): ");
            String amtStr = scanner.nextLine().trim();
            double amt = amtStr.isEmpty() ? 500.0 : Double.parseDouble(amtStr);

            System.out.println("\nProcessing transaction...");
            String result = lb.processTransaction(custId, from, to, amt);
            System.out.println("Server Response: " + result);

            System.out.println("Waiting for worker thread and replication to finalize...");
            Thread.sleep(2000);

            System.out.println("\n--- [Updated Balances Post-Transaction] ---");
            double fromBalance = lb.getBalance(from);
            double toBalance = lb.getBalance(to);
            System.out.printf("Source Account (%s) New Balance     : Rs. %.2f%n", from, fromBalance);
            System.out.printf("Destination Account (%s) New Balance: Rs. %.2f%n", to, toBalance);

        } catch (Exception e) {
            System.err.println("Transaction Error: " + e.getMessage());
        }
    }

    /* -------------------------------------------------------------------
     * 3. SHOW LOAD BALANCING
     * ------------------------------------------------------------------- */
    private static void showLoadBalancing(LoadBalancerService lb) {
        System.out.println("\n--- [Show Load Balancing] ---");
        System.out.println("Sending sequential balance requests across cluster nodes...\n");

        try {
            int reqNum = 1;
            for (String acc : knownAccounts) {
                double bal = lb.getBalance(acc);
                System.out.printf("Request #%d: Account %s => Rs. %.2f%n", reqNum++, acc, bal);
                Thread.sleep(150);
            }
        } catch (Exception e) {
            System.err.println("Load Balancing Query Error: " + e.getMessage());
        }
    }

    /* -------------------------------------------------------------------
     * 4. SHOW REPLICATION
     * ------------------------------------------------------------------- */
    private static void showReplication(LoadBalancerService lb) {
        System.out.println("\n--- [Show Replication] ---");
        System.out.println("Fetching current state from synchronized accounts:\n");

        try {
            for (String acc : knownAccounts) {
                double bal = lb.getBalance(acc);
                System.out.printf("  Account %s : Rs. %.2f%n", acc, bal);
            }
            System.out.println("\nTo inspect physical SQLite storage parity across nodes:");
            System.out.println("  docker exec -it finvault-server-1 sqlite3 finance_node_1.db \"SELECT * FROM accounts;\"");
            System.out.println("  docker exec -it finvault-server-2 sqlite3 finance_node_2.db \"SELECT * FROM accounts;\"");
            System.out.println("  docker exec -it finvault-server-3 sqlite3 finance_node_3.db \"SELECT * FROM accounts;\"");
            System.out.println("  docker exec -it finvault-server-4 sqlite3 finance_node_4.db \"SELECT * FROM accounts;\"");
        } catch (Exception e) {
            System.err.println("Replication Verification Error: " + e.getMessage());
        }
    }

    /* -------------------------------------------------------------------
     * 5. SHOW MULTITHREADING
     * ------------------------------------------------------------------- */
    private static void showMultithreading(LoadBalancerService lb) {
        System.out.println("\n--- [Show Multithreading] ---");
        System.out.println("Initiating 5 concurrent transaction requests to server thread pool...\n");

        for (int i = 1; i <= 5; i++) {
            final int txnId = i;
            new Thread(() -> {
                try {
                    String res = lb.processTransaction("CUST_THREAD_" + txnId, "ACC101", "ACC102", 100.0 * txnId);
                    System.out.println("[Worker Thread Response " + txnId + "]: " + res);
                } catch (Exception e) {
                    System.err.println("Concurrent Transaction " + txnId + " Failed: " + e.getMessage());
                }
            }).start();
        }
    }

    /* -------------------------------------------------------------------
     * 6. SHOW FAULT TOLERANCE
     * ------------------------------------------------------------------- */
    private static void showFaultTolerance() {
        System.out.println("\n--- [Show Fault Tolerance & Failover] ---");
        System.out.println("Steps to test Primary node crash and Bully election failover:");
        System.out.println("1. Stop current Primary Node 1:");
        System.out.println("     docker stop finvault-server-1");
        System.out.println("2. Monitor election logs in Replication Manager:");
        System.out.println("     docker compose logs -f replication-manager");
        System.out.println("3. Node 4 will win the election and assume the PRIMARY role.");
        System.out.println("4. Select Option 2 from this menu to execute a new transaction.");
        System.out.println("   The Load Balancer will automatically route writes to Node 4.");
    }

    /* -------------------------------------------------------------------
     * 7. RUN SYSTEM DIAGNOSTIC
     * ------------------------------------------------------------------- */
    private static void runSystemDiagnostic(LoadBalancerService lb) {
        System.out.println("\n=================================================");
        System.out.println("       RUNNING FULL SYSTEM DIAGNOSTIC             ");
        System.out.println("=================================================");

        System.out.println("\nStep 1: Checking initial balances...");
        showReplication(lb);

        System.out.println("\nStep 2: Processing transaction (ACC101 -> ACC102, Rs. 1000)...");
        try {
            String res = lb.processTransaction("DIAGNOSTIC_CLIENT", "ACC101", "ACC102", 1000.0);
            System.out.println("Result: " + res);
        } catch (Exception e) {
            System.err.println("Diagnostic Transaction Error: " + e.getMessage());
        }

        System.out.println("\nStep 3: Running load-balanced balance checks...");
        showLoadBalancing(lb);

        System.out.println("\nStep 4: Confirming state synchronization...");
        showReplication(lb);

        System.out.println("\n=================================================");
        System.out.println("        SYSTEM DIAGNOSTIC COMPLETED              ");
        System.out.println("=================================================");
    }
}