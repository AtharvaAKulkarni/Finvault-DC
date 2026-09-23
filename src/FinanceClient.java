import java.rmi.Naming;


public class FinanceClient {

    private static final String RMI_HOST =
            System.getenv().getOrDefault(
                    "FINANCE_SERVER_HOST",
                    "localhost"
            );

    private static final String RMI_URL =
            "rmi://"
                    + RMI_HOST
                    + ":1234/FinanceServer";


    public static void main(String[] args) {

        try {

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "        FINVAULT FINANCE CLIENT"
            );

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "Connecting to: "
                            + RMI_HOST
            );


            FinanceService finance =
                    (FinanceService)
                            Naming.lookup(
                                    RMI_URL
                            );


            System.out.println(
                    "RMI Lookup Successful."
            );

            System.out.println(
                    "Connected to Finance Server."
            );


            /*
             * =================================================
             * CUSTOMER 1
             * =================================================
             */

            Thread customer1 =
                    new Thread(() -> {

                        try {

                            System.out.println(
                                    "[Customer 1] "
                                            + "ACC101 -> ACC102"
                            );

                            String response =
                                    finance.processTransaction(
                                            "CUST001",
                                            "ACC101",
                                            "ACC102",
                                            5000
                                    );

                            System.out.println(
                                    "[Customer 1] Response: "
                                            + response
                            );

                        } catch (Exception e) {

                            System.out.println(
                                    "[Customer 1] Error: "
                                            + e.getMessage()
                            );
                        }
                    });


            /*
             * =================================================
             * CUSTOMER 2
             * =================================================
             */

            Thread customer2 =
                    new Thread(() -> {

                        try {

                            System.out.println(
                                    "[Customer 2] "
                                            + "ACC103 -> ACC104"
                            );

                            String response =
                                    finance.processTransaction(
                                            "CUST002",
                                            "ACC103",
                                            "ACC104",
                                            8000
                                    );

                            System.out.println(
                                    "[Customer 2] Response: "
                                            + response
                            );

                        } catch (Exception e) {

                            System.out.println(
                                    "[Customer 2] Error: "
                                            + e.getMessage()
                            );
                        }
                    });


            /*
             * =================================================
             * CUSTOMER 3
             * =================================================
             */

            Thread customer3 =
                    new Thread(() -> {

                        try {

                            System.out.println(
                                    "[Customer 3] "
                                            + "ACC102 -> ACC103"
                            );

                            String response =
                                    finance.processTransaction(
                                            "CUST003",
                                            "ACC102",
                                            "ACC103",
                                            3000
                                    );

                            System.out.println(
                                    "[Customer 3] Response: "
                                            + response
                            );

                        } catch (Exception e) {

                            System.out.println(
                                    "[Customer 3] Error: "
                                            + e.getMessage()
                            );
                        }
                    });


            /*
             * =================================================
             * START CUSTOMERS
             * =================================================
             */

            customer1.start();
            customer2.start();
            customer3.start();


            customer1.join();
            customer2.join();
            customer3.join();


            /*
             * Your FinanceServer currently processes
             * the transaction in its executor thread.
             *
             * Give the workers time to finish before
             * reading balances.
             */

            Thread.sleep(3000);


            /*
             * =================================================
             * FINAL BALANCES
             * =================================================
             */

            System.out.println();

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "          FINAL ACCOUNT BALANCES"
            );

            System.out.println(
                    "============================================"
            );

            System.out.println(
                    "ACC101 : Rs. "
                            + finance.getBalance(
                            "ACC101"
                    )
            );

            System.out.println(
                    "ACC102 : Rs. "
                            + finance.getBalance(
                            "ACC102"
                    )
            );

            System.out.println(
                    "ACC103 : Rs. "
                            + finance.getBalance(
                            "ACC103"
                    )
            );

            System.out.println(
                    "ACC104 : Rs. "
                            + finance.getBalance(
                            "ACC104"
                    )
            );


        } catch (Exception e) {

            System.out.println(
                    "Client Error: "
                            + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}