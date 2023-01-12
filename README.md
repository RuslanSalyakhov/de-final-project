# de-final-project
Final project of Data engineering specialization at Yandex Practicum.

Step 1: Investigate S3 data.

Step 2: Create transactions, currencies tables in Staging schema, global_metrics table in DWH schema of Vertica.

Step 3. Create pipeline loading data from S3 to staging tables transactions and currencies. 

Step 4. Create pipeline updating data mart with information for each day with data including:

        date_update — date;
        currency_from — 3-digit currency code;
        amount_total — total amount of transactions in $;
        cnt_transactions — number of transactions;
        avg_transactions_per_account — average number of transactions per account;
        cnt_accounts_make_transactions — number of unique accounts executing transactions. 
