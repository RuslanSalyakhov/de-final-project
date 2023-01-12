-- Создать представление RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions_dollars
CREATE OR REPLACE VIEW RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions_dollars AS
(SELECT t.operation_id, t.account_number_from, t.account_number_to, t.currency_code,
        t.country, t.status, t.transaction_type, t.amount, t.transaction_dt,
        cur.date_update, cur.currency_code_with, cur.currency_code_div,
        CASE 
            WHEN (cur.currency_code = 420) THEN t.amount 
            WHEN (cur.currency_code != 420) THEN (t.amount / cur.currency_code_div) 
        END AS amount_dollars       
FROM RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions t 
LEFT JOIN RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies cur 
ON (t.currency_code = cur.currency_code AND t.transaction_dt::date = cur.date_update::date)
WHERE ((cur.currency_code = 420 AND cur.currency_code_with = 410 AND t.account_number_from != -1) 
	OR (cur.currency_code_with = 420 AND t.account_number_from != -1)) AND t.status = 'done');

-- Пример агрегирующего запроса для добавления записи в витрину
SELECT date_update::date, 420 AS currency_from, SUM(amount_dollars) AS amount_total, 
		COUNT(1) AS cnt_transactions, COUNT(account_number_to)/COUNT(DISTINCT account_number_to) AS avg_transactions_per_account, 
		COUNT(DISTINCT account_number_to) AS cnt_accounts_make_transactions
FROM RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions_dollars 
WHERE date_update::date = '2022-10-01' GROUP BY date_update