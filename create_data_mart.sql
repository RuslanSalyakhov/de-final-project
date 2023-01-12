-- Создать таблицу RUSLAN_SALIAHOFF_YANDEX_RU__DWH.global_metrics
CREATE TABLE RUSLAN_SALIAHOFF_YANDEX_RU__DWH.global_metrics (
    date_update TIMESTAMP(3),
    currency_from VARCHAR(3),
    amount_total NUMERIC(18,2),
    cnt_transactions NUMERIC(18,2),
    avg_transactions_per_account NUMERIC(18,2),
    cnt_accounts_make_transactions INT,
    PRIMARY KEY (date_update)
)
PARTITION BY date_update::date;