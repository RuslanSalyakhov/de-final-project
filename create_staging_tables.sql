-- Создать таблицу RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions
CREATE TABLE RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions (
    operation_id UUID,
    account_number_from INT,
    account_number_to INT,
    currency_code VARCHAR(3),
    country VARCHAR(128),
    "status" VARCHAR(32),
    transaction_type VARCHAR(32),
    amount NUMERIC(18,2),
    transaction_dt TIMESTAMP(3),
    PRIMARY KEY (operation_id, transaction_dt)
)
ORDER BY operation_id, transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt::date;

-- Создать таблицу RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies
CREATE TABLE RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies (
    date_update TIMESTAMP(3),
    currency_code VARCHAR(3),
    currency_code_with VARCHAR(3),
    currency_code_div NUMERIC(5,2),
    CONSTRAINT pk_date_update_currency_code_with PRIMARY KEY (date_update, currency_code, currency_code_with) ENABLED
)
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update) ALL NODES
PARTITION BY date_update::date;