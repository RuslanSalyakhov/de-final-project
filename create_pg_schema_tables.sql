-- Staging schema and tables on PosgreSQL used to save Spark processed data coming from Kafka
-- Create schema RUSLAN_SALIAHOFF_YANDEX_RU__STAGING
CREATE SCHEMA RUSLAN_SALIAHOFF_YANDEX_RU__STAGING;
-- Create table RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions. Don't use any constrains as well as primary key for staging.
CREATE TABLE RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.transactions (
    operation_id VARCHAR(36), -- Since UUID type is not supported by Spark we use the same 36 characters length char type
    account_number_from INT,
    account_number_to INT,
    currency_code VARCHAR(3),
    country VARCHAR(128),
    "status" VARCHAR(32),
    transaction_type VARCHAR(32),
    amount NUMERIC(18,2),
    transaction_dt TIMESTAMP(3)
)
-- Create table RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies. Don't use any constrains as well as primary key for staging.
CREATE TABLE RUSLAN_SALIAHOFF_YANDEX_RU__STAGING.сurrencies (
    date_update TIMESTAMP(3),
    currency_code VARCHAR(3),
    currency_code_with VARCHAR(3),
    currency_code_div NUMERIC(5,2))