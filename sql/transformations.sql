```sql
-- ============================================================
-- BigQuery Data Transformations
-- Project: ecomm-analytics-portfolio
-- Pipeline: RAW → STAGING → ANALYTICS → ML
-- ============================================================


-- ============================================================
-- 1. STAGING LAYER
-- Table: ecom_staging.transactions_clean
-- Purpose:
-- - Cast data types
-- - Parse timestamps
-- - Normalize categorical fields
-- - Filter invalid records
-- ============================================================

CREATE OR REPLACE TABLE `ecomm-analytics-portfolio.ecom_staging.transactions_clean` AS
SELECT
  transaction_id,
  customer_id,

  SAFE_CAST(transaction_amount AS NUMERIC) AS transaction_amount,

  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', transaction_date) AS transaction_timestamp,

  LOWER(TRIM(payment_method)) AS payment_method,
  LOWER(TRIM(product_category)) AS product_category,

  SAFE_CAST(quantity AS INT64) AS quantity,
  SAFE_CAST(customer_age AS INT64) AS customer_age,

  TRIM(customer_location) AS customer_location,
  LOWER(TRIM(device_used)) AS device_used,

  ip_address,
  shipping_address,
  billing_address,

  SAFE_CAST(is_fraudulent AS INT64) AS is_fraudulent,
  SAFE_CAST(account_age_days AS INT64) AS account_age_days,
  SAFE_CAST(transaction_hour AS INT64) AS transaction_hour

FROM `ecomm-analytics-portfolio.ecom_raw.transactions_raw`

WHERE
  SAFE_CAST(transaction_amount AS NUMERIC) > 0
  AND SAFE_CAST(quantity AS INT64) > 0
  AND SAFE_CAST(customer_age AS INT64) > 0
  AND SAFE_CAST(account_age_days AS INT64) >= 0
  AND SAFE_CAST(transaction_hour AS INT64) BETWEEN 0 AND 23;


-- ============================================================
-- 2. ANALYTICS LAYER
-- Table: ecom_analytics.fact_transactions
-- Purpose:
-- - Create business-ready transaction table
-- - Add time-based features
-- - Add behavioral and fraud-risk signals
-- ============================================================

CREATE OR REPLACE TABLE `ecomm-analytics-portfolio.ecom_analytics.fact_transactions` AS
SELECT
  transaction_id,
  customer_id,

  transaction_amount,
  quantity,

  DATE(transaction_timestamp) AS transaction_date,
  EXTRACT(HOUR FROM transaction_timestamp) AS hour,
  EXTRACT(DAYOFWEEK FROM transaction_timestamp) AS day_of_week,

  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM transaction_timestamp) IN (1, 7) THEN 1
    ELSE 0
  END AS is_weekend,

  SAFE_DIVIDE(transaction_amount, quantity) AS amount_per_item,

  CASE 
    WHEN account_age_days < 30 THEN 1
    ELSE 0
  END AS is_new_account,

  CASE 
    WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 0 AND 5 THEN 1
    ELSE 0
  END AS is_night_transaction,

  CASE 
    WHEN LOWER(TRIM(shipping_address)) != LOWER(TRIM(billing_address)) THEN 1
    ELSE 0
  END AS address_mismatch,

  payment_method,
  product_category,
  customer_age,
  customer_location,
  device_used,

  is_fraudulent,
  account_age_days

FROM `ecomm-analytics-portfolio.ecom_staging.transactions_clean`;


-- ============================================================
-- 3. ML FEATURE TABLE
-- Table: ecom_ml.features_fraud_train
-- Purpose:
-- - Prepare final dataset for machine learning
-- - Keep only modeling features and target variable
-- ============================================================

CREATE OR REPLACE TABLE `ecomm-analytics-portfolio.ecom_ml.features_fraud_train` AS
SELECT
  transaction_id,

  transaction_amount,
  quantity,
  amount_per_item,

  customer_age,
  account_age_days,

  hour,
  day_of_week,
  is_weekend,
  is_new_account,
  is_night_transaction,
  address_mismatch,

  payment_method,
  product_category,
  customer_location,
  device_used,

  is_fraudulent

FROM `ecomm-analytics-portfolio.ecom_analytics.fact_transactions`;


-- ============================================================
-- 4. VALIDATION QUERIES
-- Optional checks after running transformations
-- ============================================================

-- Row count by layer
SELECT
  'raw' AS layer,
  COUNT(*) AS total_rows
FROM `ecomm-analytics-portfolio.ecom_raw.transactions_raw`

UNION ALL

SELECT
  'staging' AS layer,
  COUNT(*) AS total_rows
FROM `ecomm-analytics-portfolio.ecom_staging.transactions_clean`

UNION ALL

SELECT
  'analytics' AS layer,
  COUNT(*) AS total_rows
FROM `ecomm-analytics-portfolio.ecom_analytics.fact_transactions`

UNION ALL

SELECT
  'ml' AS layer,
  COUNT(*) AS total_rows
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`;


-- Target distribution
SELECT
  is_fraudulent,
  COUNT(*) AS transactions,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY is_fraudulent
ORDER BY is_fraudulent;


-- Basic null validation
SELECT
  COUNTIF(transaction_amount IS NULL) AS null_transaction_amount,
  COUNTIF(quantity IS NULL) AS null_quantity,
  COUNTIF(customer_age IS NULL) AS null_customer_age,
  COUNTIF(is_fraudulent IS NULL) AS null_target
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`;
