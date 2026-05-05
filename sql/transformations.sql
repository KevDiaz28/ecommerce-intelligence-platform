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


-- ============================================================
-- 4.1 Raw uniqueness validation
-- Expected result from current pipeline:
-- total_rows = 1,496,056
-- distinct_transaction_ids = 1,496,056
-- ============================================================

SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT transaction_id) AS distinct_transaction_ids
FROM `ecomm-analytics-portfolio.ecom_raw.transactions_raw`;


-- ============================================================
-- 4.2 Row count by layer
-- Ensures data volume remains consistent across layers
-- ============================================================

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


-- ============================================================
-- 4.3 Target distribution
-- Expected result from current pipeline:
-- is_fraudulent = 0 → ~94.98%
-- is_fraudulent = 1 → ~5.02%
-- This confirms a realistic class imbalance for fraud detection.
-- ============================================================

SELECT
  is_fraudulent,
  COUNT(*) AS transactions,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY is_fraudulent
ORDER BY is_fraudulent;


-- ============================================================
-- 4.4 Basic null validation
-- Checks key modeling fields and target variable
-- ============================================================

SELECT
  COUNTIF(transaction_amount IS NULL) AS null_transaction_amount,
  COUNTIF(quantity IS NULL) AS null_quantity,
  COUNTIF(amount_per_item IS NULL) AS null_amount_per_item,
  COUNTIF(customer_age IS NULL) AS null_customer_age,
  COUNTIF(account_age_days IS NULL) AS null_account_age_days,
  COUNTIF(hour IS NULL) AS null_hour,
  COUNTIF(day_of_week IS NULL) AS null_day_of_week,
  COUNTIF(is_fraudulent IS NULL) AS null_target
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`;


-- ============================================================
-- 4.5 Feature range validation
-- Ensures numeric and engineered variables are within expected ranges
-- ============================================================

SELECT
  MIN(transaction_amount) AS min_transaction_amount,
  MAX(transaction_amount) AS max_transaction_amount,
  MIN(quantity) AS min_quantity,
  MAX(quantity) AS max_quantity,
  MIN(customer_age) AS min_customer_age,
  MAX(customer_age) AS max_customer_age,
  MIN(account_age_days) AS min_account_age_days,
  MAX(account_age_days) AS max_account_age_days,
  MIN(hour) AS min_hour,
  MAX(hour) AS max_hour,
  MIN(day_of_week) AS min_day_of_week,
  MAX(day_of_week) AS max_day_of_week
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`;


-- ============================================================
-- 4.6 Fraud rate by engineered risk flags
-- Helps validate whether engineered signals are useful for modeling
-- ============================================================

SELECT
  is_new_account,
  is_night_transaction,
  address_mismatch,
  COUNT(*) AS transactions,
  ROUND(AVG(is_fraudulent) * 100, 2) AS fraud_rate_percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY
  is_new_account,
  is_night_transaction,
  address_mismatch
ORDER BY fraud_rate_percentage DESC;


-- ============================================================
-- 4.7 Fraud rate by payment method
-- Business validation for categorical risk patterns
-- ============================================================

SELECT
  payment_method,
  COUNT(*) AS transactions,
  ROUND(AVG(is_fraudulent) * 100, 2) AS fraud_rate_percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY payment_method
ORDER BY fraud_rate_percentage DESC;


-- ============================================================
-- 4.8 Fraud rate by device
-- Business validation for channel/device behavior
-- ============================================================

SELECT
  device_used,
  COUNT(*) AS transactions,
  ROUND(AVG(is_fraudulent) * 100, 2) AS fraud_rate_percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY device_used
ORDER BY fraud_rate_percentage DESC;


-- ============================================================
-- 4.9 Fraud rate by product category
-- Business validation for product-level risk
-- ============================================================

SELECT
  product_category,
  COUNT(*) AS transactions,
  ROUND(AVG(is_fraudulent) * 100, 2) AS fraud_rate_percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY product_category
ORDER BY fraud_rate_percentage DESC;


-- ============================================================
-- 4.10 Top locations by fraud rate
-- Uses minimum transaction threshold to avoid noisy locations
-- ============================================================

SELECT
  customer_location,
  COUNT(*) AS transactions,
  ROUND(AVG(is_fraudulent) * 100, 2) AS fraud_rate_percentage
FROM `ecomm-analytics-portfolio.ecom_ml.features_fraud_train`
GROUP BY customer_location
HAVING COUNT(*) >= 100
ORDER BY fraud_rate_percentage DESC
LIMIT 20;
