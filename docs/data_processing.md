# Data Processing (BigQuery)

## Overview

This project transforms raw e-commerce transaction data into structured analytical layers using BigQuery.

The data pipeline follows a layered architecture:

```text
RAW → STAGING → ANALYTICS → ML
```

---

## 1. Raw Layer

Table:

`ecom_raw.transactions_raw`

### Purpose

- Store ingested data from Cloud Storage (Parquet format)
- Preserve cleaned structure from Python/Colab preprocessing
- Serve as the base source for downstream transformations

At this stage, no business logic is applied.

---

## 2. Staging Layer

Table:

`ecom_staging.transactions_clean`

### Purpose

- Cast data types correctly
- Parse timestamps
- Normalize categorical variables
- Filter invalid records
- Prepare a reliable dataset for analytics

### Key transformations

```sql
SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', transaction_date) AS transaction_timestamp,
LOWER(TRIM(payment_method)) AS payment_method,
LOWER(TRIM(product_category)) AS product_category,
LOWER(TRIM(device_used)) AS device_used,
SAFE_CAST(transaction_amount AS NUMERIC) AS transaction_amount,
SAFE_CAST(quantity AS INT64) AS quantity,
SAFE_CAST(customer_age AS INT64) AS customer_age
```

### Data quality rules

Only valid records are kept:

```text
transaction_amount > 0
quantity > 0
customer_age > 0
account_age_days >= 0
transaction_hour BETWEEN 0 AND 23
```

---

## 3. Analytics Layer

Table:

`ecom_analytics.fact_transactions`

### Purpose

- Create business-ready dataset
- Generate behavioral features
- Build fraud-related signals
- Enable analysis and model training

### Time-based features

```sql
DATE(transaction_timestamp) AS transaction_date,
EXTRACT(HOUR FROM transaction_timestamp) AS hour,
EXTRACT(DAYOFWEEK FROM transaction_timestamp) AS day_of_week
```

```sql
CASE
  WHEN EXTRACT(DAYOFWEEK FROM transaction_timestamp) IN (1, 7) THEN 1
  ELSE 0
END AS is_weekend
```

### Behavioral features

```sql
SAFE_DIVIDE(transaction_amount, quantity) AS amount_per_item
```

```sql
CASE
  WHEN account_age_days < 30 THEN 1
  ELSE 0
END AS is_new_account
```

```sql
CASE
  WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 0 AND 5 THEN 1
  ELSE 0
END AS is_night_transaction
```

### Risk signal

```sql
CASE
  WHEN LOWER(TRIM(shipping_address)) != LOWER(TRIM(billing_address))
  THEN 1 ELSE 0
END AS address_mismatch
```

---

## 4. ML Layer

Table:

`ecom_ml.features_fraud_train`

### Purpose

- Prepare final dataset for machine learning
- Select modeling features
- Exclude unnecessary raw fields
- Define target variable

### Target variable

```text
is_fraudulent
```

---

## Result

The dataset is transformed into a structured format ready for:

- machine learning models
- fraud detection
- behavioral analytics
- interactive simulation
