# Data Ingestion & Cleaning Pipeline
## Problem
The original dataset (CSV) could not be directly loaded into BigQuery due to:
- inconsistent delimiters
- line breaks inside fields (addresses)
- malformed rows
- parsing errors
BigQuery returned multiple ingestion errors.
## Solution
Instead of forcing ingestion, the data was preprocessed using Python (Pandas) in Google Colab.
## Pipeline
CSV → Colab (Pandas) → Cleaning → Parquet → Cloud Storage → BigQuery

## Key Steps
### 1. Load raw data
```python
import pandas as pd
df = pd.read_csv(
    "data.csv",
    engine="python",
    on_bad_lines="skip"
)

### 2. Basic cleaning
df = df.drop_duplicates()
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
df = df.dropna()

### 3. Export parquet
df.to_parquet("clean.parquet", index=False)

### 4. Upload to Cloud Storage
from google.cloud import storage
client = storage.Client()
bucket = client.bucket("ecomm-analytics-bucket")
blob = bucket.blob("clean.parquet")
blob.upload_from_filename("clean.parquet")

Why Parquet?
columnar format
schema preserved
avoids CSV parsing issues
optimized for BigQuery
Result

Clean dataset successfully loaded into BigQuery:
ecom_raw.transactions_raw
