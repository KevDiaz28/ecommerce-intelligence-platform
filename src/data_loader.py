import pandas as pd


def load_parquet_data(input_path: str) -> pd.DataFrame:
    """
    Load parquet data from a local path or GCS prefix.
    Example:
    gs://ecomm-analytics-bucket/regression/
    """
    df = pd.read_parquet(input_path)
    return df
