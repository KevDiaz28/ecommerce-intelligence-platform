import argparse

from sklearn.model_selection import train_test_split

from data_loader import load_parquet_data
from metrics import evaluate_regression_model
from model_factory import get_regression_models
from preprocessing import build_tabular_preprocessor
from trainer import train_and_evaluate_models


def main(input_data: str, output_dir: str, sample_size: int = None):
    df = load_parquet_data(input_data)

    if sample_size is not None and sample_size < len(df):
        df = df.sample(n=sample_size, random_state=42)
        print(f"Using sample size: {sample_size}")

    print(f"Dataset shape: {df.shape}")

    target = "target_transaction_amount"

    numeric_features = [
        "quantity",
        "amount_per_item",
        "customer_age",
        "account_age_days",
        "hour",
        "day_of_week",
        "is_weekend",
        "is_new_account",
        "is_night_transaction",
        "address_mismatch",
    ]

    categorical_features = [
        "payment_method",
        "product_category",
        "customer_location",
        "device_used",
    ]

    features = numeric_features + categorical_features

    df = df[features + [target]].dropna()

    X = df[features]
    y = df[target]

    print(f"Modeling dataset shape: {df.shape}")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42
    )

    preprocessor = build_tabular_preprocessor(
        numeric_features=numeric_features,
        categorical_features=categorical_features
    )

    models = get_regression_models()

    train_and_evaluate_models(
        models=models,
        preprocessor=preprocessor,
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        output_dir=output_dir,
        evaluation_function=evaluate_regression_model
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_data", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--sample_size", type=int, default=None)

    args = parser.parse_args()

    main(
        input_data=args.input_data,
        output_dir=args.output_dir,
        sample_size=args.sample_size
    )
