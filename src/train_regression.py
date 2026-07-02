import argparse
import json
import os

import joblib
import pandas as pd

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeRegressor

from data_loader import load_parquet_data
from metrics import evaluate_regression_model
from preprocessing import build_tabular_preprocessor


def main(input_data: str, output_dir: str):
    df = load_parquet_data(input_data)

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

    models = {
        "linear_regression": LinearRegression(),
        "decision_tree_regressor": DecisionTreeRegressor(
            max_depth=10,
            min_samples_leaf=50,
            random_state=42
        ),
        "random_forest_regressor": RandomForestRegressor(
            n_estimators=150,
            max_depth=12,
            min_samples_leaf=50,
            random_state=42,
            n_jobs=-1
        )
    }

    os.makedirs(output_dir, exist_ok=True)

    metrics = []

    for model_name, estimator in models.items():
        pipeline = Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("model", estimator)
        ])

        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        model_metrics = evaluate_regression_model(
            model_name=model_name,
            y_true=y_test,
            y_pred=preds
        )

        metrics.append(model_metrics)

        model_path = os.path.join(output_dir, f"{model_name}.joblib")
        joblib.dump(pipeline, model_path)

    metrics_df = pd.DataFrame(metrics).sort_values("rmse")
    metrics_df.to_csv(os.path.join(output_dir, "leaderboard.csv"), index=False)

    with open(os.path.join(output_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    print(metrics_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_data", required=True)
    parser.add_argument("--output_dir", required=True)

    args = parser.parse_args()

    main(
        input_data=args.input_data,
        output_dir=args.output_dir
    )
