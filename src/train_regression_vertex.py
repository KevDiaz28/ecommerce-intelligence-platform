import argparse
import json
import os

import joblib
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.tree import DecisionTreeRegressor


def evaluate_model(model_name, y_true, y_pred):
    return {
        "model": model_name,
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
    }


def build_preprocessor():
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

    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore"))
    ])

    return ColumnTransformer([
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features)
    ])


def main(input_data: str, output_dir: str):
    df = pd.read_parquet(input_data)

    target = "target_transaction_amount"

    features = [
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
        "payment_method",
        "product_category",
        "customer_location",
        "device_used",
    ]

    df = df[features + [target]].dropna()

    X = df[features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42
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
        pipeline = Pipeline([
            ("preprocessor", build_preprocessor()),
            ("model", estimator)
        ])

        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        model_metrics = evaluate_model(model_name, y_test, preds)
        metrics.append(model_metrics)

        model_path = os.path.join(output_dir, f"{model_name}.joblib")
        joblib.dump(pipeline, model_path)

    metrics_df = pd.DataFrame(metrics).sort_values("rmse")
    best_model_name = metrics_df.iloc[0]["model"]

    with open(os.path.join(output_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    metrics_df.to_csv(os.path.join(output_dir, "leaderboard.csv"), index=False)

    print("Model metrics:")
    print(metrics_df)

    print(f"Best model: {best_model_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_data", required=True)
    parser.add_argument("--output_dir", required=True)

    args = parser.parse_args()

    main(
        input_data=args.input_data,
        output_dir=args.output_dir
    )
