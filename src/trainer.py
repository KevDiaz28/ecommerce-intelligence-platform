import json
import os

import joblib
import pandas as pd
from sklearn.pipeline import Pipeline


def train_and_evaluate_models(
    models,
    preprocessor,
    X_train,
    X_test,
    y_train,
    y_test,
    output_dir,
    evaluation_function
):
    os.makedirs(output_dir, exist_ok=True)

    metrics = []

    for model_name, estimator in models.items():
        print(f"Training model: {model_name}")

        pipeline = Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("model", estimator)
        ])

        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        model_metrics = evaluation_function(
            model_name=model_name,
            y_true=y_test,
            y_pred=preds
        )

        metrics.append(model_metrics)

        model_path = os.path.join(output_dir, f"{model_name}.joblib")
        joblib.dump(pipeline, model_path)

        print(f"Saved model: {model_path}")
        print(model_metrics)

    metrics_df = pd.DataFrame(metrics).sort_values("rmse")

    leaderboard_path = os.path.join(output_dir, "leaderboard.csv")
    metrics_path = os.path.join(output_dir, "metrics.json")

    metrics_df.to_csv(leaderboard_path, index=False)

    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print("Training completed.")
    print("Leaderboard:")
    print(metrics_df)

    return metrics_df
