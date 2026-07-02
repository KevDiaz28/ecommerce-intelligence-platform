from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor


def get_regression_models():
    return {
        "linear_regression": LinearRegression(),

        "decision_tree_regressor": DecisionTreeRegressor(
            max_depth=10,
            min_samples_leaf=100,
            random_state=42
        ),

        "random_forest_regressor": RandomForestRegressor(
            n_estimators=30,
            max_depth=10,
            min_samples_leaf=100,
            random_state=42,
            n_jobs=-1
        ),

        "xgboost_regressor": XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1
        )
    }
