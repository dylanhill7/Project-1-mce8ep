import duckdb
import logging
import pandas as pd
import os
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DB_NAME = os.path.join(BASE_DIR, "mlb_analysis.duckdb")
MODEL_DIR = os.path.join(BASE_DIR, "models")

# xwOBA removed
FEATURE_COLUMNS = [
    "BB%",
    "K%",
    "ISO",
    "BABIP",
    "wOBA",
    "wRC+",
    "BsR",
    "Off",
    "Def",
    "WAR"
]

BREAKOUT_TABLE = "breakout_master"
REGRESSION_TABLE = "regression_master"

BREAKOUT_TARGET = "breakout_label"
REGRESSION_TARGET = "regression_label"

RANDOM_STATE = 42


# -------------------------------
# Helper functions
# -------------------------------
def table_exists(con, table_name):
    query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
    """
    result = con.execute(query, [table_name]).fetchone()[0]
    return result > 0


def load_table(con, table_name):
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    logger.info("Loaded table '%s' with %d rows.", table_name, len(df))
    return df


def preprocess_dataframe(df, feature_cols, target_col, dataset_name):
    """
    Drop xwOBA, convert to numeric, and impute missing values with medians.
    Returns:
        working_df: cleaned dataframe
        medians: feature medians used for imputation
    """
    if "xwOBA" in df.columns:
        df = df.drop(columns=["xwOBA"])
        logger.info("Dropped xwOBA column from %s.", dataset_name)

    working_df = df[feature_cols + [target_col]].copy()

    for col in feature_cols:
        working_df[col] = pd.to_numeric(working_df[col], errors="coerce")

    working_df[target_col] = pd.to_numeric(working_df[target_col], errors="coerce")

    print(f"\nMissing values BEFORE imputation for {dataset_name}:")
    print(working_df.isnull().sum())

    logger.info(
        "Missing values before imputation for %s:\n%s",
        dataset_name,
        working_df.isnull().sum().to_string()
    )

    medians = working_df[feature_cols].median()

    working_df[feature_cols] = working_df[feature_cols].fillna(medians)

    working_df = working_df.dropna(subset=[target_col])

    print(f"\nMissing values AFTER imputation for {dataset_name}:")
    print(working_df.isnull().sum())

    logger.info(
        "Missing values after imputation for %s:\n%s",
        dataset_name,
        working_df.isnull().sum().to_string()
    )

    return working_df, medians


def save_model_artifact(
    model,
    feature_cols,
    feature_medians,
    best_params,
    best_cv_score,
    test_accuracy,
    output_path
):
    artifact = {
        "model": model,
        "feature_columns": feature_cols,
        "feature_medians": feature_medians.to_dict(),
        "best_params": best_params,
        "best_cv_score": best_cv_score,
        "test_accuracy": test_accuracy
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    joblib.dump(artifact, output_path)

    logger.info("Saved model artifact to %s", output_path)
    print(f"\nSaved model artifact to {output_path}")


def run_random_forest_pipeline(
    df,
    feature_cols,
    target_col,
    dataset_name,
    feature_medians,
    model_output_path
):
    print(f"\n{'=' * 60}")
    print(f"RUNNING MODEL FOR: {dataset_name}")
    print(f"{'=' * 60}")

    X = df[feature_cols]
    y = df[target_col]

    print("\nTarget distribution:")
    print(y.value_counts())

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=RANDOM_STATE,
        stratify=y
    )

    print(f"\nTraining rows: {len(X_train)}")
    print(f"Testing rows: {len(X_test)}")

    cv_strategy = StratifiedKFold(
        n_splits=5,
        shuffle=True,
        random_state=RANDOM_STATE
    )

    param_grid = {
        "n_estimators": [300, 500, 800],
        "max_depth": [5, 10, 20],
        "max_features": ["sqrt", "log2", None]
    }

    rf = RandomForestClassifier(random_state=RANDOM_STATE)

    grid_search = GridSearchCV(
        estimator=rf,
        param_grid=param_grid,
        cv=cv_strategy,
        scoring="accuracy",
        n_jobs=-1,
        verbose=1
    )

    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_

    print("\nBest hyperparameters:")
    print(grid_search.best_params_)

    print(f"\nBest CV accuracy: {grid_search.best_score_:.4f}")

    y_pred = best_model.predict(X_test)
    test_accuracy = accuracy_score(y_test, y_pred)

    print(f"\nTest accuracy: {test_accuracy:.4f}")
    print("\nConfusion matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("\nClassification report:")
    print(classification_report(y_test, y_pred))

    importances = pd.DataFrame({
        "feature": feature_cols,
        "importance": best_model.feature_importances_
    }).sort_values(by="importance", ascending=False)

    print("\nFeature importances:")
    print(importances)

    save_model_artifact(
        model=best_model,
        feature_cols=feature_cols,
        feature_medians=feature_medians,
        best_params=grid_search.best_params_,
        best_cv_score=grid_search.best_score_,
        test_accuracy=test_accuracy,
        output_path=model_output_path
    )

    return {
        "best_model": best_model,
        "best_params": grid_search.best_params_,
        "best_cv_score": grid_search.best_score_,
        "test_accuracy": test_accuracy,
        "feature_importances": importances
    }


# -------------------------------
# Main pipeline
# -------------------------------
def main():
    con = None

    try:
        con = duckdb.connect(database=DB_NAME, read_only=True)
        print(f"Connected to DuckDB database: {DB_NAME}")

        required_tables = [BREAKOUT_TABLE, REGRESSION_TABLE]
        for table_name in required_tables:
            if not table_exists(con, table_name):
                raise ValueError(
                    f"Required table '{table_name}' does not exist. "
                    "Run your transform scripts first."
                )

        breakout_df = load_table(con, BREAKOUT_TABLE)
        regression_df = load_table(con, REGRESSION_TABLE)

        breakout_clean, breakout_medians = preprocess_dataframe(
            breakout_df,
            FEATURE_COLUMNS,
            BREAKOUT_TARGET,
            "Breakout Dataset"
        )

        regression_clean, regression_medians = preprocess_dataframe(
            regression_df,
            FEATURE_COLUMNS,
            REGRESSION_TARGET,
            "Regression Dataset"
        )

        breakout_results = run_random_forest_pipeline(
            breakout_clean,
            FEATURE_COLUMNS,
            BREAKOUT_TARGET,
            "Breakout Dataset",
            breakout_medians,
            os.path.join(MODEL_DIR, "breakout_model.joblib")
        )

        regression_results = run_random_forest_pipeline(
            regression_clean,
            FEATURE_COLUMNS,
            REGRESSION_TARGET,
            "Regression Dataset",
            regression_medians,
            os.path.join(MODEL_DIR, "regression_model.joblib")
        )

        print(f"\n{'=' * 60}")
        print("FINAL SUMMARY")
        print(f"{'=' * 60}")
        print(f"Breakout best params: {breakout_results['best_params']}")
        print(f"Breakout best CV accuracy: {breakout_results['best_cv_score']:.4f}")
        print(f"Breakout test accuracy: {breakout_results['test_accuracy']:.4f}")

        print()

        print(f"Regression best params: {regression_results['best_params']}")
        print(f"Regression best CV accuracy: {regression_results['best_cv_score']:.4f}")
        print(f"Regression test accuracy: {regression_results['test_accuracy']:.4f}")

    except Exception as e:
        logger.error("An error occurred in analysis.py: %s", e)
        print(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
            print("\nDuckDB connection closed.")


if __name__ == "__main__":
    main()