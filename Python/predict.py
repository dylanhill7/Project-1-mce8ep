import logging
import os
import pandas as pd
import joblib

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="predict.log"
)
logger = logging.getLogger(__name__)

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

BREAKOUT_MODEL_PATH = os.path.join(BASE_DIR, "models", "breakout_model.joblib")
REGRESSION_MODEL_PATH = os.path.join(BASE_DIR, "models", "regression_model.joblib")

BREAKOUT_2025_PATH = os.path.join(BASE_DIR, "Data", "20-25(2025).csv")
REGRESSION_2025_PATH = os.path.join(BASE_DIR, "Data", "30-35(2025).csv")

PREDICTIONS_DIR = os.path.join(BASE_DIR, "predictions")

# -------------------------------
# Helper functions
# -------------------------------
def load_model_artifact(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model artifact not found: {path}")

    artifact = joblib.load(path)
    logger.info("Loaded model artifact from %s", path)
    return artifact


def preprocess_prediction_data(df, feature_cols, feature_medians, dataset_name):
    """
    Match the training preprocessing:
    - drop xwOBA if present
    - coerce features to numeric
    - impute with saved medians
    """
    if "xwOBA" in df.columns:
        df = df.drop(columns=["xwOBA"])
        logger.info("Dropped xwOBA column from %s.", dataset_name)

    working_df = df.copy()

    for col in feature_cols:
        if col not in working_df.columns:
            working_df[col] = pd.NA

    for col in feature_cols:
        working_df[col] = pd.to_numeric(working_df[col], errors="coerce")

    median_series = pd.Series(feature_medians)
    working_df[feature_cols] = working_df[feature_cols].fillna(median_series)

    return working_df


def predict_candidates(input_csv, model_artifact, dataset_name, output_csv):
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Prediction input file not found: {input_csv}")

    df = pd.read_csv(input_csv)
    logger.info("Loaded %s with %d rows.", input_csv, len(df))

    model = model_artifact["model"]
    feature_cols = model_artifact["feature_columns"]
    feature_medians = model_artifact["feature_medians"]

    processed_df = preprocess_prediction_data(
        df,
        feature_cols,
        feature_medians,
        dataset_name
    )

    X_pred = processed_df[feature_cols]

    predicted_class = model.predict(X_pred)
    predicted_prob = model.predict_proba(X_pred)[:, 1]

    output_df = processed_df.copy()
    output_df["predicted_label"] = predicted_class
    output_df["predicted_probability"] = predicted_prob

    sort_col = "predicted_probability"
    output_df = output_df.sort_values(by=sort_col, ascending=False)

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    output_df.to_csv(output_csv, index=False)

    print(f"\nTop predictions for {dataset_name}:")
    preview_cols = [col for col in ["Name", "Season", "predicted_label", "predicted_probability"] if col in output_df.columns]
    print(output_df[preview_cols].head(15))

    logger.info("Saved predictions for %s to %s", dataset_name, output_csv)


# -------------------------------
# Main prediction pipeline
# -------------------------------
def main():
    try:
        breakout_artifact = load_model_artifact(BREAKOUT_MODEL_PATH)
        regression_artifact = load_model_artifact(REGRESSION_MODEL_PATH)

        predict_candidates(
            input_csv=BREAKOUT_2025_PATH,
            model_artifact=breakout_artifact,
            dataset_name="Breakout Candidates (Age 20-25, 2025)",
            output_csv=os.path.join(PREDICTIONS_DIR, "breakout_predictions_2025.csv")
        )

        predict_candidates(
            input_csv=REGRESSION_2025_PATH,
            model_artifact=regression_artifact,
            dataset_name="Regression Candidates (Age 30-35, 2025)",
            output_csv=os.path.join(PREDICTIONS_DIR, "regression_predictions_2025.csv")
        )

        print("\nPrediction pipeline completed successfully.")

    except Exception as e:
        logger.error("An error occurred in predict.py: %s", e)
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()