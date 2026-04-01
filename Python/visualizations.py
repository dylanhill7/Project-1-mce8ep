import logging
import os
from xml.parsers.expat import model
import joblib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import shap

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="visualization.log"
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

OUTPUT_DIR = os.path.join(BASE_DIR, "predictions")

TOP_N = 5
TOP_FEATURES_PER_PLAYER = 5


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


def get_player_name_column(df):
    """
    Prefer Name if present, otherwise try common alternatives.
    """
    for col in ["Name", "player_name", "Player", "player"]:
        if col in df.columns:
            return col
    raise ValueError("Could not find a player name column in prediction data.")


def compute_predictions_and_shap(input_csv, model_artifact, dataset_name):
    """
    Load 2025 candidate data, preprocess it, generate predictions,
    and compute SHAP values.
    """
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

    predicted_prob = model.predict_proba(X_pred)[:, 1]
    predicted_class = model.predict(X_pred)

    output_df = processed_df.copy()
    output_df["predicted_probability"] = predicted_prob
    output_df["predicted_label"] = predicted_class

    explainer = shap.TreeExplainer(model)

    shap_explanation = explainer(X_pred)

    # shap_explanation.values shape:
    # (n_samples, n_features) OR (n_samples, n_features, n_classes)
    if len(shap_explanation.values.shape) == 3:
        shap_values_class1 = shap_explanation.values[:, :, 1]
    else:
        shap_values_class1 = shap_explanation.values

    return output_df, X_pred, shap_values_class1


def build_top_contribution_table(output_df, X_pred, shap_values, top_n, top_features_per_player):
    """
    Build a table for the top N predicted players, keeping only the top
    feature contributions by absolute SHAP value for each player.
    """
    name_col = get_player_name_column(output_df)

    ranked = output_df.sort_values(by="predicted_probability", ascending=False).head(top_n).copy()

    contribution_rows = []

    for idx in ranked.index:
        player_name = output_df.loc[idx, name_col]
        predicted_probability = output_df.loc[idx, "predicted_probability"]

        player_shap = shap_values[idx]
        player_features = X_pred.loc[idx]

        shap_series = pd.Series(player_shap, index=X_pred.columns)
        top_features = shap_series.abs().sort_values(ascending=False).head(top_features_per_player).index

        row = {
            "player_name": player_name,
            "predicted_probability": predicted_probability
        }

        for feature in top_features:
            row[f"{feature}_contribution"] = shap_series[feature]
            row[f"{feature}_value"] = player_features[feature]

        contribution_rows.append(row)

    return ranked, pd.DataFrame(contribution_rows)


def plot_stacked_shap_bars(output_df, X_pred, shap_values, title, output_path, top_n=5, top_features_per_player=5):
    """
    Create a stacked horizontal bar chart for the top N predicted players.
    Each bar is split into the top SHAP feature contributions for that player.
    Positive contributions push probability upward; negative contributions push it downward.
    """
    name_col = get_player_name_column(output_df)

    ranked = output_df.sort_values(by="predicted_probability", ascending=False).head(top_n).copy()

    player_names = ranked[name_col].tolist()
    player_probs = ranked["predicted_probability"].tolist()
    ranked_indices = ranked.index.tolist()

    plt.figure(figsize=(12, 8))

    # We'll manually stack positive and negative contributions separately
    y_positions = np.arange(len(player_names))

    # Collect all unique top features across the selected players
    feature_sets = []
    player_top_shap = {}

    for idx in ranked_indices:
        shap_series = pd.Series(shap_values[idx], index=X_pred.columns)
        top_features = shap_series.abs().sort_values(ascending=False).head(top_features_per_player)
        player_top_shap[idx] = top_features
        feature_sets.extend(top_features.index.tolist())

    unique_features = list(dict.fromkeys(feature_sets))

    # Use matplotlib default color cycle
    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    feature_colors = {
        feature: color_cycle[i % len(color_cycle)]
        for i, feature in enumerate(unique_features)
    }

    # Plot stacked contributions
    for i, idx in enumerate(ranked_indices):
        shap_series = player_top_shap[idx]

        pos_left = 0.0
        neg_left = 0.0

        # Plot negative contributions first
        for feature, value in shap_series.items():
            if value < 0:
                plt.barh(
                    y=i,
                    width=value,
                    left=neg_left,
                    color=feature_colors[feature],
                    edgecolor="white",
                    label=feature if i == 0 else None
                )
                neg_left += value

        # Plot positive contributions
        for feature, value in shap_series.items():
            if value > 0:
                plt.barh(
                    y=i,
                    width=value,
                    left=pos_left,
                    color=feature_colors[feature],
                    edgecolor="white",
                    label=feature if i == 0 else None
                )
                pos_left += value

        # Add probability label at the right
        plt.text(
            x=max(pos_left, 0) + 0.01,
            y=i,
            s=f"p={player_probs[i]:.3f}",
            va="center"
        )

    plt.axvline(x=0, linewidth=1)

    plt.yticks(y_positions, player_names)
    plt.gca().invert_yaxis()

    plt.xlabel("SHAP contribution to positive-class prediction")
    plt.title(title)

    # Clean duplicate legend entries
    handles, labels = plt.gca().get_legend_handles_labels()
    dedup = dict(zip(labels, handles))
    plt.legend(dedup.values(), dedup.keys(), bbox_to_anchor=(1.02, 1), loc="upper left")

    plt.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info("Saved SHAP stacked bar chart to %s", output_path)
    print(f"Saved chart to {output_path}")


# -------------------------------
# Main visualization pipeline
# -------------------------------
def main():
    try:
        breakout_artifact = load_model_artifact(BREAKOUT_MODEL_PATH)
        regression_artifact = load_model_artifact(REGRESSION_MODEL_PATH)

        breakout_output_df, breakout_X, breakout_shap = compute_predictions_and_shap(
            input_csv=BREAKOUT_2025_PATH,
            model_artifact=breakout_artifact,
            dataset_name="Breakout Candidates (Age 20-25, 2025)"
        )

        regression_output_df, regression_X, regression_shap = compute_predictions_and_shap(
            input_csv=REGRESSION_2025_PATH,
            model_artifact=regression_artifact,
            dataset_name="Regression Candidates (Age 30-35, 2025)"
        )

        plot_stacked_shap_bars(
            output_df=breakout_output_df,
            X_pred=breakout_X,
            shap_values=breakout_shap,
            title="Top 5 Predicted Breakout Candidates (2025 data) with SHAP Contributions",
            output_path=os.path.join(OUTPUT_DIR, "top5_breakout_shap.png"),
            top_n=TOP_N,
            top_features_per_player=TOP_FEATURES_PER_PLAYER
        )

        plot_stacked_shap_bars(
            output_df=regression_output_df,
            X_pred=regression_X,
            shap_values=regression_shap,
            title="Top 5 Predicted Regression Candidates (2025 data) with SHAP Contributions",
            output_path=os.path.join(OUTPUT_DIR, "top5_regression_shap.png"),
            top_n=TOP_N,
            top_features_per_player=TOP_FEATURES_PER_PLAYER
        )

        print("\nVisualization pipeline completed successfully.")

    except Exception as e:
        logger.error("An error occurred in visualization.py: %s", e)
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()