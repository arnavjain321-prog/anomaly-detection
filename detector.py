#!/usr/bin/env python3
import logging
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from typing import Optional

logger = logging.getLogger(__name__)


class AnomalyDetector:

    def __init__(self, z_threshold: float = 3.0, contamination: float = 0.05):
        self.z_threshold = z_threshold
        self.contamination = contamination

    def zscore_flag(
        self,
        values: pd.Series,
        mean: float,
        std: float
    ) -> pd.Series:
        """
        Flag values more than z_threshold standard deviations from the
        established baseline mean. Returns a Series of z-scores.
        """
        try:
            if std == 0:
                logger.warning("Standard deviation is zero; returning zero z-scores")
                return pd.Series([0.0] * len(values), index=values.index)
            return (values - mean).abs() / std
        except Exception as e:
            logger.exception("Failed to compute z-scores: %s", e)
            raise

    def isolation_forest_flag(self, df: pd.DataFrame, numeric_cols: list[str]):
        """
        Multivariate anomaly detection across all numeric channels simultaneously.
        IsolationForest returns -1 for anomalies, 1 for normal points.
        Scores closer to -1 indicate stronger anomalies.
        """
        try:
            available_cols = [col for col in numeric_cols if col in df.columns]

            if not available_cols:
                raise ValueError("No valid numeric columns available for IsolationForest")

            logger.info("Running IsolationForest on columns: %s", available_cols)

            model = IsolationForest(
                contamination=self.contamination,
                random_state=42,
                n_estimators=100
            )

            X = df[available_cols].fillna(df[available_cols].median())

            if X.empty:
                raise ValueError("Input data for IsolationForest is empty")

            model.fit(X)

            labels = model.predict(X)
            scores = model.decision_function(X)

            logger.info("IsolationForest completed successfully")
            return labels, scores

        except Exception as e:
            logger.exception("IsolationForest failed: %s", e)
            raise

    def run(
        self,
        df: pd.DataFrame,
        numeric_cols: list[str],
        baseline: dict,
        method: str = "both"
    ) -> pd.DataFrame:
        try:
            logger.info("Starting anomaly detection with method='%s'", method)
            result = df.copy()

            available_cols = [col for col in numeric_cols if col in df.columns]
            missing_cols = [col for col in numeric_cols if col not in df.columns]

            if missing_cols:
                logger.warning("Missing numeric columns: %s", missing_cols)

            # --- Z-score per channel ---
            if method in ("zscore", "both"):
                for col in available_cols:
                    stats = baseline.get(col)
                    if stats and stats.get("count", 0) >= 30:
                        z_scores = self.zscore_flag(df[col], stats["mean"], stats["std"])
                        result[f"{col}_zscore"] = z_scores.round(4)
                        result[f"{col}_zscore_flag"] = z_scores > self.z_threshold
                        logger.info("Computed z-scores for column '%s'", col)
                    else:
                        result[f"{col}_zscore"] = None
                        result[f"{col}_zscore_flag"] = None
                        logger.info(
                            "Baseline for column '%s' not mature enough for z-score detection",
                            col
                        )

            # --- IsolationForest across all channels ---
            if method in ("isolation", "both"):
                labels, scores = self.isolation_forest_flag(df, available_cols)
                result["if_label"] = labels
                result["if_score"] = scores.round(4)
                result["if_flag"] = labels == -1

            # --- Final anomaly flag ---
            if method == "both":
                zscore_flags = [
                    result[f"{col}_zscore_flag"]
                    for col in available_cols
                    if f"{col}_zscore_flag" in result.columns
                    and result[f"{col}_zscore_flag"].notna().any()
                ]
                if zscore_flags:
                    any_zscore = pd.concat(zscore_flags, axis=1).any(axis=1)
                    result["anomaly"] = any_zscore | result["if_flag"]
                else:
                    result["anomaly"] = result["if_flag"]

            elif method == "zscore":
                zscore_flags = [
                    result[f"{col}_zscore_flag"]
                    for col in available_cols
                    if f"{col}_zscore_flag" in result.columns
                    and result[f"{col}_zscore_flag"].notna().any()
                ]
                if zscore_flags:
                    result["anomaly"] = pd.concat(zscore_flags, axis=1).any(axis=1)
                else:
                    result["anomaly"] = False

            elif method == "isolation":
                result["anomaly"] = result["if_flag"]

            logger.info("Anomaly detection finished successfully")
            return result

        except Exception as e:
            logger.exception("Anomaly detection run failed: %s", e)
            raise