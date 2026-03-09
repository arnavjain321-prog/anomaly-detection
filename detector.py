#!/usr/bin/env python3
import logging
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from typing import Optional

logger = logging.getLogger("detector")


class AnomalyDetector:
    def __init__(self, z_threshold: float = 3.0, contamination: float = 0.05):
        self.z_threshold = z_threshold
        self.contamination = contamination

    def zscore_flag(
        self,
        values: pd.Series,
        mean: float,
        std: float,
    ) -> pd.Series:
        """
        Flag values more than z_threshold standard deviations from the
        established baseline mean. Returns a Series of z-scores.
        """
        try:
            if std == 0:
                return pd.Series([0.0] * len(values))
            return (values - mean).abs() / std
        except Exception as e:
            logger.error(f"zscore_flag failed: {e}")
            raise

    def isolation_forest_flag(self, df: pd.DataFrame, numeric_cols: list[str]):
        """
        Multivariate anomaly detection across all numeric channels simultaneously.
        IsolationForest returns -1 for anomalies, 1 for normal points.
        Scores closer to -1 indicate stronger anomalies.
        """
        try:
            model = IsolationForest(
                contamination=self.contamination,
                random_state=42,
                n_estimators=100,
            )
            X = df[numeric_cols].fillna(df[numeric_cols].median())
            model.fit(X)
            labels = model.predict(X)
            scores = model.decision_function(X)
            logger.info(
                f"IsolationForest run on {len(df)} rows, "
                f"{int((labels == -1).sum())} anomalies flagged."
            )
            return labels, scores
        except Exception as e:
            logger.error(f"IsolationForest detection failed: {e}")
            raise

    def run(
        self,
        df: pd.DataFrame,
        numeric_cols: list[str],
        baseline: dict,
        method: str = "both",
    ) -> pd.DataFrame:
        try:
            result = df.copy()

            # --- Z-score per channel ---
            if method in ("zscore", "both"):
                for col in numeric_cols:
                    stats = baseline.get(col)
                    if stats and stats["count"] >= 30:
                        try:
                            z_scores = self.zscore_flag(df[col], stats["mean"], stats["std"])
                            result[f"{col}_zscore"] = z_scores.round(4)
                            result[f"{col}_zscore_flag"] = z_scores > self.z_threshold
                            flagged = int((z_scores > self.z_threshold).sum())
                            logger.info(f"Z-score for '{col}': {flagged} flags (threshold={self.z_threshold})")
                        except Exception as e:
                            logger.warning(f"Z-score failed for column '{col}': {e}")
                            result[f"{col}_zscore"] = None
                            result[f"{col}_zscore_flag"] = None
                    else:
                        logger.info(f"Skipping z-score for '{col}': insufficient baseline history.")
                        result[f"{col}_zscore"] = None
                        result[f"{col}_zscore_flag"] = None

            # --- IsolationForest across all channels ---
            if method in ("isolation", "both"):
                labels, scores = self.isolation_forest_flag(df, numeric_cols)
                result["if_label"] = labels
                result["if_score"] = scores.round(4)
                result["if_flag"] = labels == -1

            # --- Consensus flag ---
            if method == "both":
                zscore_flags = [
                    result[f"{col}_zscore_flag"]
                    for col in numeric_cols
                    if f"{col}_zscore_flag" in result.columns
                    and result[f"{col}_zscore_flag"].notna().any()
                ]
                if zscore_flags:
                    any_zscore = pd.concat(zscore_flags, axis=1).any(axis=1)
                    result["anomaly"] = any_zscore | result["if_flag"]
                else:
                    result["anomaly"] = result["if_flag"]

            total_anomalies = int(result["anomaly"].sum()) if "anomaly" in result.columns else 0
            logger.info(f"Detection complete: {total_anomalies}/{len(df)} rows flagged as anomalies.")
            return result

        except Exception as e:
            logger.error(f"AnomalyDetector.run failed: {e}")
            raise