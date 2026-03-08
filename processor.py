#!/usr/bin/env python3
import json
import io
import boto3
import pandas as pd
import logging
from datetime import datetime

from baseline import BaselineManager
from detector import AnomalyDetector

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)

NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"]


def process_file(bucket: str, key: str):
    logger.info("Processing started for s3://%s/%s", bucket, key)

    try:
        # 1. Download raw file
        logger.info("Downloading raw file from s3://%s/%s", bucket, key)
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        logger.info("Loaded %d rows with columns: %s", len(df), list(df.columns))

        if df.empty:
            logger.warning("Input file %s is empty", key)

        # 2. Load current baseline
        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()
        logger.info("Baseline loaded successfully for processing %s", key)

        # 3. Update baseline with values from this batch BEFORE scoring
        for col in NUMERIC_COLS:
            if col in df.columns:
                clean_values = df[col].dropna().tolist()
                if clean_values:
                    logger.info(
                        "Updating baseline for column '%s' with %d values",
                        col,
                        len(clean_values)
                    )
                    baseline = baseline_mgr.update(baseline, col, clean_values)
                else:
                    logger.warning("Column '%s' exists but has no non-null values", col)
            else:
                logger.warning("Expected numeric column '%s' not found in input file", col)

        # 4. Run detection
        logger.info("Running anomaly detection for file %s", key)
        detector = AnomalyDetector(z_threshold=3.0, contamination=0.05)
        scored_df = detector.run(df, NUMERIC_COLS, baseline, method="both")
        logger.info("Anomaly detection complete for file %s", key)

        # 5. Write scored file to processed/ prefix
        output_key = key.replace("raw/", "processed/")
        csv_buffer = io.StringIO()
        scored_df.to_csv(csv_buffer, index=False)

        logger.info("Writing scored output to s3://%s/%s", bucket, output_key)
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )

        # 6. Save updated baseline back to S3
        logger.info("Saving updated baseline after processing %s", key)
        baseline_mgr.save(baseline)

        # 7. Build processing summary
        anomaly_count = int(scored_df["anomaly"].sum()) if "anomaly" in scored_df.columns else 0
        summary = {
            "source_key": key,
            "output_key": output_key,
            "processed_at": datetime.utcnow().isoformat(),
            "total_rows": len(df),
            "anomaly_count": anomaly_count,
            "anomaly_rate": round(anomaly_count / len(df), 4) if len(df) > 0 else 0,
            "baseline_observation_counts": {
                col: baseline.get(col, {}).get("count", 0) for col in NUMERIC_COLS
            }
        }

        # 8. Write summary JSON
        summary_key = output_key.replace(".csv", "_summary.json")
        logger.info("Writing summary JSON to s3://%s/%s", bucket, summary_key)
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )

        logger.info(
            "Processing complete for %s: %d/%d anomalies flagged",
            key,
            anomaly_count,
            len(df)
        )

        return summary

    except Exception as e:
        logger.exception("Failed processing file s3://%s/%s: %s", bucket, key, e)
        raise