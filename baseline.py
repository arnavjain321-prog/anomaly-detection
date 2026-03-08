#!/usr/bin/env python3
import json
import math
import boto3
import logging
import os
from datetime import datetime
from typing import Optional
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)

LOG_FILE = "app.log"
LOG_S3_KEY = "logs/app.log"


class BaselineManager:
    """
    Maintains a per-channel running baseline using Welford's online algorithm,
    which computes mean and variance incrementally without storing all past data.
    """

    def __init__(self, bucket: str, baseline_key: str = "state/baseline.json"):
        self.bucket = bucket
        self.baseline_key = baseline_key

    def load(self) -> dict:
        try:
            logger.info("Loading baseline from s3://%s/%s", self.bucket, self.baseline_key)
            response = s3.get_object(Bucket=self.bucket, Key=self.baseline_key)
            baseline = json.loads(response["Body"].read())
            logger.info("Baseline loaded successfully")
            return baseline

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["NoSuchKey", "404"]:
                logger.warning(
                    "Baseline file not found at s3://%s/%s, starting fresh",
                    self.bucket,
                    self.baseline_key
                )
                return {}
            logger.exception("Failed to load baseline from S3: %s", e)
            raise

        except json.JSONDecodeError as e:
            logger.exception("Baseline file is not valid JSON: %s", e)
            raise

        except Exception as e:
            logger.exception("Unexpected error while loading baseline: %s", e)
            raise

    def save(self, baseline: dict):
        try:
            baseline["last_updated"] = datetime.utcnow().isoformat()
            logger.info("Saving baseline to s3://%s/%s", self.bucket, self.baseline_key)

            s3.put_object(
                Bucket=self.bucket,
                Key=self.baseline_key,
                Body=json.dumps(baseline, indent=2),
                ContentType="application/json"
            )

            logger.info("Baseline saved successfully")
            self.upload_log_file()

        except Exception as e:
            logger.exception("Failed to save baseline: %s", e)
            raise

    def upload_log_file(self):
        try:
            if not os.path.exists(LOG_FILE):
                logger.warning("Log file %s does not exist yet, skipping upload", LOG_FILE)
                return

            logger.info("Uploading log file to s3://%s/%s", self.bucket, LOG_S3_KEY)
            s3.upload_file(LOG_FILE, self.bucket, LOG_S3_KEY)
            logger.info("Log file uploaded successfully")

        except Exception as e:
            logger.exception("Failed to upload log file to S3: %s", e)

    def update(self, baseline: dict, channel: str, new_values: list[float]) -> dict:
        """
        Welford's online algorithm for numerically stable mean and variance.
        Each channel tracks: count, mean, M2 (sum of squared deviations).
        Variance = M2 / count, std = sqrt(variance).
        """
        try:
            logger.info("Updating baseline for channel '%s' with %d new values", channel, len(new_values))

            if channel not in baseline:
                baseline[channel] = {"count": 0, "mean": 0.0, "M2": 0.0}
                logger.info("Initialized new channel baseline for '%s'", channel)

            state = baseline[channel]

            for value in new_values:
                state["count"] += 1
                delta = value - state["mean"]
                state["mean"] += delta / state["count"]
                delta2 = value - state["mean"]
                state["M2"] += delta * delta2

            if state["count"] >= 2:
                variance = state["M2"] / state["count"]
                state["std"] = math.sqrt(variance)
            else:
                state["std"] = 0.0

            baseline[channel] = state

            logger.info(
                "Updated channel '%s': count=%d, mean=%.4f, std=%.4f",
                channel,
                state["count"],
                state["mean"],
                state["std"]
            )

            return baseline

        except Exception as e:
            logger.exception("Failed to update baseline for channel '%s': %s", channel, e)
            raise

    def get_stats(self, baseline: dict, channel: str) -> Optional[dict]:
        try:
            stats = baseline.get(channel)
            if stats is None:
                logger.warning("No baseline stats found for channel '%s'", channel)
            return stats
        except Exception as e:
            logger.exception("Failed to get stats for channel '%s': %s", channel, e)
            raise