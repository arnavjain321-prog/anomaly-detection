import io
import json
import os
import logging
import boto3
import pandas as pd
import requests
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from baseline import BaselineManager
from processor import process_file

LOG_FILE = "app.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Anomaly Detection Pipeline")

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME environment variable is not set")

logger.info("Application starting with bucket: %s", BUCKET_NAME)


@app.post("/notify")
async def handle_sns(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
        msg_type = request.headers.get("x-amz-sns-message-type")
        logger.info("Received SNS request with message type: %s", msg_type)

        if msg_type == "SubscriptionConfirmation":
            confirm_url = body.get("SubscribeURL")
            if not confirm_url:
                logger.error("SubscriptionConfirmation missing SubscribeURL")
                raise HTTPException(status_code=400, detail="Missing SubscribeURL")

            logger.info("Confirming SNS subscription at URL: %s", confirm_url)
            resp = requests.get(confirm_url, timeout=10)
            resp.raise_for_status()
            logger.info("SNS subscription confirmed successfully")
            return {"status": "confirmed"}

        if msg_type == "Notification":
            message = body.get("Message")
            if not message:
                logger.error("Notification missing Message body")
                raise HTTPException(status_code=400, detail="Missing Message")

            s3_event = json.loads(message)
            records = s3_event.get("Records", [])
            logger.info("SNS notification contains %d record(s)", len(records))

            for record in records:
                try:
                    key = record["s3"]["object"]["key"]
                    logger.info("Received S3 event for key: %s", key)

                    if key.startswith("raw/") and key.endswith(".csv"):
                        logger.info("Queueing background task for file: %s", key)
                        background_tasks.add_task(process_file, BUCKET_NAME, key)
                    else:
                        logger.info("Skipping non-matching key: %s", key)

                except KeyError as e:
                    logger.exception("Malformed SNS/S3 event record: missing key %s", e)

        return {"status": "ok"}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error handling SNS message: %s", e)
        raise HTTPException(status_code=500, detail="Failed to handle SNS message")


@app.get("/anomalies/recent")
def get_recent_anomalies(limit: int = 50):
    try:
        logger.info("Fetching recent anomalies with limit=%d", limit)

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")

        keys = sorted(
            [
                obj["Key"]
                for page in pages
                for obj in page.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ],
            reverse=True,
        )[:10]

        logger.info("Found %d recent processed CSV files", len(keys))

        all_anomalies = []
        for key in keys:
            try:
                response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                df = pd.read_csv(io.BytesIO(response["Body"].read()))

                if "anomaly" in df.columns:
                    flagged = df[df["anomaly"] == True].copy()
                    flagged["source_file"] = key
                    all_anomalies.append(flagged)
                    logger.info("Found %d anomalies in %s", len(flagged), key)
                else:
                    logger.warning("File %s missing 'anomaly' column", key)

            except Exception as e:
                logger.exception("Error reading processed file %s: %s", key, e)

        if not all_anomalies:
            return {"count": 0, "anomalies": []}

        combined = pd.concat(all_anomalies).head(limit)
        return {"count": len(combined), "anomalies": combined.to_dict(orient="records")}

    except Exception as e:
        logger.exception("Error fetching recent anomalies: %s", e)
        raise HTTPException(status_code=500, detail="Failed to fetch recent anomalies")


@app.get("/anomalies/summary")
def get_anomaly_summary():
    try:
        logger.info("Fetching anomaly summary")

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")

        summaries = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("_summary.json"):
                    try:
                        response = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
                        summaries.append(json.loads(response["Body"].read()))
                    except Exception as e:
                        logger.exception("Failed to read summary file %s: %s", obj["Key"], e)

        if not summaries:
            return {"message": "No processed files yet."}

        total_rows = sum(s["total_rows"] for s in summaries)
        total_anomalies = sum(s["anomaly_count"] for s in summaries)

        return {
            "files_processed": len(summaries),
            "total_rows_scored": total_rows,
            "total_anomalies": total_anomalies,
            "overall_anomaly_rate": round(total_anomalies / total_rows, 4) if total_rows > 0 else 0,
            "most_recent": sorted(summaries, key=lambda x: x["processed_at"], reverse=True)[:5],
        }

    except Exception as e:
        logger.exception("Error building anomaly summary: %s", e)
        raise HTTPException(status_code=500, detail="Failed to build anomaly summary")


@app.get("/baseline/current")
def get_current_baseline():
    try:
        logger.info("Fetching current baseline")
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()

        channels = {}
        for channel, stats in baseline.items():
            if channel == "last_updated":
                continue
            channels[channel] = {
                "observations": stats["count"],
                "mean": round(stats["mean"], 4),
                "std": round(stats.get("std", 0.0), 4),
                "baseline_mature": stats["count"] >= 30,
            }

        return {
            "last_updated": baseline.get("last_updated"),
            "channels": channels,
        }

    except Exception as e:
        logger.exception("Error fetching current baseline: %s", e)
        raise HTTPException(status_code=500, detail="Failed to fetch baseline")


@app.get("/health")
def health():
    logger.info("Health check requested")
    return {
        "status": "ok",
        "bucket": BUCKET_NAME,
        "timestamp": datetime.utcnow().isoformat()
    }