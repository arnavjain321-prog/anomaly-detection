# app.py
import io
import json
import logging
import os
import boto3
import pandas as pd
import requests
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from baseline import BaselineManager
from processor import process_file

# ── Logging setup ─────────────────────────────────────────────────────────────

LOG_FILE = "/var/log/anomaly-detection/app.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("app")

# ── App and AWS setup ─────────────────────────────────────────────────────────

app = FastAPI(title="Anomaly Detection Pipeline")

try:
    s3 = boto3.client("s3")
    BUCKET_NAME = os.environ["BUCKET_NAME"]
    logger.info(f"App started. Using bucket: {BUCKET_NAME}")
except KeyError:
    logger.error("BUCKET_NAME environment variable is not set.")
    raise RuntimeError("BUCKET_NAME environment variable is not set.")


# ── SNS subscription confirmation + message handler ──────────────────────────

@app.post("/notify")
async def handle_sns(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception as e:
        logger.error(f"/notify: Failed to parse request body: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    msg_type = request.headers.get("x-amz-sns-message-type")
    logger.info(f"/notify: Received SNS message type: {msg_type}")

    if msg_type == "SubscriptionConfirmation":
        confirm_url = body.get("SubscribeURL")
        if not confirm_url:
            logger.error("/notify: SubscriptionConfirmation missing SubscribeURL")
            raise HTTPException(status_code=400, detail="Missing SubscribeURL")
        try:
            resp = requests.get(confirm_url, timeout=10)
            resp.raise_for_status()
            logger.info(f"/notify: SNS subscription confirmed via {confirm_url}")
        except requests.RequestException as e:
            logger.error(f"/notify: Failed to confirm SNS subscription: {e}")
            raise HTTPException(status_code=500, detail="Subscription confirmation failed")
        return {"status": "confirmed"}

    if msg_type == "Notification":
        try:
            s3_event = json.loads(body["Message"])
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"/notify: Failed to parse SNS Notification message: {e}")
            raise HTTPException(status_code=400, detail="Invalid SNS message payload")

        for record in s3_event.get("Records", []):
            try:
                key = record["s3"]["object"]["key"]
            except KeyError as e:
                logger.warning(f"/notify: Malformed S3 record, skipping: {e}")
                continue

            if key.startswith("raw/") and key.endswith(".csv"):
                logger.info(f"/notify: Queuing background processing for s3://{BUCKET_NAME}/{key}")
                background_tasks.add_task(process_file, BUCKET_NAME, key)
            else:
                logger.debug(f"/notify: Skipping non-target key: {key}")

    return {"status": "ok"}


# ── Query endpoints ───────────────────────────────────────────────────────────

@app.get("/anomalies/recent")
def get_recent_anomalies(limit: int = 50):
    """Return rows flagged as anomalies across the 10 most recent processed files."""
    try:
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
    except Exception as e:
        logger.error(f"/anomalies/recent: Failed to list processed files: {e}")
        raise HTTPException(status_code=500, detail="Failed to list processed files")

    all_anomalies = []
    for key in keys:
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_csv(io.BytesIO(response["Body"].read()))
            if "anomaly" in df.columns:
                flagged = df[df["anomaly"] == True].copy()
                flagged["source_file"] = key
                all_anomalies.append(flagged)
        except Exception as e:
            logger.warning(f"/anomalies/recent: Skipping {key} due to error: {e}")
            continue

    if not all_anomalies:
        return {"count": 0, "anomalies": []}

    try:
        combined = pd.concat(all_anomalies).head(limit)
        return {"count": len(combined), "anomalies": combined.to_dict(orient="records")}
    except Exception as e:
        logger.error(f"/anomalies/recent: Failed to combine anomaly data: {e}")
        raise HTTPException(status_code=500, detail="Failed to assemble anomaly results")


@app.get("/anomalies/summary")
def get_anomaly_summary():
    """Aggregate anomaly rates across all processed files using their summary JSONs."""
    try:
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
                        logger.warning(f"/anomalies/summary: Skipping {obj['Key']}: {e}")
                        continue
    except Exception as e:
        logger.error(f"/anomalies/summary: Failed to list summary files: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve summaries")

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


@app.get("/baseline/current")
def get_current_baseline():
    """Show the current per-channel statistics the detector is working from."""
    try:
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()
    except Exception as e:
        logger.error(f"/baseline/current: Failed to load baseline: {e}")
        raise HTTPException(status_code=500, detail="Failed to load baseline")

    channels = {}
    for channel, stats in baseline.items():
        if channel == "last_updated":
            continue
        try:
            channels[channel] = {
                "observations": stats["count"],
                "mean": round(stats["mean"], 4),
                "std": round(stats.get("std", 0.0), 4),
                "baseline_mature": stats["count"] >= 30,
            }
        except (KeyError, TypeError) as e:
            logger.warning(f"/baseline/current: Skipping malformed channel '{channel}': {e}")
            continue

    return {
        "last_updated": baseline.get("last_updated"),
        "channels": channels,
    }


@app.get("/health")
def health():
    return {"status": "ok", "bucket": BUCKET_NAME, "timestamp": datetime.utcnow().isoformat()}
