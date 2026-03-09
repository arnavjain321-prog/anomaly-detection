# DS5220 Data Project 1 — Graduate Questions
**Name:** Arnav Jain
**Date:** March 2026

---

## Question 1: Technical Challenges

Honestly the biggest headache going from CloudFormation to Terraform was the S3 and SNS circular dependency issue. In CloudFormation, if you try to put the bucket notification config directly in the S3 bucket resource, it breaks because the SNS topic policy that lets S3 publish to the topic doesn't exist yet — and CloudFormation can't figure out how to order those two things. I ended up having to create a Lambda-backed custom resource just to set the notification after both resources were already up. That felt like a lot of extra boilerplate just to wire two things together.

Terraform actually made this way easier. You just create an `aws_s3_bucket_notification` resource separately and reference both the bucket and the topic in it. Terraform handles the dependency ordering on its own so you don't need any workaround. That was probably the biggest "aha" moment for me between the two tools — CloudFormation is more rigid about what you can do at creation time, while Terraform is more flexible since it builds a dependency graph and applies things in the right order automatically.

---

## Question 2: Access Permissions

The element that grants SNS permission to send messages to the API is the `SNSTopicPolicy` resource in `cloudformation.yaml` (around line 43). The key part is this statement inside the policy:

```yaml
- Sid: AllowS3PublishToTopic
  Effect: Allow
  Principal:
    Service: s3.amazonaws.com
  Action: sns:Publish
  Resource: !Ref SNSTopic
  Condition:
    ArnLike:
      aws:SourceArn: !Sub "arn:aws:s3:::${DataBucket}"
```

This gives S3 permission to call `sns:Publish` on the `ds5220-dp1` topic, but only from our specific bucket. Without this, S3 would just get an authorization error when trying to send the notification. Once S3 publishes to SNS, SNS takes care of delivering the HTTP POST to `/notify` on our EC2 instance — SNS subscriptions to HTTP endpoints don't need any additional permissions beyond the topic policy allowing the publish in the first place.

The same thing is in `main.tf` in the `aws_sns_topic_policy` resource, just written as a jsonencode block instead of YAML.

---

## Question 3: Event Flow and Reliability

When a CSV lands in `raw/`, S3 checks if it matches the notification filter (prefix `raw/`, suffix `.csv`). If it does, S3 publishes an event to the `ds5220-dp1` SNS topic. SNS then sends an HTTP POST to `http://<ElasticIP>:8000/notify`. The FastAPI endpoint parses the message, pulls out the S3 object key, and kicks off `process_file()` as a background task. That function downloads the CSV, updates the baseline stats, runs the anomaly detector, writes the scored CSV and summary JSON back to S3 under `processed/`, saves the updated baseline, and syncs the log file.

If the EC2 instance is down or returns an error from `/notify`, SNS will retry — it does a few immediate retries then backs off exponentially. The default retry window for HTTP endpoints can be up to 23 days, but if it never gets a 2xx back it eventually gives up and drops the message. There's no built-in dead letter queue for HTTP subscriptions which is a real problem for reliability.

For production I'd change a few things. First, put an SQS queue between SNS and the processing logic instead of going directly to HTTP — that way messages are stored durably even if the instance is down. Then have the EC2 instance or a Lambda poll the queue. I'd also add a DLQ on the SQS queue to catch anything that fails after retries. And honestly I'd replace the single EC2 instance with something that can scale, since one instance is a single point of failure.

---

## Question 4: IAM and Least Privilege

Looking through the code, the actual S3 operations the app uses are:

- `GetObject` — downloading raw CSVs in `processor.py`, reading `baseline.json` in `baseline.py`, and reading processed files and summaries in `app.py`
- `PutObject` — writing scored CSVs, summary JSONs, `baseline.json`, and `logs/app.log`
- `ListBucket` — paginating through `processed/` objects in the `/anomalies/recent` and `/anomalies/summary` endpoints
- `GetBucketLocation` — the boto3 SDK uses this internally

`DeleteObject` is in the current policy but nothing in the code actually calls it, so it could be removed. A minimal policy that still lets everything work would look like:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME/*"
    }
  ]
}
```

This is a lot tighter than "full access." If the instance got compromised, an attacker could still read and write objects but couldn't delete anything or touch other buckets, which limits the damage quite a bit.

---

## Question 5: Architecture and Scaling

At 100x the file volume, the current setup would fall apart pretty quickly. The FastAPI app runs background tasks in the same process, so a flood of incoming files would overwhelm a single `t3.micro` — you'd start seeing slow processing, dropped notifications, or the instance just running out of memory. The bigger problem though is `baseline.json`. Right now the flow is: read baseline from S3, update it in memory, write it back. If two instances are doing that at the same time they'll overwrite each other and lose observations. It's a classic race condition.

To handle 100x scale I'd rethink the architecture a few ways. Instead of SNS posting directly to HTTP, I'd have SNS publish to an SQS queue and then have a pool of workers pulling from the queue. That lets you scale the number of workers independently and gives you durable message storage. For the baseline consistency problem, I'd move it out of S3 and into something that supports atomic updates — DynamoDB with conditional writes would work, where each worker reads the current version, computes its update, and only writes if the version hasn't changed since it read. If there's a conflict it just retries. Redis with atomic increment operations would also work and would be faster. The tradeoff is more infrastructure to manage and some added latency from the locking, but for anything production-grade that's worth it. Keeping a single shared JSON file in S3 as the source of truth just doesn't scale when multiple writers are involved.
