# 🧠 AI-Driven Cross-Region S3 Migration with AWS Boto3

This roadmap walks through building a fully automated, serverless data pipeline that migrates files between Amazon S3 buckets in different regions using **AWS Lambda**, **SNS**, **SQS**, **Glue**, and **Athena** — all orchestrated using **Boto3 SDK** in Python.

## 🗺️ Project Goals

- 🗂️ Migrate files across S3 buckets in different AWS regions.
- ⚙️ Automate the flow using event-driven architecture (SNS → SQS → Lambda).
- 🧪 Catalog and query the target data using Glue + Athena.
- 🔐 Manage resources securely with IAM roles and policies.
- 💡 Implement the entire pipeline using Python + Boto3 SDK.

---

## ✅ Prerequisites

- AWS CLI configured
- Python 3.x with `boto3`, `python-dotenv`
- IAM user with admin permissions (or scoped access for S3, Lambda, SNS, SQS, Glue, Athena)

---

## 📁 Project Structure

```
project/
│
├── run_data.json               # Resource tracking and deployment status
├── .env                        # AWS credentials and region
├── roadmap.md                  # This roadmap
├── deploy.py                   # Main deployment orchestrator
├── iam-s3.py                   # IAM role and S3 bucket creation
├── deploy/
│   ├── create_buckets.py
│   ├── setup_iam_role.py
│   ├── create_sns_topic.py
│   ├── create_sqs_queue.py
│   ├── create_lambda.py
│   ├── create_glue_crawler.py
│   └── setup_s3_notifications.py
└── lambda/
    └── handler.py              # Lambda source code
```

---

## 🔧 Deployment Orchestration

**`deploy.py`** - Main deployment orchestrator that calls all component creation scripts in the correct order:
- Manages the deployment workflow
- Updates `run_data.json` with resource information
- Handles error checking and rollback if needed
- Coordinates cross-region resource creation

**`iam-s3.py`** - First component script that creates:
- IAM role (`s3-to-s3-role`) with required policies
- Source S3 bucket in `us-west-1`
- Target S3 bucket in `us-east-1`
- Updates deployment status in `run_data.json`

---

## 🚀 Step-by-Step Roadmap

### 1. IAM Role Creation

- Use `boto3.client('iam')` to:
  - Create a role with a custom trust policy (Glue, Lambda)
  - Attach policies:
    - `AmazonS3FullAccess`
    - `AmazonSQSFullAccess`
    - `AWSGlueConsoleFullAccess`
    - `CloudWatchLogsFullAccess`
- Name: `s3-to-s3-role`

### 2. S3 Buckets

- Create two S3 buckets using `boto3.client('s3')`
  - Source bucket in non-default region (e.g., `us-west-1`)
  - Target bucket in default region (e.g., `us-east-1`)
- Enable versioning and encryption if required

### 3. SNS Topic

- Use `boto3.client('sns')` in the **source region**
- Create topic: `s3-to-s3-sns-topic`
- Set public access (for demo only) or restrict to IAM principals

### 4. SQS Queue

- Use `boto3.client('sqs')` in the **target region**
- Create queue: `s3-to-s3-sqs-lambda-queue`
- Subscribe to SNS topic:
  - `sns.subscribe()` → `sqs.queue_arn`

### 5. Lambda Function

- Create Lambda in the **target region**
- Runtime: Python 3.x
- Role: `s3-to-s3-role`
- Upload zipped `lambda/handler.py` or use `create_function(Code={'ZipFile': ...})`
- Grant SQS trigger permissions

#### Lambda Code Logic (Overview):

```python
import boto3, json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(json.loads(record['body'])['Message'])
        src_bucket = payload['Records'][0]['s3']['bucket']['name']
        key = payload['Records'][0]['s3']['object']['key']
        s3.copy_object(
            Bucket='Your-Target-S3-Bucket-Name',
            CopySource={'Bucket': src_bucket, 'Key': key},
            Key=key
        )
```

### 6. Glue Crawler

- Use `boto3.client('glue')`
- Create a CSV classifier
- Create Glue Crawler:
  - Name: `s3-to-s3-crawler`
  - Source: Target S3 bucket
  - Classifier: CSV
  - Role: `s3-to-s3-role`
  - Schedule: On demand
  - Output: Default database

### 7. S3 Event Notification

- Use `boto3.client('s3')` to update bucket notification config:
  - Event: `s3:ObjectCreated:*`
  - Destination: SNS topic ARN

---

## 🧪 Testing Flow

1. Upload `customers.csv` to source S3 bucket (via Console or Boto3)
2. Observe:
   - SNS triggers SQS
   - Lambda triggered by SQS
   - Lambda copies file to target S3 bucket
3. Manually start Glue crawler (or automate via Lambda extension)
4. Verify:
   - Table appears in Glue Catalog
   - Use Athena to query table with SQL

---

## 🔍 Validation Checklist

- [ ] IAM Role created and attached
- [ ] Buckets created and populated
- [ ] SNS + SQS integrated
- [ ] Lambda functional and tested
- [ ] Crawler correctly configured
- [ ] Athena queries return expected results

---

## 🛠️ Optional Enhancements

- Add Dead Letter Queue (DLQ) to Lambda
- Add Lambda error monitoring via CloudWatch
- Use CDK or Terraform for repeatable deployments
- Add CloudTrail for audit

---

## 📚 References

- [Boto3 Docs](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [AWS S3 Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html)
- [Glue Crawler Setup](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
- [Athena SQL](https://docs.aws.amazon.com/athena/latest/ug/querying.html)

---

## 🧠 Final Note

This pipeline introduces real-world AWS services and patterns commonly used in **data lake architectures**, **AI training pipelines**, and **automated analytics workflows**.

Own the process. Automate everything. Query anything.

Let me know if you'd like a ZIP file with example Python scripts or want it broken into multiple `.py` modules for deployment automation.
