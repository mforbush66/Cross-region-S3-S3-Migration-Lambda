#!/usr/bin/env python3
"""
SNS, SQS, and Lambda Function Creation Script
Creates the event-driven architecture components:
- SNS topic in source region (us-west-1)
- SQS queue in target region (us-east-1)
- Lambda function in target region (us-east-1)
- Cross-region subscription (SNS -> SQS)
- Lambda trigger from SQS
"""

import boto3
import json
import sys
import os
import tempfile
import zipfile
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_run_data():
    """Load the run_data.json file"""
    try:
        with open('run_data.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: run_data.json not found")
        return None

def save_run_data(data):
    """Save updated data to run_data.json"""
    with open('run_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    print("Updated run_data.json")

def get_account_id():
    """Get AWS account ID"""
    sts = boto3.client('sts')
    return sts.get_caller_identity()['Account']

def create_sns_topic(run_data, account_id):
    """Create SNS topic in source region with S3 publish policy"""
    print("Creating SNS topic...")
    
    source_region = run_data['regions']['source_region']
    sns = boto3.client('sns', region_name=source_region)
    
    # Add timestamp to avoid potential conflicts
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    base_topic_name = run_data['resources']['sns']['topic_name']
    topic_name = f"{base_topic_name}-{timestamp}"
    source_bucket = run_data['resources']['s3']['source_bucket']['name']
    
    try:
        # Create SNS topic
        response = sns.create_topic(Name=topic_name)
        topic_arn = response['TopicArn']
        
        print(f"Created SNS topic: {topic_arn}")
        
        # Create policy that allows S3 to publish to SNS
        policy = {
            "Version": "2012-10-17",
            "Id": "S3-to-SNS-Policy",
            "Statement": [
                {
                    "Sid": "AllowS3Publish",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "s3.amazonaws.com"
                    },
                    "Action": "SNS:Publish",
                    "Resource": topic_arn,
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": account_id
                        },
                        "ArnEquals": {
                            "aws:SourceArn": f"arn:aws:s3:::{source_bucket}"
                        }
                    }
                }
            ]
        }
        
        # Set the topic policy to allow S3 to publish
        sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName='Policy',
            AttributeValue=json.dumps(policy)
        )
        
        print(f"Set SNS topic policy to allow S3 bucket '{source_bucket}' to publish")
        
        # Update run_data with actual values
        run_data['resources']['sns']['topic_name'] = topic_name
        run_data['resources']['sns']['topic_arn'] = topic_arn
        run_data['deployment_status']['sns_topic'] = 'completed'
        
        return topic_arn
        
    except Exception as e:
        print(f"Error creating SNS topic: {str(e)}")
        run_data['deployment_status']['sns_topic'] = 'failed'
        raise

def create_sqs_queue(run_data, account_id):
    """Create SQS queue in target region"""
    print("Creating SQS queue...")
    
    target_region = run_data['regions']['target_region']
    sqs = boto3.client('sqs', region_name=target_region)
    
    # Add timestamp to avoid 60-second deletion cooldown
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    base_queue_name = run_data['resources']['sqs']['queue_name']
    queue_name = f"{base_queue_name}-{timestamp}"
    
    try:
        # Create SQS queue with appropriate attributes
        queue_attributes = {
            'VisibilityTimeout': '300',  # 5 minutes for Lambda processing
            'MessageRetentionPeriod': '1209600',  # 14 days
            'ReceiveMessageWaitTimeSeconds': '20'  # Long polling
        }
        
        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes=queue_attributes
        )
        
        queue_url = response['QueueUrl']
        
        # Get queue ARN
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = queue_attrs['Attributes']['QueueArn']
        
        print(f"Created SQS queue: {queue_arn}")
        print(f"Queue URL: {queue_url}")
        
        # Update run_data with actual values
        run_data['resources']['sqs']['queue_name'] = queue_name
        run_data['resources']['sqs']['queue_url'] = queue_url
        run_data['resources']['sqs']['queue_arn'] = queue_arn
        run_data['deployment_status']['sqs_queue'] = 'completed'
        
        return queue_url, queue_arn
        
    except Exception as e:
        print(f"Error creating SQS queue: {str(e)}")
        run_data['deployment_status']['sqs_queue'] = 'failed'
        raise

def setup_sns_sqs_subscription(run_data, topic_arn, queue_arn):
    """Subscribe SQS queue to SNS topic across regions"""
    print("Setting up SNS -> SQS subscription...")
    
    source_region = run_data['regions']['source_region']
    target_region = run_data['regions']['target_region']
    account_id = get_account_id()
    
    try:
        # Create SNS client in source region
        sns = boto3.client('sns', region_name=source_region)
        
        # Create SQS client in target region
        sqs = boto3.client('sqs', region_name=target_region)
        queue_url = run_data['resources']['sqs']['queue_url']
        
        # Create queue policy to allow SNS to send messages
        queue_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "sns.amazonaws.com"
                    },
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceArn": topic_arn
                        }
                    }
                }
            ]
        }
        
        # Set queue policy
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                'Policy': json.dumps(queue_policy)
            }
        )
        
        # Subscribe SQS to SNS
        subscription_response = sns.subscribe(
            TopicArn=topic_arn,
            Protocol='sqs',
            Endpoint=queue_arn
        )
        
        subscription_arn = subscription_response['SubscriptionArn']
        print(f"Created SNS subscription: {subscription_arn}")
        
        # Update run_data
        run_data['resources']['sqs']['subscribed_to_sns'] = True
        
        return subscription_arn
        
    except Exception as e:
        print(f"Error setting up SNS -> SQS subscription: {str(e)}")
        raise

def create_lambda_function_code():
    """Create Lambda function code for S3 migration"""
    lambda_code = '''
import boto3
import json
import urllib.parse
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function to migrate files from source S3 bucket to target S3 bucket
    Triggered by SQS messages from SNS notifications
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    s3_source = boto3.client('s3', region_name='us-west-1')
    s3_target = boto3.client('s3', region_name='us-east-1')
    
    processed_count = 0
    errors = []
    
    try:
        # Process each SQS record
        for record in event['Records']:
            try:
                # Parse the SQS message body (contains SNS message)
                message_body = json.loads(record['body'])
                sns_message = json.loads(message_body['Message'])
                
                print(f"Processing SNS message: {json.dumps(sns_message)}")
                
                # Process each S3 event record in the SNS message
                for s3_record in sns_message['Records']:
                    # Extract S3 event information
                    event_name = s3_record['eventName']
                    source_bucket = s3_record['s3']['bucket']['name']
                    object_key = urllib.parse.unquote_plus(
                        s3_record['s3']['object']['key'], 
                        encoding='utf-8'
                    )
                    
                    print(f"Processing: {event_name} for {source_bucket}/{object_key}")
                    
                    # Only process ObjectCreated events
                    if event_name.startswith('ObjectCreated'):
                        # Target bucket name (replace with your actual target bucket)
                        target_bucket = 's3-migration-target-758045074543-us-east-1'
                        
                        # Copy object from source to target
                        copy_source = {
                            'Bucket': source_bucket,
                            'Key': object_key
                        }
                        
                        print(f"Copying {source_bucket}/{object_key} to {target_bucket}/{object_key}")
                        
                        s3_target.copy_object(
                            CopySource=copy_source,
                            Bucket=target_bucket,
                            Key=object_key,
                            MetadataDirective='COPY'
                        )
                        
                        processed_count += 1
                        print(f"Successfully copied {object_key}")
                    
                    else:
                        print(f"Skipping event: {event_name}")
                        
            except Exception as record_error:
                error_msg = f"Error processing record: {str(record_error)}"
                print(error_msg)
                errors.append(error_msg)
                continue
    
    except Exception as e:
        error_msg = f"Lambda execution error: {str(e)}"
        print(error_msg)
        errors.append(error_msg)
    
    # Return response
    response = {
        'statusCode': 200 if not errors else 207,
        'body': json.dumps({
            'message': f'Processed {processed_count} files',
            'processed_count': processed_count,
            'errors': errors,
            'timestamp': datetime.now().isoformat()
        })
    }
    
    print(f"Lambda response: {json.dumps(response)}")
    return response
'''
    return lambda_code

def create_lambda_function(run_data, account_id):
    """Create Lambda function in target region"""
    print("Creating Lambda function...")
    
    # Wait for IAM role to propagate (AWS eventual consistency)
    print("Waiting for IAM role to propagate...")
    time.sleep(10)
    
    target_region = run_data['regions']['target_region']
    lambda_client = boto3.client('lambda', region_name=target_region)
    
    function_name = run_data['resources']['lambda']['function_name']
    role_arn = run_data['resources']['iam']['role_arn']
    
    try:
        # Create Lambda deployment package
        lambda_code = create_lambda_function_code()
        
        # Create a temporary zip file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            with zipfile.ZipFile(temp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr('lambda_function.py', lambda_code)
            
            # Read the zip file content
            with open(temp_zip.name, 'rb') as zip_file:
                zip_content = zip_file.read()
        
        # Clean up temp file
        os.unlink(temp_zip.name)
        
        # Create Lambda function
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.9',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Description='Cross-region S3 migration function triggered by SQS',
            Timeout=300,  # 5 minutes
            MemorySize=256,
            Environment={
                'Variables': {
                    'SOURCE_REGION': run_data['regions']['source_region'],
                    'TARGET_REGION': run_data['regions']['target_region'],
                    'TARGET_BUCKET': run_data['resources']['s3']['target_bucket']['name']
                }
            }
        )
        
        function_arn = response['FunctionArn']
        print(f"Created Lambda function: {function_arn}")
        
        # Update run_data with actual ARN
        run_data['resources']['lambda']['function_arn'] = function_arn
        run_data['deployment_status']['lambda_function'] = 'completed'
        
        return function_arn
        
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Lambda function {function_name} already exists")
        # Get existing function ARN
        response = lambda_client.get_function(FunctionName=function_name)
        function_arn = response['Configuration']['FunctionArn']
        run_data['resources']['lambda']['function_arn'] = function_arn
        run_data['deployment_status']['lambda_function'] = 'completed'
        return function_arn
    except Exception as e:
        print(f"Error creating Lambda function: {str(e)}")
        run_data['deployment_status']['lambda_function'] = 'failed'
        raise

def setup_sqs_lambda_trigger(run_data, function_arn, queue_arn):
    """Set up SQS as trigger for Lambda function"""
    print("Setting up SQS -> Lambda trigger...")
    
    target_region = run_data['regions']['target_region']
    lambda_client = boto3.client('lambda', region_name=target_region)
    function_name = run_data['resources']['lambda']['function_name']
    
    try:
        # Create event source mapping
        response = lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            BatchSize=10,  # Process up to 10 messages at once
            MaximumBatchingWindowInSeconds=5  # Wait up to 5 seconds to batch messages
        )
        
        mapping_uuid = response['UUID']
        print(f"Created SQS -> Lambda trigger: {mapping_uuid}")
        
        return mapping_uuid
        
    except lambda_client.exceptions.ResourceConflictException as e:
        print(f"SQS -> Lambda trigger already exists")
        # Get existing event source mappings
        mappings = lambda_client.list_event_source_mappings(FunctionName=function_name)
        for mapping in mappings['EventSourceMappings']:
            if mapping['EventSourceArn'] == queue_arn:
                print(f"Using existing mapping: {mapping['UUID']}")
                return mapping['UUID']
        raise e
    except Exception as e:
        print(f"Error setting up SQS -> Lambda trigger: {str(e)}")
        raise

def main():
    """Main function to create SNS, SQS, and Lambda resources"""
    print("=== SNS, SQS, and Lambda Creation ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Load configuration
    run_data = load_run_data()
    if not run_data:
        return False
    
    try:
        # Get AWS account ID
        account_id = get_account_id()
        print(f"AWS Account ID: {account_id}")
        
        # Create SNS topic
        topic_arn = create_sns_topic(run_data, account_id)
        
        # Create SQS queue
        queue_url, queue_arn = create_sqs_queue(run_data, account_id)
        
        # Set up SNS -> SQS subscription
        subscription_arn = setup_sns_sqs_subscription(run_data, topic_arn, queue_arn)
        
        # Create Lambda function
        function_arn = create_lambda_function(run_data, account_id)
        
        # Set up SQS -> Lambda trigger
        mapping_uuid = setup_sqs_lambda_trigger(run_data, function_arn, queue_arn)
        
        # Update timestamp
        run_data['last_run'] = datetime.now().isoformat()
        
        # Save updated configuration
        save_run_data(run_data)
        
        print("\n=== Summary ===")
        print(f"SNS Topic: {topic_arn}")
        print(f"SQS Queue: {queue_arn}")
        print(f"Lambda Function: {function_arn}")
        print(f"SNS Subscription: {subscription_arn}")
        print(f"Lambda Trigger: {mapping_uuid}")
        print("\nSNS, SQS, and Lambda setup completed successfully!")
        
        return True
        
    except Exception as e:
        print(f"\nError during setup: {str(e)}")
        # Update timestamp even on failure
        run_data['last_run'] = datetime.now().isoformat()
        save_run_data(run_data)
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)