#!/usr/bin/env python3
"""
Unwind Cross-Region S3 Migration Pipeline
Deletes and decommissions all AWS resources created by the pipeline
Uses run_data.json to identify resources to delete
"""

import boto3
import json
import sys
import time
from datetime import datetime
from botocore.exceptions import ClientError

def load_run_data():
    """Load run_data.json configuration"""
    try:
        with open('run_data.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ run_data.json not found. No resources to delete.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("❌ Invalid JSON in run_data.json")
        sys.exit(1)

def delete_s3_objects_and_buckets(run_data):
    """Delete S3 objects and buckets"""
    print("=== Deleting S3 Buckets and Objects ===")
    
    # Delete source bucket
    if 'source_bucket' in run_data['resources']['s3']:
        source_bucket = run_data['resources']['s3']['source_bucket']['name']
        source_region = run_data['regions']['source_region']
        
        try:
            s3 = boto3.client('s3', region_name=source_region)
            
            # Delete all objects and versions first
            print(f"Deleting objects from source bucket: {source_bucket}")
            
            # Delete all object versions
            paginator = s3.get_paginator('list_object_versions')
            for page in paginator.paginate(Bucket=source_bucket):
                objects_to_delete = []
                
                # Add current versions
                if 'Versions' in page:
                    for version in page['Versions']:
                        objects_to_delete.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                
                # Add delete markers
                if 'DeleteMarkers' in page:
                    for marker in page['DeleteMarkers']:
                        objects_to_delete.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                
                if objects_to_delete:
                    s3.delete_objects(Bucket=source_bucket, Delete={'Objects': objects_to_delete})
                    print(f"  Deleted {len(objects_to_delete)} object versions/markers")
            
            # Delete bucket
            s3.delete_bucket(Bucket=source_bucket)
            print(f"✅ Deleted source bucket: {source_bucket}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"⚠️  Source bucket {source_bucket} does not exist")
            else:
                print(f"❌ Error deleting source bucket: {str(e)}")
    
    # Delete target bucket
    if 'target_bucket' in run_data['resources']['s3']:
        target_bucket = run_data['resources']['s3']['target_bucket']['name']
        target_region = run_data['regions']['target_region']
        
        try:
            s3 = boto3.client('s3', region_name=target_region)
            
            # Delete all objects and versions first
            print(f"Deleting objects from target bucket: {target_bucket}")
            
            # Delete all object versions
            paginator = s3.get_paginator('list_object_versions')
            for page in paginator.paginate(Bucket=target_bucket):
                objects_to_delete = []
                
                # Add current versions
                if 'Versions' in page:
                    for version in page['Versions']:
                        objects_to_delete.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                
                # Add delete markers
                if 'DeleteMarkers' in page:
                    for marker in page['DeleteMarkers']:
                        objects_to_delete.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                
                if objects_to_delete:
                    s3.delete_objects(Bucket=target_bucket, Delete={'Objects': objects_to_delete})
                    print(f"  Deleted {len(objects_to_delete)} object versions/markers")
            
            # Delete bucket
            s3.delete_bucket(Bucket=target_bucket)
            print(f"✅ Deleted target bucket: {target_bucket}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"⚠️  Target bucket {target_bucket} does not exist")
            else:
                print(f"❌ Error deleting target bucket: {str(e)}")
    
    # Delete Athena query results bucket
    if 'athena' in run_data['resources'] and 'query_results_bucket' in run_data['resources']['athena']:
        athena_bucket = run_data['resources']['athena']['query_results_bucket']
        target_region = run_data['regions']['target_region']
        
        try:
            s3 = boto3.client('s3', region_name=target_region)
            
            # Delete all objects and versions first
            print(f"Deleting objects from Athena results bucket: {athena_bucket}")
            
            # Delete all object versions
            paginator = s3.get_paginator('list_object_versions')
            for page in paginator.paginate(Bucket=athena_bucket):
                objects_to_delete = []
                
                # Add current versions
                if 'Versions' in page:
                    for version in page['Versions']:
                        objects_to_delete.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                
                # Add delete markers
                if 'DeleteMarkers' in page:
                    for marker in page['DeleteMarkers']:
                        objects_to_delete.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                
                if objects_to_delete:
                    s3.delete_objects(Bucket=athena_bucket, Delete={'Objects': objects_to_delete})
                    print(f"  Deleted {len(objects_to_delete)} object versions/markers")
            
            # Delete bucket
            s3.delete_bucket(Bucket=athena_bucket)
            print(f"✅ Deleted Athena results bucket: {athena_bucket}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"⚠️  Athena bucket {athena_bucket} does not exist")
            else:
                print(f"❌ Error deleting Athena bucket: {str(e)}")

def delete_lambda_function(run_data):
    """Delete Lambda function"""
    print("=== Deleting Lambda Function ===")
    
    if 'lambda' not in run_data['resources']:
        print("⚠️  No Lambda function to delete")
        return
    
    function_name = run_data['resources']['lambda']['function_name']
    target_region = run_data['regions']['target_region']
    
    try:
        lambda_client = boto3.client('lambda', region_name=target_region)
        lambda_client.delete_function(FunctionName=function_name)
        print(f"✅ Deleted Lambda function: {function_name}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"⚠️  Lambda function {function_name} does not exist")
        else:
            print(f"❌ Error deleting Lambda function: {str(e)}")

def delete_sqs_queue(run_data):
    """Delete SQS queue"""
    print("=== Deleting SQS Queue ===")
    
    if 'sqs' not in run_data['resources']:
        print("⚠️  No SQS queue to delete")
        return
    
    queue_url = run_data['resources']['sqs']['queue_url']
    target_region = run_data['regions']['target_region']
    
    try:
        sqs = boto3.client('sqs', region_name=target_region)
        sqs.delete_queue(QueueUrl=queue_url)
        print(f"✅ Deleted SQS queue: {queue_url}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            print(f"⚠️  SQS queue does not exist")
        else:
            print(f"❌ Error deleting SQS queue: {str(e)}")

def delete_sns_topic(run_data):
    """Delete SNS topic"""
    print("=== Deleting SNS Topic ===")
    
    if 'sns' not in run_data['resources']:
        print("⚠️  No SNS topic to delete")
        return
    
    topic_arn = run_data['resources']['sns']['topic_arn']
    source_region = run_data['regions']['source_region']
    
    try:
        sns = boto3.client('sns', region_name=source_region)
        sns.delete_topic(TopicArn=topic_arn)
        print(f"✅ Deleted SNS topic: {topic_arn}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NotFound':
            print(f"⚠️  SNS topic does not exist")
        else:
            print(f"❌ Error deleting SNS topic: {str(e)}")

def delete_glue_resources(run_data):
    """Delete Glue crawler, database, and classifier"""
    print("=== Deleting Glue Resources ===")
    
    if 'glue' not in run_data['resources']:
        print("⚠️  No Glue resources to delete")
        return
    
    target_region = run_data['regions']['target_region']
    glue = boto3.client('glue', region_name=target_region)
    
    # Delete crawler
    if 'crawler_name' in run_data['resources']['glue']:
        crawler_name = run_data['resources']['glue']['crawler_name']
        try:
            # Stop crawler if running
            try:
                response = glue.get_crawler(Name=crawler_name)
                if response['Crawler']['State'] == 'RUNNING':
                    print(f"Stopping crawler: {crawler_name}")
                    glue.stop_crawler(Name=crawler_name)
                    
                    # Wait for crawler to stop
                    while True:
                        response = glue.get_crawler(Name=crawler_name)
                        if response['Crawler']['State'] == 'READY':
                            break
                        print("⏳ Waiting for crawler to stop...")
                        time.sleep(5)
            except ClientError:
                pass  # Crawler might not exist
            
            glue.delete_crawler(Name=crawler_name)
            print(f"✅ Deleted Glue crawler: {crawler_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(f"⚠️  Glue crawler {crawler_name} does not exist")
            else:
                print(f"❌ Error deleting Glue crawler: {str(e)}")
    
    # Delete classifier
    if 'classifier_name' in run_data['resources']['glue']:
        classifier_name = run_data['resources']['glue']['classifier_name']
        try:
            glue.delete_classifier(Name=classifier_name)
            print(f"✅ Deleted Glue classifier: {classifier_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(f"⚠️  Glue classifier {classifier_name} does not exist")
            else:
                print(f"❌ Error deleting Glue classifier: {str(e)}")
    
    # Delete database (this will also delete tables)
    if 'database_name' in run_data['resources']['glue']:
        database_name = run_data['resources']['glue']['database_name']
        try:
            glue.delete_database(Name=database_name)
            print(f"✅ Deleted Glue database: {database_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(f"⚠️  Glue database {database_name} does not exist")
            else:
                print(f"❌ Error deleting Glue database: {str(e)}")

def delete_iam_role(run_data):
    """Delete IAM role and attached policies"""
    print("=== Deleting IAM Role ===")
    
    if 'iam' not in run_data['resources']:
        print("⚠️  No IAM role to delete")
        return
    
    role_name = run_data['resources']['iam']['role_name']
    
    try:
        iam = boto3.client('iam')
        
        # Detach all attached policies
        try:
            response = iam.list_attached_role_policies(RoleName=role_name)
            for policy in response['AttachedPolicies']:
                iam.detach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy['PolicyArn']
                )
                print(f"  Detached policy: {policy['PolicyName']}")
        except ClientError:
            pass
        
        # Delete inline policies
        try:
            response = iam.list_role_policies(RoleName=role_name)
            for policy_name in response['PolicyNames']:
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
                print(f"  Deleted inline policy: {policy_name}")
        except ClientError:
            pass
        
        # Delete the role
        iam.delete_role(RoleName=role_name)
        print(f"✅ Deleted IAM role: {role_name}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"⚠️  IAM role {role_name} does not exist")
        else:
            print(f"❌ Error deleting IAM role: {str(e)}")

def cleanup_local_files():
    """Clean up local generated files"""
    print("=== Cleaning Up Local Files ===")
    
    import os
    import shutil
    
    files_to_delete = [
        'lambda_function.zip',
        'athena_queries'
    ]
    
    for item in files_to_delete:
        try:
            if os.path.isfile(item):
                os.remove(item)
                print(f"✅ Deleted file: {item}")
            elif os.path.isdir(item):
                shutil.rmtree(item)
                print(f"✅ Deleted directory: {item}")
        except Exception as e:
            print(f"❌ Error deleting {item}: {str(e)}")

def update_run_data_status(run_data):
    """Update run_data.json to reflect deleted resources"""
    print("=== Updating run_data.json ===")
    
    # Reset deployment status
    for key in run_data['deployment_status']:
        run_data['deployment_status'][key] = 'deleted'
    
    # Add deletion timestamp
    run_data['deletion_timestamp'] = datetime.now().isoformat()
    run_data['status'] = 'decommissioned'
    
    try:
        with open('run_data.json', 'w') as f:
            json.dump(run_data, f, indent=2)
        print("✅ Updated run_data.json with deletion status")
    except Exception as e:
        print(f"❌ Error updating run_data.json: {str(e)}")

def main():
    """Main unwind function"""
    print("🗑️  Cross-Region S3 Migration Pipeline Unwind")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Confirm deletion
    print("⚠️  WARNING: This will delete ALL AWS resources created by the pipeline!")
    print("This action cannot be undone.")
    
    confirm = input("\nType 'DELETE' to confirm resource deletion: ")
    if confirm != 'DELETE':
        print("❌ Deletion cancelled. Exiting.")
        sys.exit(0)
    
    try:
        # Load configuration
        run_data = load_run_data()
        
        print(f"\nAccount ID: {run_data.get('account_id', 'Unknown')}")
        print(f"Source Region: {run_data['regions']['source_region']}")
        print(f"Target Region: {run_data['regions']['target_region']}")
        print("\nStarting resource deletion...\n")
        
        # Delete resources in reverse order of creation
        deletion_steps = [
            ("S3 Buckets and Objects", lambda: delete_s3_objects_and_buckets(run_data)),
            ("Lambda Function", lambda: delete_lambda_function(run_data)),
            ("SQS Queue", lambda: delete_sqs_queue(run_data)),
            ("SNS Topic", lambda: delete_sns_topic(run_data)),
            ("Glue Resources", lambda: delete_glue_resources(run_data)),
            ("IAM Role", lambda: delete_iam_role(run_data)),
            ("Local Files", lambda: cleanup_local_files()),
            ("Update Status", lambda: update_run_data_status(run_data))
        ]
        
        for step_name, step_func in deletion_steps:
            print(f"\n{'=' * 60}")
            try:
                step_func()
            except Exception as e:
                print(f"❌ Error in {step_name}: {str(e)}")
                continue
        
        print(f"\n{'=' * 60}")
        print("🎉 UNWIND COMPLETED")
        print(f"{'=' * 60}")
        print("All AWS resources have been deleted.")
        print("The cross-region S3 migration pipeline has been decommissioned.")
        print(f"\nCompleted at: {datetime.now().isoformat()}")
        
    except Exception as e:
        print(f"\n❌ Unwind error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()