#!/usr/bin/env python3
"""
IAM Role and S3 Bucket Creation Script
Creates the foundational resources for cross-region S3 migration:
- IAM role with required policies
- Source S3 bucket in us-west-1
- Target S3 bucket in us-east-1
"""

import boto3
import json
import os
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

def create_iam_role(run_data, account_id):
    """Create IAM role with required policies"""
    print("Creating IAM role...")
    
    iam = boto3.client('iam')
    role_name = run_data['resources']['iam']['role_name']
    
    # Trust policy for Lambda and Glue
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": ["lambda.amazonaws.com", "glue.amazonaws.com"]
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        # Create the role
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Role for cross-region S3 migration with Lambda and Glue'
        )
        
        role_arn = response['Role']['Arn']
        print(f"Created IAM role: {role_arn}")
        
        # Attach required policies
        policies = run_data['resources']['iam']['policies_attached']
        for policy in policies:
            iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=f'arn:aws:iam::aws:policy/{policy}'
            )
            print(f"Attached policy: {policy}")
        
        # Update run_data with actual ARN
        run_data['resources']['iam']['role_arn'] = role_arn
        run_data['deployment_status']['iam_role'] = 'completed'
        
        return role_arn
        
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"IAM role {role_name} already exists")
        # Get existing role ARN
        response = iam.get_role(RoleName=role_name)
        role_arn = response['Role']['Arn']
        run_data['resources']['iam']['role_arn'] = role_arn
        run_data['deployment_status']['iam_role'] = 'completed'
        return role_arn
    except Exception as e:
        print(f"Error creating IAM role: {str(e)}")
        run_data['deployment_status']['iam_role'] = 'failed'
        raise

def create_s3_bucket(bucket_name, region, account_id):
    """Create S3 bucket in specified region"""
    # Replace placeholders in bucket name
    actual_bucket_name = bucket_name.replace('{account-id}', account_id).replace('{region}', region)
    
    print(f"Creating S3 bucket: {actual_bucket_name} in {region}")
    
    try:
        if region == 'us-east-1':
            # us-east-1 doesn't need LocationConstraint
            s3 = boto3.client('s3', region_name=region)
            s3.create_bucket(Bucket=actual_bucket_name)
        else:
            s3 = boto3.client('s3', region_name=region)
            s3.create_bucket(
                Bucket=actual_bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        
        # Enable versioning
        s3.put_bucket_versioning(
            Bucket=actual_bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Enable server-side encryption
        s3.put_bucket_encryption(
            Bucket=actual_bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        }
                    }
                ]
            }
        )
        
        print(f"Successfully created bucket: {actual_bucket_name}")
        print(f"- Versioning: Enabled")
        print(f"- Encryption: AES256")
        
        return actual_bucket_name
        
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket {actual_bucket_name} already exists")
        return actual_bucket_name
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {actual_bucket_name} already owned by you")
        return actual_bucket_name
    except Exception as e:
        print(f"Error creating bucket {actual_bucket_name}: {str(e)}")
        raise

def create_s3_buckets(run_data, account_id):
    """Create both source and target S3 buckets"""
    print("Creating S3 buckets...")
    
    try:
        # Create source bucket
        source_config = run_data['resources']['s3']['source_bucket']
        source_bucket_name = create_s3_bucket(
            source_config['name'],
            source_config['region'],
            account_id
        )
        run_data['resources']['s3']['source_bucket']['name'] = source_bucket_name
        
        # Create target bucket
        target_config = run_data['resources']['s3']['target_bucket']
        target_bucket_name = create_s3_bucket(
            target_config['name'],
            target_config['region'],
            account_id
        )
        run_data['resources']['s3']['target_bucket']['name'] = target_bucket_name
        
        run_data['deployment_status']['s3_buckets'] = 'completed'
        
        return source_bucket_name, target_bucket_name
        
    except Exception as e:
        print(f"Error creating S3 buckets: {str(e)}")
        run_data['deployment_status']['s3_buckets'] = 'failed'
        raise

def main():
    """Main function to create IAM role and S3 buckets"""
    print("=== IAM Role and S3 Bucket Creation ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Load configuration
    run_data = load_run_data()
    if not run_data:
        return False
    
    try:
        # Get AWS account ID
        account_id = get_account_id()
        print(f"AWS Account ID: {account_id}")
        
        # Create IAM role
        role_arn = create_iam_role(run_data, account_id)
        
        # Create S3 buckets
        source_bucket, target_bucket = create_s3_buckets(run_data, account_id)
        
        # Update timestamp
        run_data['last_run'] = datetime.now().isoformat()
        
        # Save updated configuration
        save_run_data(run_data)
        
        print("\n=== Summary ===")
        print(f"IAM Role: {role_arn}")
        print(f"Source Bucket: {source_bucket} (us-west-1)")
        print(f"Target Bucket: {target_bucket} (us-east-1)")
        print("\nIAM and S3 setup completed successfully!")
        
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