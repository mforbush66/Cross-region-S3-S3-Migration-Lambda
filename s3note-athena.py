#!/usr/bin/env python3
"""
S3 Notifications and Athena Setup Script
Configures S3 event notifications and Athena query environment
"""

import boto3
import json
import sys
from datetime import datetime
from botocore.exceptions import ClientError

def load_run_data():
    """Load run_data.json configuration"""
    try:
        with open('run_data.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ run_data.json not found. Please run deploy.py first.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("❌ Invalid JSON in run_data.json")
        sys.exit(1)

def save_run_data(run_data):
    """Save updated run_data.json"""
    run_data['last_run'] = datetime.now().isoformat()
    with open('run_data.json', 'w') as f:
        json.dump(run_data, f, indent=2)
    print("Updated run_data.json")

def get_account_id():
    """Get AWS account ID"""
    sts = boto3.client('sts')
    return sts.get_caller_identity()['Account']

def setup_s3_notifications(run_data, account_id):
    """Setup S3 event notifications to SNS"""
    print("Setting up S3 event notifications...")
    
    source_region = run_data['regions']['source_region']
    s3 = boto3.client('s3', region_name=source_region)
    
    # Get source bucket and SNS topic ARN
    source_bucket = run_data['resources']['s3']['source_bucket']['name']
    sns_topic_arn = run_data['resources']['sns']['topic_arn']
    
    try:
        # Get current notification configuration
        try:
            current_config = s3.get_bucket_notification_configuration(Bucket=source_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchConfiguration':
                current_config = {}
            else:
                raise
        
        # Prepare notification configuration
        notification_config = {
            'TopicConfigurations': [
                {
                    'Id': 's3-to-sns-notification',
                    'TopicArn': sns_topic_arn,
                    'Events': [
                        's3:ObjectCreated:*'
                    ],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'suffix',
                                    'Value': '.csv'
                                }
                            ]
                        }
                    }
                }
            ]
        }
        
        # Preserve existing configurations if any
        if 'QueueConfigurations' in current_config:
            notification_config['QueueConfigurations'] = current_config['QueueConfigurations']
        if 'LambdaConfigurations' in current_config:
            notification_config['LambdaConfigurations'] = current_config['LambdaConfigurations']
        
        # Apply notification configuration
        s3.put_bucket_notification_configuration(
            Bucket=source_bucket,
            NotificationConfiguration=notification_config
        )
        
        print(f"Configured S3 notifications for bucket: {source_bucket}")
        print(f"Events: s3:ObjectCreated:* -> {sns_topic_arn}")
        print("Filter: .csv files only")
        
        # Update deployment status
        run_data['deployment_status']['s3_notifications'] = 'completed'
        
        return True
        
    except Exception as e:
        print(f"Error setting up S3 notifications: {str(e)}")
        raise

def create_athena_query_result_bucket(run_data, account_id):
    """Create S3 bucket for Athena query results if it doesn't exist"""
    print("Setting up Athena query results bucket...")
    
    target_region = run_data['regions']['target_region']
    s3 = boto3.client('s3', region_name=target_region)
    
    # Generate bucket name for Athena query results
    athena_bucket_name = f"aws-athena-query-results-{account_id}-{target_region}"
    
    try:
        # Check if bucket already exists
        try:
            s3.head_bucket(Bucket=athena_bucket_name)
            print(f"Athena query results bucket already exists: {athena_bucket_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Create bucket
                if target_region == 'us-east-1':
                    s3.create_bucket(Bucket=athena_bucket_name)
                else:
                    s3.create_bucket(
                        Bucket=athena_bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': target_region}
                    )
                
                # Enable versioning
                s3.put_bucket_versioning(
                    Bucket=athena_bucket_name,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
                
                # Enable encryption
                s3.put_bucket_encryption(
                    Bucket=athena_bucket_name,
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
                
                print(f"Created Athena query results bucket: {athena_bucket_name}")
            else:
                raise
        
        # Update run_data with bucket location
        athena_s3_location = f"s3://{athena_bucket_name}/"
        run_data['resources']['athena']['query_result_location'] = athena_s3_location
        
        return athena_s3_location
        
    except Exception as e:
        print(f"Error creating Athena query results bucket: {str(e)}")
        raise

def setup_athena_workgroup(run_data, account_id, query_result_location):
    """Setup Athena workgroup configuration"""
    print("Setting up Athena workgroup...")
    
    target_region = run_data['regions']['target_region']
    athena = boto3.client('athena', region_name=target_region)
    
    workgroup_name = run_data['resources']['athena']['workgroup']
    database_name = run_data['resources']['athena']['database']
    
    try:
        # Check if workgroup exists and get its configuration
        try:
            response = athena.get_work_group(WorkGroup=workgroup_name)
            current_config = response['WorkGroup']['Configuration']
            
            # Update workgroup configuration if needed
            updated_config = {
                'ResultConfigurationUpdates': {
                    'OutputLocation': query_result_location,
                    'EncryptionConfiguration': {
                        'EncryptionOption': 'SSE_S3'
                    }
                },
                'EnforceWorkGroupConfiguration': False,
                'PublishCloudWatchMetricsEnabled': True
            }
            
            # Only update if configuration is different
            if (current_config.get('ResultConfiguration', {}).get('OutputLocation') != query_result_location):
                athena.update_work_group(
                    WorkGroup=workgroup_name,
                    ConfigurationUpdates=updated_config
                )
                print(f"Updated Athena workgroup '{workgroup_name}' configuration")
            else:
                print(f"Athena workgroup '{workgroup_name}' already properly configured")
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidRequestException':
                # Workgroup doesn't exist, create it
                athena.create_work_group(
                    Name=workgroup_name,
                    Description='Workgroup for S3 cross-region migration queries',
                    Configuration={
                        'ResultConfiguration': {
                            'OutputLocation': query_result_location,
                            'EncryptionConfiguration': {
                                'EncryptionOption': 'SSE_S3'
                            }
                        },
                        'EnforceWorkGroupConfiguration': False,
                        'PublishCloudWatchMetrics': True
                    }
                )
                print(f"Created Athena workgroup: {workgroup_name}")
            else:
                raise
        
        print(f"Query result location: {query_result_location}")
        print(f"Default database: {database_name}")
        
        # Update deployment status
        run_data['deployment_status']['athena_setup'] = 'completed'
        
        return workgroup_name
        
    except Exception as e:
        print(f"Error setting up Athena workgroup: {str(e)}")
        raise

def create_sample_athena_queries(run_data):
    """Create sample Athena queries for testing"""
    print("Creating sample Athena queries...")
    
    database_name = run_data['resources']['athena']['database']
    
    sample_queries = {
        'list_tables.sql': f"""
-- List all tables in the migration database
SHOW TABLES IN {database_name};
""",
        'describe_table.sql': f"""
-- Describe table structure (replace 'table_name' with actual table name)
DESCRIBE {database_name}.s3_migration_table_name;
""",
        'sample_query.sql': f"""
-- Sample query to select data from migrated files
-- Replace 'table_name' with the actual table name created by Glue crawler
SELECT *
FROM {database_name}.s3_migration_table_name
LIMIT 10;
""",
        'count_records.sql': f"""
-- Count total records in migrated data
SELECT COUNT(*) as total_records
FROM {database_name}.s3_migration_table_name;
"""
    }
    
    try:
        # Create queries directory if it doesn't exist
        import os
        queries_dir = 'athena_queries'
        os.makedirs(queries_dir, exist_ok=True)
        
        # Write sample queries to files
        for filename, query in sample_queries.items():
            filepath = os.path.join(queries_dir, filename)
            with open(filepath, 'w') as f:
                f.write(query)
        
        print(f"Created sample Athena queries in '{queries_dir}/' directory")
        print("Available queries:")
        for filename in sample_queries.keys():
            print(f"  - {filename}")
        
        return queries_dir
        
    except Exception as e:
        print(f"Error creating sample queries: {str(e)}")
        # Don't raise - this is not critical
        return None

def main():
    """Main function"""
    print("=== S3 Notifications and Athena Setup ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        # Load configuration
        run_data = load_run_data()
        account_id = get_account_id()
        print(f"AWS Account ID: {account_id}")
        
        # Setup S3 event notifications
        setup_s3_notifications(run_data, account_id)
        
        # Create Athena query results bucket
        query_result_location = create_athena_query_result_bucket(run_data, account_id)
        
        # Setup Athena workgroup
        workgroup_name = setup_athena_workgroup(run_data, account_id, query_result_location)
        
        # Create sample Athena queries
        queries_dir = create_sample_athena_queries(run_data)
        
        # Save updated configuration
        save_run_data(run_data)
        
        print("\n=== Summary ===")
        print(f"S3 Notifications: Configured for {run_data['resources']['s3']['source_bucket']['name']}")
        print(f"SNS Topic: {run_data['resources']['sns']['topic_arn']}")
        print(f"Athena Workgroup: {workgroup_name}")
        print(f"Query Results: {query_result_location}")
        print(f"Database: {run_data['resources']['athena']['database']}")
        if queries_dir:
            print(f"Sample Queries: {queries_dir}/")
        
        print("\n✅ Next Steps:")
        print("1. Upload a CSV file to the source S3 bucket to test the pipeline")
        print("2. Run the Glue crawler to catalog the migrated data")
        print("3. Use Athena to query the cataloged data")
        print("\nS3 notifications and Athena setup completed successfully!")
        
    except Exception as e:
        print(f"\nError during setup: {str(e)}")
        # Update deployment status to failed
        try:
            run_data = load_run_data()
            if 'deployment_status' in run_data:
                if 's3_notifications' not in run_data['deployment_status'] or run_data['deployment_status']['s3_notifications'] != 'completed':
                    run_data['deployment_status']['s3_notifications'] = 'failed'
                if 'athena_setup' not in run_data['deployment_status'] or run_data['deployment_status']['athena_setup'] != 'completed':
                    run_data['deployment_status']['athena_setup'] = 'failed'
            save_run_data(run_data)
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()