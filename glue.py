#!/usr/bin/env python3
"""
Glue Crawler Creation Script
Creates AWS Glue database, classifier, and crawler for S3 data cataloging
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

def create_glue_database(run_data, account_id):
    """Create Glue database"""
    print("Creating Glue database...")
    
    target_region = run_data['regions']['target_region']
    glue = boto3.client('glue', region_name=target_region)
    database_name = run_data['resources']['glue']['database_name']
    
    try:
        # Check if database already exists
        try:
            glue.get_database(Name=database_name)
            print(f"Database '{database_name}' already exists")
            return database_name
        except ClientError as e:
            if e.response['Error']['Code'] != 'EntityNotFoundException':
                raise
        
        # Create database
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for S3 cross-region migration data catalog'
            }
        )
        
        print(f"Created Glue database: {database_name}")
        return database_name
        
    except Exception as e:
        print(f"Error creating Glue database: {str(e)}")
        raise

def create_csv_classifier(run_data):
    """Create CSV classifier for Glue crawler"""
    print("Creating CSV classifier...")
    
    target_region = run_data['regions']['target_region']
    glue = boto3.client('glue', region_name=target_region)
    classifier_name = 's3-migration-csv-classifier'
    
    try:
        # Check if classifier already exists
        try:
            glue.get_classifier(Name=classifier_name)
            print(f"CSV classifier '{classifier_name}' already exists")
            return classifier_name
        except ClientError as e:
            if e.response['Error']['Code'] != 'EntityNotFoundException':
                raise
        
        # Create CSV classifier
        glue.create_classifier(
            CsvClassifier={
                'Name': classifier_name,
                'Delimiter': ',',
                'QuoteSymbol': '"',
                'ContainsHeader': 'PRESENT',
                'Header': [],  # Will be auto-detected
                'DisableValueTrimming': False,
                'AllowSingleColumn': False
            }
        )
        
        print(f"Created CSV classifier: {classifier_name}")
        return classifier_name
        
    except Exception as e:
        print(f"Error creating CSV classifier: {str(e)}")
        raise

def create_glue_crawler(run_data, account_id, database_name, classifier_name):
    """Create Glue crawler"""
    print("Creating Glue crawler...")
    
    target_region = run_data['regions']['target_region']
    glue = boto3.client('glue', region_name=target_region)
    crawler_name = run_data['resources']['glue']['crawler_name']
    
    # Get target bucket name
    target_bucket = run_data['resources']['s3']['target_bucket']['name']
    target_path = f"s3://{target_bucket}/"
    
    # Get IAM role ARN
    role_arn = run_data['resources']['iam']['role_arn']
    
    try:
        # Check if crawler already exists
        try:
            response = glue.get_crawler(Name=crawler_name)
            print(f"Crawler '{crawler_name}' already exists")
            crawler_arn = f"arn:aws:glue:{target_region}:{account_id}:crawler/{crawler_name}"
            
            # Update run_data with actual values
            run_data['resources']['glue']['crawler_arn'] = crawler_arn
            run_data['resources']['glue']['target_path'] = target_path
            run_data['deployment_status']['glue_crawler'] = 'completed'
            
            return crawler_arn
        except ClientError as e:
            if e.response['Error']['Code'] != 'EntityNotFoundException':
                raise
        
        # Create crawler
        glue.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Description='Crawler for S3 cross-region migration data',
            Targets={
                'S3Targets': [
                    {
                        'Path': target_path,
                        'Exclusions': []
                    }
                ]
            },
            Classifiers=[classifier_name],
            TablePrefix='s3_migration_',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            LineageConfiguration={
                'CrawlerLineageSettings': 'DISABLE'
            },
            Configuration=json.dumps({
                'Version': 1.0,
                'CrawlerOutput': {
                    'Partitions': {'AddOrUpdateBehavior': 'InheritFromTable'},
                    'Tables': {'AddOrUpdateBehavior': 'MergeNewColumns'}
                }
            })
        )
        
        crawler_arn = f"arn:aws:glue:{target_region}:{account_id}:crawler/{crawler_name}"
        
        print(f"Created Glue crawler: {crawler_arn}")
        print(f"Target path: {target_path}")
        
        # Update run_data with actual values
        run_data['resources']['glue']['crawler_arn'] = crawler_arn
        run_data['resources']['glue']['target_path'] = target_path
        run_data['deployment_status']['glue_crawler'] = 'completed'
        
        return crawler_arn
        
    except Exception as e:
        print(f"Error creating Glue crawler: {str(e)}")
        raise

def main():
    """Main function"""
    print("=== Glue Database and Crawler Creation ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        # Load configuration
        run_data = load_run_data()
        account_id = get_account_id()
        print(f"AWS Account ID: {account_id}")
        
        # Create Glue database
        database_name = create_glue_database(run_data, account_id)
        
        # Create CSV classifier
        classifier_name = create_csv_classifier(run_data)
        
        # Create Glue crawler
        crawler_arn = create_glue_crawler(run_data, account_id, database_name, classifier_name)
        
        # Save updated configuration
        save_run_data(run_data)
        
        print("\n=== Summary ===")
        print(f"Database: {database_name}")
        print(f"Classifier: {classifier_name}")
        print(f"Crawler: {crawler_arn}")
        print(f"Target Path: {run_data['resources']['glue']['target_path']}")
        print("\nGlue setup completed successfully!")
        
    except Exception as e:
        print(f"\nError during setup: {str(e)}")
        # Update deployment status to failed
        try:
            run_data = load_run_data()
            run_data['deployment_status']['glue_crawler'] = 'failed'
            save_run_data(run_data)
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()