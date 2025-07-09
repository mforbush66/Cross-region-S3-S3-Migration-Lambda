#!/usr/bin/env python3
"""
Main Deployment Orchestrator for Cross-Region S3 Migration
Coordinates the creation of all AWS resources in the correct order:
1. IAM Role and S3 Buckets (iam-s3.py)
2. SNS Topic (future)
3. SQS Queue (future)
4. Lambda Function (future)
5. Glue Crawler (future)
6. S3 Notifications (future)
"""

import subprocess
import sys
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

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("Checking prerequisites...")
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("Error: .env file not found")
        return False
    
    # Check if run_data.json exists
    if not os.path.exists('run_data.json'):
        print("Error: run_data.json not found")
        return False
    
    # Check if required Python packages are available
    try:
        import boto3
        import dotenv
        print("✓ Required packages available")
    except ImportError as e:
        print(f"Error: Missing required package: {e}")
        return False
    
    # Test AWS credentials
    try:
        import boto3
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✓ AWS credentials valid - Account: {identity['Account']}")
    except Exception as e:
        print(f"Error: AWS credentials issue: {e}")
        return False
    
    print("All prerequisites met!")
    return True

def run_component_script(script_name, description):
    """Run a component creation script"""
    print(f"\n{'='*50}")
    print(f"Running: {script_name}")
    print(f"Description: {description}")
    print(f"{'='*50}")
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print(f"Errors/Warnings: {result.stderr}")
        
        # Check return code
        if result.returncode == 0:
            print(f"✓ {script_name} completed successfully")
            return True
        else:
            print(f"✗ {script_name} failed with return code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"✗ Error running {script_name}: {str(e)}")
        return False

def display_deployment_status(run_data):
    """Display current deployment status"""
    print("\n" + "="*60)
    print("DEPLOYMENT STATUS")
    print("="*60)
    
    status = run_data.get('deployment_status', {})
    
    components = [
        ('iam_role', 'IAM Role Creation'),
        ('s3_buckets', 'S3 Buckets Creation'),
        ('sns_topic', 'SNS Topic Creation'),
        ('sqs_queue', 'SQS Queue Creation'),
        ('lambda_function', 'Lambda Function Creation'),
        ('glue_crawler', 'Glue Crawler Creation'),
        ('s3_notifications', 'S3 Notifications Setup'),
        ('athena_setup', 'Athena Setup')
    ]
    
    for key, description in components:
        status_value = status.get(key, 'pending')
        if status_value == 'completed':
            icon = '✓'
            color = '\033[92m'  # Green
        elif status_value == 'failed':
            icon = '✗'
            color = '\033[91m'  # Red
        else:
            icon = '○'
            color = '\033[93m'  # Yellow
        
        reset_color = '\033[0m'
        print(f"{color}{icon} {description}: {status_value.upper()}{reset_color}")
    
    print("="*60)

def main():
    """Main deployment orchestrator"""
    print("🚀 Cross-Region S3 Migration Deployment Orchestrator")
    print(f"Started at: {datetime.now().isoformat()}")
    print("="*60)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please fix the issues above.")
        return False
    
    # Load run data
    run_data = load_run_data()
    if not run_data:
        return False
    
    # Display current status
    display_deployment_status(run_data)
    
    # Deployment steps
    deployment_steps = [
        {
            'script': 'iam-s3.py',
            'description': 'Create IAM Role and S3 Buckets',
            'status_key': ['iam_role', 's3_buckets']
        },
        {
            'script': 'sns-sqs-lamda.py',
            'description': 'Create SNS Topic, SQS Queue, and Lambda Function',
            'status_key': ['sns_topic', 'sqs_queue', 'lambda_function']
        },
        {
            'script': 'glue.py',
            'description': 'Create Glue Database and Crawler',
            'status_key': ['glue_crawler']
        },
        {
            'script': 's3note-athena.py',
            'description': 'Setup S3 Notifications and Athena',
            'status_key': ['s3_notifications', 'athena_setup']
        }
    ]
    
    # Execute deployment steps
    all_success = True
    
    for step in deployment_steps:
        # Check if this step is already completed
        status_keys = step['status_key']
        already_completed = all(
            run_data.get('deployment_status', {}).get(key) == 'completed'
            for key in status_keys
        )
        
        if already_completed:
            print(f"\n⏭️  Skipping {step['script']} - already completed")
            continue
        
        # Run the component script
        success = run_component_script(step['script'], step['description'])
        
        if not success:
            print(f"\n❌ Deployment failed at step: {step['script']}")
            all_success = False
            break
        
        # Reload run_data to get updated status
        run_data = load_run_data()
        if not run_data:
            all_success = False
            break
    
    # Final status
    print("\n" + "="*60)
    if all_success:
        print("🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!")
        
        # Display final resource summary
        print("\n📋 CREATED RESOURCES:")
        resources = run_data.get('resources', {})
        
        if 'iam' in resources:
            print(f"IAM Role: {resources['iam'].get('role_arn', 'N/A')}")
        
        if 's3' in resources:
            s3_resources = resources['s3']
            if 'source_bucket' in s3_resources:
                print(f"Source Bucket: {s3_resources['source_bucket'].get('name', 'N/A')} (us-west-1)")
            if 'target_bucket' in s3_resources:
                print(f"Target Bucket: {s3_resources['target_bucket'].get('name', 'N/A')} (us-east-1)")
        
    else:
        print("❌ DEPLOYMENT FAILED")
        print("Check the error messages above and fix any issues.")
    
    # Update final timestamp
    run_data['last_run'] = datetime.now().isoformat()
    save_run_data(run_data)
    
    # Display final status
    display_deployment_status(run_data)
    
    print("="*60)
    print(f"Completed at: {datetime.now().isoformat()}")
    
    return all_success

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)