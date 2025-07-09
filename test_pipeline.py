#!/usr/bin/env python3
"""
Test Cross-Region S3 Migration Pipeline
Uploads test data and monitors the complete pipeline execution
"""

import boto3
import json
import sys
import time
import os
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

def upload_test_file(run_data):
    """Upload test CSV file to source S3 bucket"""
    print("=== Step 1: Upload Test File ===")
    
    source_region = run_data['regions']['source_region']
    source_bucket = run_data['resources']['s3']['source_bucket']['name']
    test_file_path = 'data/customers.csv'
    
    # Check if test file exists
    if not os.path.exists(test_file_path):
        print(f"❌ Test file not found: {test_file_path}")
        return False
    
    try:
        s3 = boto3.client('s3', region_name=source_region)
        
        # Upload file
        s3.upload_file(test_file_path, source_bucket, 'customers.csv')
        print(f"✅ Uploaded {test_file_path} to s3://{source_bucket}/customers.csv")
        
        return True
        
    except Exception as e:
        print(f"❌ Error uploading file: {str(e)}")
        return False

def check_target_bucket(run_data, max_wait_time=120):
    """Check if file was copied to target bucket"""
    print("=== Step 2: Monitor Cross-Region Copy ===")
    
    target_region = run_data['regions']['target_region']
    target_bucket = run_data['resources']['s3']['target_bucket']['name']
    
    s3 = boto3.client('s3', region_name=target_region)
    
    print(f"Waiting for file to appear in target bucket: {target_bucket}")
    print("This may take 1-2 minutes for the Lambda function to process...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        try:
            # Check if file exists in target bucket
            s3.head_object(Bucket=target_bucket, Key='customers.csv')
            print(f"✅ File successfully copied to s3://{target_bucket}/customers.csv")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # File not found yet, wait and retry
                print("⏳ File not yet copied, waiting 10 seconds...")
                time.sleep(10)
                continue
            else:
                print(f"❌ Error checking target bucket: {str(e)}")
                return False
    
    print(f"❌ File not copied within {max_wait_time} seconds")
    return False

def run_glue_crawler(run_data):
    """Start Glue crawler to catalog the data"""
    print("=== Step 3: Run Glue Crawler ===")
    
    target_region = run_data['regions']['target_region']
    crawler_name = run_data['resources']['glue']['crawler_name']
    
    try:
        glue = boto3.client('glue', region_name=target_region)
        
        # Start crawler
        glue.start_crawler(Name=crawler_name)
        print(f"✅ Started Glue crawler: {crawler_name}")
        
        # Wait for crawler to complete
        print("Waiting for crawler to complete...")
        max_wait = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            response = glue.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            
            if state == 'READY':
                print("✅ Glue crawler completed successfully")
                return True
            elif state in ['RUNNING', 'STOPPING']:
                print(f"⏳ Crawler state: {state}, waiting...")
                time.sleep(15)
            else:
                print(f"❌ Crawler in unexpected state: {state}")
                return False
        
        print(f"❌ Crawler did not complete within {max_wait} seconds")
        return False
        
    except Exception as e:
        print(f"❌ Error running Glue crawler: {str(e)}")
        return False

def check_glue_tables(run_data):
    """Check if Glue catalog tables were created"""
    print("=== Step 4: Verify Glue Catalog ===")
    
    target_region = run_data['regions']['target_region']
    database_name = run_data['resources']['glue']['database_name']
    
    try:
        glue = boto3.client('glue', region_name=target_region)
        
        # Get tables in database
        response = glue.get_tables(DatabaseName=database_name)
        tables = response['TableList']
        
        if tables:
            print(f"✅ Found {len(tables)} table(s) in Glue catalog:")
            for table in tables:
                print(f"  - {table['Name']} ({len(table.get('StorageDescriptor', {}).get('Columns', []))} columns)")
            return True
        else:
            print("❌ No tables found in Glue catalog")
            return False
            
    except Exception as e:
        print(f"❌ Error checking Glue catalog: {str(e)}")
        return False

def test_athena_query(run_data):
    """Test Athena query on cataloged data"""
    print("=== Step 5: Test Athena Query ===")
    
    target_region = run_data['regions']['target_region']
    database_name = run_data['resources']['glue']['database_name']
    workgroup = run_data['resources']['athena']['workgroup']
    
    try:
        athena = boto3.client('athena', region_name=target_region)
        glue = boto3.client('glue', region_name=target_region)
        
        # Get first table name
        response = glue.get_tables(DatabaseName=database_name)
        tables = response['TableList']
        
        if not tables:
            print("❌ No tables available for querying")
            return False
        
        table_name = tables[0]['Name']
        query = f'SELECT * FROM "{database_name}"."{table_name}" LIMIT 5'
        
        print(f"Executing query: {query}")
        
        # Start query execution
        response = athena.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")
        
        # Wait for query to complete
        max_wait = 60  # 1 minute
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                # Get query results
                results = athena.get_query_results(QueryExecutionId=query_execution_id)
                rows = results['ResultSet']['Rows']
                
                if len(rows) > 1:  # First row is header
                    data_rows = len(rows) - 1  # Subtract header row
                    print(f"✅ Athena query successful! Retrieved {data_rows} rows of data")
                    
                    # Display results in clean table format
                    if data_rows > 0:
                        headers = [col['VarCharValue'] for col in rows[0]['Data']]
                        
                        # Calculate column widths
                        col_widths = [len(header) for header in headers]
                        for row in rows[1:]:
                            values = [col.get('VarCharValue', 'NULL') for col in row['Data']]
                            for i, value in enumerate(values):
                                col_widths[i] = max(col_widths[i], len(str(value)))
                        
                        # Print table header
                        print("\nQuery Results:")
                        print("┌" + "┬".join("─" * (width + 2) for width in col_widths) + "┐")
                        print("│" + "│".join(f" {header:<{col_widths[i]}} " for i, header in enumerate(headers)) + "│")
                        print("├" + "┼".join("─" * (width + 2) for width in col_widths) + "┤")
                        
                        # Print data rows
                        for row in rows[1:]:
                            values = [col.get('VarCharValue', 'NULL') for col in row['Data']]
                            print("│" + "│".join(f" {str(value):<{col_widths[i]}} " for i, value in enumerate(values)) + "│")
                        
                        print("└" + "┴".join("─" * (width + 2) for width in col_widths) + "┘")
                    
                    return True
                else:
                    print("❌ No data returned from query")
                    return False
                    
            elif status in ['QUEUED', 'RUNNING']:
                print(f"⏳ Query status: {status}, waiting...")
                time.sleep(5)
            else:
                print(f"❌ Query failed with status: {status}")
                if 'StateChangeReason' in response['QueryExecution']['Status']:
                    print(f"Reason: {response['QueryExecution']['Status']['StateChangeReason']}")
                return False
        
        print(f"❌ Query did not complete within {max_wait} seconds")
        return False
        
    except Exception as e:
        print(f"❌ Error testing Athena query: {str(e)}")
        return False

def check_lambda_logs(run_data):
    """Check Lambda function logs for any errors"""
    print("=== Step 6: Check Lambda Logs ===")
    
    target_region = run_data['regions']['target_region']
    function_name = run_data['resources']['lambda']['function_name']
    
    try:
        logs = boto3.client('logs', region_name=target_region)
        log_group = f"/aws/lambda/{function_name}"
        
        # Get recent log streams
        response = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy='LastEventTime',
            descending=True,
            limit=1
        )
        
        if not response['logStreams']:
            print("❌ No log streams found")
            return False
        
        log_stream = response['logStreams'][0]['logStreamName']
        
        # Get recent log events
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            limit=50
        )
        
        events = response['events']
        if events:
            print(f"✅ Found {len(events)} recent log events")
            print("Recent Lambda execution logs:")
            for event in events[-5:]:  # Show last 5 events
                timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                print(f"  {timestamp}: {event['message'].strip()}")
            return True
        else:
            print("❌ No recent log events found")
            return False
            
    except Exception as e:
        print(f"❌ Error checking Lambda logs: {str(e)}")
        return False

def main():
    """Main test function"""
    print("🧪 Cross-Region S3 Migration Pipeline Test")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    try:
        # Load configuration
        run_data = load_run_data()
        
        # Test steps
        steps = [
            ("Upload test file", lambda: upload_test_file(run_data)),
            ("Check cross-region copy", lambda: check_target_bucket(run_data)),
            ("Run Glue crawler", lambda: run_glue_crawler(run_data)),
            ("Verify Glue catalog", lambda: check_glue_tables(run_data)),
            ("Test Athena query", lambda: test_athena_query(run_data)),
            ("Check Lambda logs", lambda: check_lambda_logs(run_data))
        ]
        
        results = []
        for step_name, step_func in steps:
            print(f"\n{'=' * 60}")
            try:
                success = step_func()
                results.append((step_name, success))
                if not success:
                    print(f"❌ Step failed: {step_name}")
                    break
            except Exception as e:
                print(f"❌ Step error: {step_name} - {str(e)}")
                results.append((step_name, False))
                break
        
        # Print summary
        print(f"\n{'=' * 60}")
        print("🏁 TEST SUMMARY")
        print(f"{'=' * 60}")
        
        all_passed = True
        for step_name, success in results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{status} - {step_name}")
            if not success:
                all_passed = False
        
        if all_passed:
            print(f"\n🎉 ALL TESTS PASSED!")
            print("The cross-region S3 migration pipeline is working correctly!")
        else:
            print(f"\n❌ SOME TESTS FAILED")
            print("Check the error messages above for troubleshooting.")
        
        print(f"\nCompleted at: {datetime.now().isoformat()}")
        
    except Exception as e:
        print(f"\n❌ Test execution error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
