#!/usr/bin/env python3
"""
Flask web server for Customer Analytics Dashboard
Queries AWS Athena for customer data and serves API endpoints
"""

import json
import os
import sys
import time
from collections import Counter
from flask import Flask, jsonify, send_from_directory
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path to import from project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)

def load_run_data():
    """Load run_data.json from parent directory"""
    run_data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'run_data.json')
    try:
        with open(run_data_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading run_data.json: {e}")
        return None

def query_athena_for_countries():
    """Query Athena for customer country data"""
    try:
        run_data = load_run_data()
        if not run_data:
            return {"error": "Could not load run_data.json"}
        
        # Get configuration from run_data
        target_region = run_data['regions']['target_region']
        database_name = run_data['resources']['glue']['database_name']
        workgroup = run_data['resources']['athena']['workgroup']
        
        # Initialize Athena client
        athena = boto3.client('athena', region_name=target_region)
        glue = boto3.client('glue', region_name=target_region)
        
        # Get table name
        response = glue.get_tables(DatabaseName=database_name)
        tables = response['TableList']
        
        if not tables:
            return {"error": "No tables found in Glue catalog"}
        
        table_name = tables[0]['Name']
        
        # Query to get country distribution
        query = f'''
        SELECT country, COUNT(*) as customer_count
        FROM "{database_name}"."{table_name}"
        WHERE country IS NOT NULL AND country != ''
        GROUP BY country
        ORDER BY customer_count DESC
        '''
        
        print(f"Executing Athena query: {query}")
        
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
                
                if len(rows) <= 1:  # Only header row
                    return {"error": "No customer data found"}
                
                # Parse results
                countries = []
                total_customers = 0
                
                for row in rows[1:]:  # Skip header row
                    country = row['Data'][0].get('VarCharValue', 'Unknown')
                    count = int(row['Data'][1].get('VarCharValue', '0'))
                    countries.append({
                        'country': country,
                        'count': count
                    })
                    total_customers += count
                
                return {
                    'countries': countries,
                    'total': total_customers,
                    'query_time': time.time() - start_time
                }
                
            elif status in ['QUEUED', 'RUNNING']:
                print(f"Query status: {status}, waiting...")
                time.sleep(2)
            else:
                error_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                return {"error": f"Query failed: {error_reason}"}
        
        return {"error": f"Query did not complete within {max_wait} seconds"}
        
    except ClientError as e:
        return {"error": f"AWS error: {e.response['Error']['Message']}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}

@app.route('/')
def index():
    """Serve the main dashboard page"""
    return send_from_directory('.', 'index.html')

@app.route('/api/customer-data')
def get_customer_data():
    """API endpoint to get customer country distribution data"""
    try:
        data = query_athena_for_countries()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": time.time()})

if __name__ == '__main__':
    print("🌍 Starting Customer Analytics Dashboard Server")
    print("📊 Dashboard: http://localhost:8888")
    print("🔗 API: http://localhost:8888/api/customer-data")
    print("💚 Health: http://localhost:8888/health")
    print("-" * 50)
    
    app.run(host='0.0.0.0', port=8888, debug=True)
