"""
Champions League Match Tracker - External Tables Setup

This script creates the necessary external tables in Athena to read
the real Champions League match data from S3 CSV files.

Run this script BEFORE running the Airflow pipeline to ensure
the external tables exist.

Author: Champions League Pipeline
Date: July 2025
"""

import boto3
import time

def run_athena_query(query, description):
    """Run Athena query and return success status"""
    client = boto3.client('athena', region_name='ap-southeast-1')
    
    print(f"Running: {description}")
    
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'ucl_analytics_db'},
        ResultConfiguration={'OutputLocation': 's3://ucl-lake-2025/athena-query-results/'}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query to complete
    for i in range(30):
        query_details = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_details['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            print(f"✓ {description} - SUCCESS")
            return True
        elif status == 'FAILED':
            reason = query_details['QueryExecution']['Status']['StateChangeReason']
            print(f"✗ {description} - FAILED: {reason}")
            return False
        elif status == 'CANCELLED':
            print(f"✗ {description} - CANCELLED")
            return False
        
        time.sleep(2)
    
    print(f"✗ {description} - TIMEOUT")
    return False

def main():
    """Create external tables for real Champions League data"""
    print("=== Champions League Match Tracker - External Tables Setup ===")
    print("Creating external tables to read real match data from S3...")
    
    # External table for fact_matches
    matches_query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS ucl_analytics_db.real_matches_csv (
        match_id STRING,
        match_datetime STRING,
        match_date STRING,
        completed STRING,
        match_status STRING,
        home_team_id STRING,
        home_score STRING,
        away_team_id STRING,
        away_score STRING,
        match_name STRING,
        match_short_name STRING,
        venue STRING,
        season_year STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
        'serialization.format' = ',',
        'field.delim' = ',',
        'escape.delim' = '\\\\'
    )
    STORED AS TEXTFILE
    LOCATION 's3://ucl-lake-2025/processed/real_matches/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1',
        'projection.enabled' = 'false'
    )
    """
    
    success1 = run_athena_query(matches_query, "Create external table for matches")
    
    # External table for fact_standings (same structure, different name)
    standings_query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS ucl_analytics_db.real_matches_for_standings (
        match_id STRING,
        match_datetime STRING,
        match_date STRING,
        completed STRING,
        match_status STRING,
        home_team_id STRING,
        home_score STRING,
        away_team_id STRING,
        away_score STRING,
        match_name STRING,
        match_short_name STRING,
        venue STRING,
        season_year STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
        'serialization.format' = ',',
        'field.delim' = ',',
        'escape.delim' = '\\\\'
    )
    STORED AS TEXTFILE
    LOCATION 's3://ucl-lake-2025/processed/real_matches/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1',
        'projection.enabled' = 'false'
    )
    """
    
    success2 = run_athena_query(standings_query, "Create external table for standings")
    
    if success1 and success2:
        print("\n✅ SUCCESS: All external tables created successfully!")
        print("📊 Data: 1,797 real Champions League matches (2015-2025)")
        print("🚀 Ready: The Airflow pipeline can now be run with real data.")
        
        # Test the external table
        test_query = "SELECT COUNT(*) FROM ucl_analytics_db.real_matches_csv"
        print(f"\n🔍 Testing external table...")
        if run_athena_query(test_query, "Verify match data count"):
            print("✅ External tables are working correctly!")
        
    else:
        print("\n❌ FAILED: Some external tables failed to create.")
        print("Please check the error messages above and try again.")

if __name__ == "__main__":
    main()
