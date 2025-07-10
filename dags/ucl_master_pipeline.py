# dags/ucl_master_pipeline.py
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum
import boto3
import subprocess
import tempfile
import os
import json
from datetime import datetime
import time

def test_api_connection():
    """Test if the API connection works with correct credentials"""
    import requests
    from airflow.models import Variable
    
    api_key = Variable.get("RAPIDAPI_KEY")
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "uefa-champions-league1.p.rapidapi.com"
    }
    
    print(f"Testing API with key: {api_key[:10]}...")
    url = "https://uefa-champions-league1.p.rapidapi.com/team/list"
    params = {"year": "2025"}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        print("Success! API connection working")
        return True
    else:
        raise Exception(f"API test failed with status {response.status_code}")

def run_ingest_script_multi_year(**context):
    """Run ingestion script for multiple years"""
    s3 = boto3.client('s3')
    
    print("=== Creating Multi-Year Ingestion Script ===")
    
    script_content = '''
import os
import requests
import boto3
import json
from datetime import datetime
import time

# Configuration
API_KEY = os.environ.get("RAPIDAPI_KEY")
API_HOST = "uefa-champions-league1.p.rapidapi.com"
S3_BUCKET_NAME = "ucl-lake-2025"
HEADERS = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}
s3_client = boto3.client('s3')

# Years to process
START_YEAR = 2015
END_YEAR = 2025

def fetch_from_api(endpoint, params=None):
    url = f"https://{API_HOST}/{endpoint}"
    print(f"Fetching: {url} with params: {params}")
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        print(f"✓ Success: {endpoint}")
        return response.json()
    else:
        print(f"✗ Failed {endpoint}: {response.status_code} - {response.text}")
        return None

def upload_to_s3(data, s3_key):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, indent=4)
        )
        print(f"✓ Uploaded: {s3_key}")
        return True
    except Exception as e:
        print(f"✗ Upload error: {e}")
        return False

# Main logic
print(f"Starting multi-year ingestion for {START_YEAR}-{END_YEAR}...")
all_team_ids = set()

for year in range(START_YEAR, END_YEAR + 1):
    print(f"\\n=== Processing Year {year} ===")
    
    # Teams
    teams_data = fetch_from_api("team/list", {"year": str(year)})
    if teams_data:
        upload_to_s3(teams_data, f"raw/teams/year={year}/teams_{year}.json")
        
        if isinstance(teams_data, list):
            teams = teams_data
        elif isinstance(teams_data, dict):
            teams = teams_data.get('teams', []) or teams_data.get('data', [])
        else:
            teams = []
        
        for team in teams:
            if isinstance(team, dict):
                team_id = team.get('id') or team.get('teamId')
                if team_id:
                    all_team_ids.add((team_id, year))
    
    # Schedule
    schedule = fetch_from_api("schedule", {"year": str(year)})
    if schedule:
        upload_to_s3(schedule, f"raw/schedules/year={year}/schedule_{year}.json")
    
    # Standings
    standings = fetch_from_api("standings", {"year": str(year)})
    if standings:
        upload_to_s3(standings, f"raw/standings/year={year}/standings_{year}.json")
    
    time.sleep(1)

# Fetch some team rosters (optional - limited to reduce API calls)
print(f"\\n=== Fetching Team Rosters ===")
recent_teams = [(tid, yr) for tid, yr in all_team_ids if yr >= 2024][:5]

for team_id, year in recent_teams:
    print(f"Fetching roster for team {team_id} ({year})")
    roster = fetch_from_api("team/roster", {"teamId": str(team_id), "year": str(year)})
    if roster:
        upload_to_s3(roster, f"raw/team_rosters/year={year}/team_{team_id}_roster_{year}.json")
    time.sleep(0.5)

print(f"\\nIngestion complete! Processed {END_YEAR - START_YEAR + 1} years")
'''
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script_content)
        temp_file = f.name
    
    api_key = Variable.get("RAPIDAPI_KEY")
    env = os.environ.copy()
    env['RAPIDAPI_KEY'] = api_key
    
    result = subprocess.run(['python3', temp_file], capture_output=True, text=True, env=env)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr}")
    
    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}")
    
    os.unlink(temp_file)
    return result.stdout

def execute_sql_from_s3(sql_file_path, database, output_location, **context):
    """Execute SQL from S3 file using boto3"""
    print(f"Reading SQL file: {sql_file_path}")
    
    # Read SQL from S3
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket='championsleague-mwaa-dags-2025', Key=sql_file_path)
        sql_query = response['Body'].read().decode('utf-8').strip()
        print(f"Successfully read SQL file: {sql_file_path}")
        print(f"SQL query length: {len(sql_query)} characters")
    except Exception as e:
        print(f"Error reading SQL file from S3: {e}")
        raise
    
    # Execute using Athena
    athena = boto3.client('athena', region_name='ap-southeast-1')
    
    response = athena.start_query_execution(
        QueryString=sql_query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    
    query_id = response['QueryExecutionId']
    print(f"Started query execution: {query_id}")
    
    # Wait for completion
    max_attempts = 60
    attempt = 0
    
    while attempt < max_attempts:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        status = result['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        time.sleep(2)
        attempt += 1
    
    if status != 'SUCCEEDED':
        error = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Query failed: {error}")
    
    print(f"Query completed successfully")
    return query_id

# Configuration
ATHENA_OUTPUT_S3 = "s3://ucl-lake-2025/athena-query-results/" 

with DAG(
    dag_id='ucl_master_pipeline_v1', 
    start_date=pendulum.datetime(2025, 7, 8, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    tags=['champions-league'],
) as dag:
    
    # Test API connection
    test_api = PythonOperator(
        task_id='test_api_connection',
        python_callable=test_api_connection
    )
    
    # Ingest data from API
    ingest_data = PythonOperator(
        task_id='ingest_raw_data_to_s3',
        python_callable=run_ingest_script_multi_year
    )

    # Verify data was uploaded
    verify_data = BashOperator(
        task_id='verify_data_uploaded',
        bash_command="""
        echo "=== Checking S3 for uploaded data ==="
        FILE_COUNT=$(aws s3 ls s3://ucl-lake-2025/raw/ --recursive | wc -l)
        echo "Found $FILE_COUNT files in raw folder"
        
        if [ $FILE_COUNT -eq 0 ]; then
            echo "ERROR: No data found in S3. Ingestion failed!"
            exit 1
        fi
        
        echo -e "\nFiles by type:"
        echo "Teams: $(aws s3 ls s3://ucl-lake-2025/raw/teams/ --recursive | wc -l)"
        echo "Schedules: $(aws s3 ls s3://ucl-lake-2025/raw/schedules/ --recursive | wc -l)"
        echo "Standings: $(aws s3 ls s3://ucl-lake-2025/raw/standings/ --recursive | wc -l)"
        echo "Rosters: $(aws s3 ls s3://ucl-lake-2025/raw/team_rosters/ --recursive | wc -l)"
        
        echo -e "\nSample files:"
        aws s3 ls s3://ucl-lake-2025/raw/ --recursive | head -10
        """
    )

    # Create database if not exists
    create_database = AthenaOperator(
        task_id='create_database',
        query="CREATE DATABASE IF NOT EXISTS ucl_analytics_db",
        database='default',
        output_location=ATHENA_OUTPUT_S3
    )

    # Drop and recreate raw table
    drop_raw_table = AthenaOperator(
        task_id='drop_raw_table',
        query="DROP TABLE IF EXISTS ucl_analytics_db.raw",
        database='ucl_analytics_db',
        output_location=ATHENA_OUTPUT_S3
    )

    # Create the raw table with partition projection
    create_raw_table = AthenaOperator(
        task_id='create_raw_table',
        query="""
        CREATE EXTERNAL TABLE IF NOT EXISTS ucl_analytics_db.raw (
            col0 string
        )
        PARTITIONED BY (
            partition_0 string,
            year string
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://ucl-lake-2025/raw/'
        TBLPROPERTIES (
            'projection.enabled' = 'true',
            'projection.partition_0.type' = 'enum',
            'projection.partition_0.values' = 'teams,schedules,standings,team_rosters',
            'projection.year.type' = 'integer',
            'projection.year.range' = '2015,2025',
            'projection.year.digits' = '4',
            'storage.location.template' = 's3://ucl-lake-2025/raw/${partition_0}/year=${year}/'
        )
        """,
        database='ucl_analytics_db',
        output_location=ATHENA_OUTPUT_S3
    )

    # Test the raw table with diagnostic query
    test_raw_table = AthenaOperator(
        task_id='test_raw_table',
        query="""
        WITH data_summary AS (
            SELECT 
                partition_0,
                year,
                COUNT(*) as record_count,
                AVG(LENGTH(col0)) as avg_json_length,
                MIN(LENGTH(col0)) as min_json_length,
                MAX(LENGTH(col0)) as max_json_length,
                'summary' as query_type
            FROM ucl_analytics_db.raw
            WHERE year IN ('2024', '2025')
            GROUP BY partition_0, year
        ),
        sample_data AS (
            SELECT 
                partition_0,
                year,
                1 as record_count,
                LENGTH(col0) as avg_json_length,
                LENGTH(col0) as min_json_length,
                LENGTH(col0) as max_json_length,
                'sample' as query_type
            FROM ucl_analytics_db.raw
            WHERE year IN ('2024', '2025')
              AND col0 IS NOT NULL
              AND LENGTH(col0) > 10
            LIMIT 5
        )
        SELECT * FROM data_summary
        UNION ALL
        SELECT * FROM sample_data
        ORDER BY query_type, partition_0, year
        """,
        database='ucl_analytics_db',
        output_location=ATHENA_OUTPUT_S3
    )

    # Drop existing dimensional and fact tables
    drop_dim_teams = BashOperator(
        task_id='drop_dim_teams',
        bash_command='''
        echo "Dropping dim_teams table and cleaning S3 data..."
        
        # Drop table first
        aws athena start-query-execution \
            --query-string "DROP TABLE IF EXISTS ucl_analytics_db.dim_teams" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId'
        
        # Wait a moment for table drop
        sleep 5
        
        # Clean S3 data
        aws s3 rm s3://ucl-lake-2025/processed/dim_teams/ --recursive || true
        echo "Completed dim_teams cleanup"
        '''
    )

    drop_dim_players = BashOperator(
        task_id='drop_dim_players',
        bash_command='''
        echo "Dropping dim_players table and cleaning S3 data..."
        aws athena start-query-execution \
            --query-string "DROP TABLE IF EXISTS ucl_analytics_db.dim_players" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId'
        sleep 5
        aws s3 rm s3://ucl-lake-2025/processed/dim_players/ --recursive || true
        echo "Completed dim_players cleanup"
        '''
    )

    drop_fact_matches = BashOperator(
        task_id='drop_fact_matches',
        bash_command='''
        echo "Dropping fact_matches table and cleaning S3 data..."
        aws athena start-query-execution \
            --query-string "DROP TABLE IF EXISTS ucl_analytics_db.fact_matches" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId'
        sleep 5
        aws s3 rm s3://ucl-lake-2025/processed/fact_matches/ --recursive || true
        echo "Completed fact_matches cleanup"
        '''
    )

    drop_fact_standings = BashOperator(
        task_id='drop_fact_standings',
        bash_command='''
        echo "Dropping fact_standings table and cleaning S3 data..."
        aws athena start-query-execution \
            --query-string "DROP TABLE IF EXISTS ucl_analytics_db.fact_standings" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId'
        sleep 5
        aws s3 rm s3://ucl-lake-2025/processed/fact_standings/ --recursive || true
        echo "Completed fact_standings cleanup"
        '''
    )

    # Create dimensional and fact tables using SQL files from correct path
    create_dim_teams = PythonOperator(
        task_id='create_dim_teams',
        python_callable=execute_sql_from_s3,
        op_kwargs={
            'sql_file_path': 'scripts/sql/create_dim_teams.sql',  # Updated path
            'database': 'ucl_analytics_db',
            'output_location': ATHENA_OUTPUT_S3
        }
    )

    create_dim_players = PythonOperator(
        task_id='create_dim_players',
        python_callable=execute_sql_from_s3,
        op_kwargs={
            'sql_file_path': 'scripts/sql/create_dim_players.sql',  # Updated path
            'database': 'ucl_analytics_db',
            'output_location': ATHENA_OUTPUT_S3
        }
    )

    create_fact_matches = PythonOperator(
        task_id='create_fact_matches',
        python_callable=execute_sql_from_s3,
        op_kwargs={
            'sql_file_path': 'scripts/sql/create_fact_matches.sql',  # Updated path
            'database': 'ucl_analytics_db',
            'output_location': ATHENA_OUTPUT_S3
        }
    )

    create_fact_standings = PythonOperator(
        task_id='create_fact_standings',
        python_callable=execute_sql_from_s3,
        op_kwargs={
            'sql_file_path': 'scripts/sql/create_fact_standings.sql',  # Updated path
            'database': 'ucl_analytics_db',
            'output_location': ATHENA_OUTPUT_S3
        }
    )

    # Verify transformed tables
    verify_results = BashOperator(
        task_id='verify_results',
        bash_command="""
        echo "=== Checking Transformed Tables ==="
        
        # Function to run athena query with better error handling
        run_query() {
            local query="$1"
            local description="$2"
            echo "Running: $description"
            
            local query_id=$(aws athena start-query-execution \
                --query-string "$query" \
                --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
                --query-execution-context Database=ucl_analytics_db \
                --region ap-southeast-1 \
                --output text --query 'QueryExecutionId')
            
            if [ -z "$query_id" ]; then
                echo "Failed to start query"
                return
            fi
            
            # Wait for query to complete
            sleep 10
            
            # Check query status first
            local status=$(aws athena get-query-execution \
                --query-execution-id "$query_id" \
                --region ap-southeast-1 \
                --output text --query 'QueryExecution.Status.State' 2>/dev/null)
            
            if [ "$status" = "SUCCEEDED" ]; then
                # Get result
                aws athena get-query-results \
                    --query-execution-id "$query_id" \
                    --region ap-southeast-1 \
                    --output text --query 'ResultSet.Rows[1].Data[0].VarCharValue' 2>/dev/null || echo "0"
            else
                echo "Query failed with status: $status"
                # Get error details
                aws athena get-query-execution \
                    --query-execution-id "$query_id" \
                    --region ap-southeast-1 \
                    --output text --query 'QueryExecution.Status.StateChangeReason' 2>/dev/null || echo "Unknown error"
            fi
        }
        
        # First, check if raw data was re-ingested properly
        echo -e "\n=== Raw Data Check (After Re-ingestion) ==="
        run_query "SELECT partition_0, year, COUNT(*) as count, AVG(LENGTH(col0)) as avg_len FROM ucl_analytics_db.raw WHERE year IN ('2024', '2025') GROUP BY partition_0, year ORDER BY partition_0, year" "Raw data counts"
        
        # Check JSON validity after re-ingestion
        echo -e "\n=== JSON Validity Check ==="
        run_query "SELECT partition_0, COUNT(*) as total, SUM(CASE WHEN try(json_parse(col0)) IS NOT NULL THEN 1 ELSE 0 END) as valid_json FROM ucl_analytics_db.raw WHERE year IN ('2024', '2025') GROUP BY partition_0" "JSON validity"
        
        # Test simple teams extraction
        echo -e "\n=== Simple Teams Test ==="
        run_query "SELECT COUNT(*) FROM ucl_analytics_db.raw WHERE partition_0 = 'teams' AND year = '2024' AND try(json_parse(col0)) IS NOT NULL AND json_extract(json_parse(col0), '$.teams') IS NOT NULL" "Teams with valid JSON and teams array"
        
        echo -e "\n=== Record Counts ==="
        echo "Teams count: $(run_query 'SELECT COUNT(*) FROM dim_teams' 'Teams count')"
        
        # Check if other tables exist before querying them
        echo "Checking if other tables exist..."
        run_query "SHOW TABLES LIKE 'fact_matches'" "Check fact_matches table"
        run_query "SHOW TABLES LIKE 'dim_players'" "Check dim_players table"  
        run_query "SHOW TABLES LIKE 'fact_standings'" "Check fact_standings table"
        
        # Only query counts if tables exist
        if aws athena start-query-execution --query-string "SELECT 1 FROM fact_matches LIMIT 1" --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ --query-execution-context Database=ucl_analytics_db --region ap-southeast-1 >/dev/null 2>&1; then
            echo "Matches count: $(run_query 'SELECT COUNT(*) FROM fact_matches' 'Matches count')"
        else
            echo "Matches count: Table does not exist"
        fi
        
        if aws athena start-query-execution --query-string "SELECT 1 FROM dim_players LIMIT 1" --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ --query-execution-context Database=ucl_analytics_db --region ap-southeast-1 >/dev/null 2>&1; then
            echo "Players count: $(run_query 'SELECT COUNT(*) FROM dim_players' 'Players count')"
        else
            echo "Players count: Table does not exist"
        fi
        
        if aws athena start-query-execution --query-string "SELECT 1 FROM fact_standings LIMIT 1" --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ --query-execution-context Database=ucl_analytics_db --region ap-southeast-1 >/dev/null 2>&1; then
            echo "Standings count: $(run_query 'SELECT COUNT(*) FROM fact_standings' 'Standings count')"
        else
            echo "Standings count: Table does not exist"
        fi
        
        # Check S3 processed data
        echo -e "\n=== Processed data in S3 ==="
        for folder in dim_teams fact_matches dim_players fact_standings; do
            FILE_COUNT=$(aws s3 ls s3://ucl-lake-2025/processed/$folder/ --recursive 2>/dev/null | wc -l)
            echo "$folder: $FILE_COUNT files"
        done
        
        # Sample data from each table
        echo -e "\n=== Sample Data ==="
        echo -e "\nSample Teams:"
        run_query "SELECT team_id, team_name, team_abbr, COALESCE(team_short_name, team_name) as short_name, season_year FROM dim_teams WHERE team_name IS NOT NULL LIMIT 5" "Teams sample"
        
        # Only show samples for tables that exist
        if aws athena start-query-execution --query-string "SELECT 1 FROM fact_standings LIMIT 1" --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ --query-execution-context Database=ucl_analytics_db --region ap-southeast-1 >/dev/null 2>&1; then
            echo -e "\nSample Standings:"
            run_query "SELECT season_year, team_id, position, points, games_played, wins, draws, losses FROM fact_standings WHERE season_year >= 2024 LIMIT 5" "Standings sample"
        else
            echo -e "\nStandings table not available yet"
        fi
        
        if aws athena start-query-execution --query-string "SELECT 1 FROM fact_matches LIMIT 1" --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ --query-execution-context Database=ucl_analytics_db --region ap-southeast-1 >/dev/null 2>&1; then
            echo -e "\nSample Matches:"
            run_query "SELECT match_id, match_date, home_team_id, away_team_id, home_score, away_score, COALESCE(match_status, 'Unknown') as status FROM fact_matches WHERE match_date IS NOT NULL LIMIT 5" "Matches sample"
        else
            echo -e "\nMatches table not available yet"
        fi
        
        if aws athena start-query-execution --query-string "SELECT 1 FROM dim_players LIMIT 1" --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ --query-execution-context Database=ucl_analytics_db --region ap-southeast-1 >/dev/null 2>&1; then
            echo -e "\nSample Players:"
            run_query "SELECT player_id, player_name, position, jersey_number, team_id FROM dim_players WHERE player_name IS NOT NULL LIMIT 5" "Players sample"
        else
            echo -e "\nPlayers table not available yet"
        fi
        """,
        trigger_rule='all_done'
    )
    
    # Add simple raw data diagnostic
    diagnose_raw_content = BashOperator(
        task_id='diagnose_raw_content',
        bash_command="""
        echo "=== Raw Data Diagnosis ==="
        
        echo -e "\n1. Raw data counts by partition/year:"
        QUERY_ID=$(aws athena start-query-execution \
            --query-string "SELECT partition_0, year, COUNT(*) as count FROM ucl_analytics_db.raw GROUP BY partition_0, year ORDER BY partition_0, year" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId')
        
        sleep 10
        aws athena get-query-results \
            --query-execution-id "$QUERY_ID" \
            --region ap-southeast-1 \
            --output table --query 'ResultSet.Rows[*].Data[*].VarCharValue' 2>/dev/null || echo "No results"
        
        echo -e "\n2. Sample JSON content from teams:"
        SAMPLE_QUERY_ID=$(aws athena start-query-execution \
            --query-string "SELECT partition_0, year, SUBSTR(col0, 1, 500) as sample_json FROM ucl_analytics_db.raw WHERE partition_0 = 'teams' AND LENGTH(col0) > 50 LIMIT 3" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId')
        
        sleep 10
        aws athena get-query-results \
            --query-execution-id "$SAMPLE_QUERY_ID" \
            --region ap-southeast-1 \
            --output table --query 'ResultSet.Rows[*].Data[*].VarCharValue' 2>/dev/null || echo "No sample data"
            
        echo -e "\n3. JSON parsing test:"
        PARSE_QUERY_ID=$(aws athena start-query-execution \
            --query-string "SELECT partition_0, year, LENGTH(col0) as json_len, try(json_parse(col0)) IS NOT NULL as is_valid_json FROM ucl_analytics_db.raw WHERE LENGTH(col0) > 50 LIMIT 5" \
            --result-configuration OutputLocation=s3://ucl-lake-2025/athena-query-results/ \
            --query-execution-context Database=ucl_analytics_db \
            --region ap-southeast-1 \
            --output text --query 'QueryExecutionId')
        
        sleep 10
        aws athena get-query-results \
            --query-execution-id "$PARSE_QUERY_ID" \
            --region ap-southeast-1 \
            --output table --query 'ResultSet.Rows[*].Data[*].VarCharValue' 2>/dev/null || echo "No parse results"
        """
    )

    # --- Define Task Dependencies ---
    
    # Main ingestion flow
    test_api >> ingest_data >> verify_data
    
    # Database and table setup
    verify_data >> create_database
    create_database >> drop_raw_table >> create_raw_table >> test_raw_table >> diagnose_raw_content
    
    # Drop existing tables in parallel
    diagnose_raw_content >> [drop_dim_teams, drop_dim_players, drop_fact_matches, drop_fact_standings]
    
    # Create new tables after drops complete
    drop_dim_teams >> create_dim_teams
    drop_dim_players >> create_dim_players
    drop_fact_matches >> create_fact_matches
    drop_fact_standings >> create_fact_standings
    
    # Make standings depend on matches being created first
    create_fact_matches >> create_fact_standings
    
    # Final verification after all tables are created
    [create_dim_teams, create_dim_players, create_fact_standings] >> verify_results

    # Raw data diagnosis
    test_raw_table >> diagnose_raw_content