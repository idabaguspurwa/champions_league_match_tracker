# Fixed ingest_data.py script
import os
import requests
import boto3
import json
from datetime import datetime

# --- Configuration ---
API_KEY = os.environ.get("RAPIDAPI_KEY") or os.getenv("RAPIDAPI_KEY")
API_HOST = "uefa-champions-league1.p.rapidapi.com"
S3_BUCKET_NAME = "ucl-lake-2025"
HEADERS = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}
s3_client = boto3.client('s3')

# Years to fetch data for
START_YEAR = 2015
END_YEAR = 2025

# --- Helper Functions ---
def fetch_from_api(endpoint, params=None):
    url = f"https://{API_HOST}/{endpoint}"
    print(f"Fetching: {url} with params: {params}")
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        print(f"✓ Successfully fetched {endpoint}")
        return response.json()
    else:
        print(f"✗ Error fetching {endpoint}: {response.status_code} - {response.text}")
        return None

def upload_to_s3(data, s3_key):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data)  # Remove indent=4 to create single-line JSON
        )
        print(f"✓ Uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        print(f"✗ Error uploading to S3: {e}")
        return False

# --- Main Ingestion Logic ---
def main():
    print(f"Starting ingestion to bucket: {S3_BUCKET_NAME}")
    print(f"API Key available: {'Yes' if API_KEY else 'No'}")
    print(f"Fetching data for years: {START_YEAR} to {END_YEAR}")
    
    if not API_KEY:
        print("ERROR: RAPIDAPI_KEY not found in environment!")
        return
    
    # Track all teams across years for roster fetching
    all_team_ids = set()
    
    # Loop through each year
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n{'='*50}")
        print(f"Processing Year: {year}")
        print(f"{'='*50}")
        
        # 1. Get Team List for this year
        print(f"\n--- Fetching Team List for {year} ---")
        team_list = fetch_from_api("teams/list", params={"year": str(year)})
        if not team_list:
            # Try alternative endpoint
            team_list = fetch_from_api("team/list", params={"year": str(year)})
        
        if team_list:
            upload_to_s3(team_list, f"raw/teams/year={year}/teams_{year}.json")
            
            # Handle both list and dict responses
            teams = []
            if isinstance(team_list, list):
                # Response is already a list of teams
                teams = team_list
            elif isinstance(team_list, dict):
                # Response is a dictionary, extract teams
                teams = team_list.get('teams', []) or team_list.get('data', []) or []
            
            # Extract team IDs
            for team in teams:
                if isinstance(team, dict):
                    team_id = team.get('id') or team.get('teamId') or team.get('team_id')
                    if team_id:
                        all_team_ids.add((team_id, year))
        
        # 2. Get Schedule for this year
        print(f"\n--- Fetching Schedule for {year} ---")
        schedule = fetch_from_api("schedule", params={"year": str(year)})
        if not schedule:
            # Try with season parameter
            schedule = fetch_from_api("schedule", params={"season": str(year)})
        
        if schedule:
            upload_to_s3(schedule, f"raw/schedules/year={year}/schedule_{year}.json")
        
        # 3. Get Standings for this year
        print(f"\n--- Fetching Standings for {year} ---")
        standings = fetch_from_api("standings", params={"year": str(year)})
        if not standings:
            # Try with season parameter
            standings = fetch_from_api("standings", params={"season": str(year)})
        
        if standings:
            upload_to_s3(standings, f"raw/standings/year={year}/standings_{year}.json")
        
        # Add small delay to avoid rate limiting
        import time
        time.sleep(1)
    
    # 4. Fetch team rosters (limit to recent years and a few teams to avoid rate limits)
    print(f"\n{'='*50}")
    print("Fetching Team Rosters")
    print(f"{'='*50}")
    
    # Only fetch rosters for recent years and limit number of teams
    recent_teams = [(tid, yr) for tid, yr in all_team_ids if yr >= 2023]
    
    for i, (team_id, year) in enumerate(recent_teams[:10]):  # Limit to 10 rosters
        print(f"\n--- Fetching roster for team {team_id} ({year}) ---")
        roster = fetch_from_api("team/roster", params={"teamId": str(team_id), "year": str(year)})
        if roster:
            upload_to_s3(roster, f"raw/team_rosters/year={year}/team_{team_id}_roster_{year}.json")
        
        # Add delay to avoid rate limiting
        time.sleep(0.5)
    
    # 5. Create a summary file
    summary = {
        "ingestion_date": datetime.now().isoformat(),
        "years_processed": list(range(START_YEAR, END_YEAR + 1)),
        "total_years": END_YEAR - START_YEAR + 1,
        "teams_found": len(all_team_ids),
        "rosters_fetched": min(10, len(recent_teams))
    }
    upload_to_s3(summary, "raw/ingestion_summary.json")
    
    print(f"\n{'='*50}")
    print("Ingestion Complete!")
    print(f"Processed {END_YEAR - START_YEAR + 1} years of data")
    print(f"Found {len(all_team_ids)} unique team-year combinations")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()