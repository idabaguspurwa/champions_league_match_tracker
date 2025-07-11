"""
Champions League Match Tracker - Real Data Extraction

This script extracts real Champions League match data from S3 JSON files
and converts it to CSV format for processing by the Airflow pipeline.

Data Coverage: 2015-2025 seasons
Output: CSV file with match details (teams, scores, dates, venues)
Location: Uploaded to s3://ucl-lake-2025/processed/real_matches/

Author: Champions League Pipeline
Date: July 2025
"""

import boto3
import json
import csv
import datetime
from dateutil.parser import parse as parse_date

def main():
    print("=== Extracting Real Matches from S3 ===")
    
    s3 = boto3.client('s3', region_name='ap-southeast-1')
    bucket = 'ucl-lake-2025'
    
    all_matches = []
    
    years = [2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]
    
    for year in years:
        print(f"\nProcessing year {year}...")
        key = f'raw/schedules/year={year}/schedule_{year}.json'
        
        try:
            # Read the schedule file
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            if 'schedule' in data:
                schedule = data['schedule']
                matches_count = 0
                
                # Process each date in the schedule
                for date_key, date_matches in schedule.items():
                    if isinstance(date_matches, list):
                        for match in date_matches:
                            if isinstance(match, dict) and 'id' in match:
                                try:
                                    match_record = extract_match_data(match, year)
                                    if match_record:
                                        all_matches.append(match_record)
                                        matches_count += 1
                                except Exception as e:
                                    print(f"  Error processing match: {e}")
                                    continue
                
                print(f"  Extracted {matches_count} matches from {year}")
            else:
                print(f"  No schedule found in {year}")
                
        except Exception as e:
            print(f"  Error reading {year}: {e}")
    
    # Save to CSV
    if all_matches:
        print(f"\nSaving {len(all_matches)} total matches to CSV...")
        
        # Save locally first
        csv_file = 'real_matches.csv'
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'match_id', 'match_datetime', 'match_date', 'completed', 
                'match_status', 'home_team_id', 'home_score', 'away_team_id', 
                'away_score', 'match_name', 'match_short_name', 'venue', 'season_year'
            ])
            
            # Data
            for match in all_matches:
                writer.writerow([
                    match['match_id'],
                    match['match_datetime'],
                    match['match_date'],
                    match['completed'],
                    match['match_status'],
                    match['home_team_id'],
                    match['home_score'],
                    match['away_team_id'],
                    match['away_score'],
                    match['match_name'],
                    match['match_short_name'],
                    match['venue'],
                    match['season_year']
                ])
        
        print(f"CSV saved to {csv_file}")
        
        # Upload to S3
        s3_key = 'processed/real_matches/real_matches.csv'
        s3.upload_file(csv_file, bucket, s3_key)
        print(f"Uploaded to s3://{bucket}/{s3_key}")
        
        # Show sample data
        print(f"\nSample matches:")
        for i, match in enumerate(all_matches[:5]):
            print(f"  {i+1}. {match['match_date']} - {match['home_team_id']} vs {match['away_team_id']} ({match['home_score']}-{match['away_score']})")
    
    else:
        print("No matches found!")

def extract_match_data(match, year):
    """Extract match data from JSON match object"""
    try:
        match_id = match.get('id', '')
        if not match_id:
            return None
            
        # Parse match date
        match_date_str = match.get('date', '')
        match_datetime = None
        match_date = None
        
        if match_date_str:
            try:
                dt = parse_date(match_date_str)
                match_datetime = dt.isoformat()
                match_date = dt.date().isoformat()
            except:
                match_datetime = match_date_str
                match_date = match_date_str[:10] if len(match_date_str) >= 10 else match_date_str
        
        # Extract completion status
        completed = match.get('completed', False)
        
        # Extract match status
        status_info = match.get('status', {})
        match_status = status_info.get('detail', 'TBD')
        
        # Extract venue
        venue_info = match.get('venue', {})
        venue = venue_info.get('fullName', 'Unknown')
        
        # Extract teams and scores from competitors
        competitors = match.get('competitors', [])
        if len(competitors) < 2:
            return None
            
        home_team = None
        away_team = None
        
        # Find home and away teams
        for competitor in competitors:
            if competitor.get('isHome', False):
                home_team = competitor
            else:
                away_team = competitor
        
        # If no home/away designation, use first as home, second as away
        if not home_team or not away_team:
            if len(competitors) >= 2:
                home_team = competitors[0]
                away_team = competitors[1]
            else:
                return None
        
        # Extract team IDs and scores
        home_team_id = home_team.get('id', '')
        away_team_id = away_team.get('id', '')
        home_score = home_team.get('score', 0)
        away_score = away_team.get('score', 0)
        
        # Convert scores to integers
        try:
            home_score = int(home_score) if home_score is not None else 0
            away_score = int(away_score) if away_score is not None else 0
        except:
            home_score = 0
            away_score = 0
        
        return {
            'match_id': match_id,
            'match_datetime': match_datetime,
            'match_date': match_date,
            'completed': completed,
            'match_status': match_status,
            'home_team_id': home_team_id,
            'home_score': home_score,
            'away_team_id': away_team_id,
            'away_score': away_score,
            'match_name': 'Champions League',
            'match_short_name': 'UCL',
            'venue': venue,
            'season_year': year
        }
        
    except Exception as e:
        print(f"Error extracting match data: {e}")
        return None

if __name__ == "__main__":
    main()
