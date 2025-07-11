# Champions League Match Tracker

A comprehensive data pipeline for tracking and analyzing UEFA Champions League matches using real historical data from 2015-2025.

## 🏆 Overview

This project processes real Champions League match data to create structured datasets for analysis and visualization. The pipeline extracts match information from JSON files, processes it through Apache Airflow, and stores the results in AWS S3 and Athena for easy querying.

## 📊 Data Coverage

- **Years**: 2015-2025 (11 seasons)
- **Matches**: 1,797 real Champions League matches
- **Teams**: All participating Champions League teams
- **Players**: Player rosters for each team
- **Standings**: Calculated from actual match results

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- AWS CLI configured
- Apache Airflow
- Access to S3 bucket: `ucl-lake-2025`

### Setup Steps

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Extract Real Match Data**
   ```bash
   python extract_real_matches.py
   ```

3. **Create External Tables**
   ```bash
   python create_external_tables.py
   ```

4. **Run the Pipeline**
   ```bash
   python -m airflow dags trigger ucl_master_pipeline_v1
   ```

## 📁 Project Structure

```
Champions League Match Tracker/
├── dags/
│   └── ucl_master_pipeline.py          # Main Airflow pipeline
├── scripts/
│   ├── sql/
│   │   ├── create_dim_teams.sql        # Teams dimension table
│   │   ├── create_dim_players.sql      # Players dimension table
│   │   ├── create_fact_matches.sql     # Matches fact table
│   │   └── create_fact_standings.sql   # Standings fact table
│   ├── ingest_data.py                  # Data ingestion script
│   └── fix_json_format.py              # JSON formatting utilities
├── extract_real_matches.py             # Real data extraction script
├── create_external_tables.py           # Athena external table setup
├── real_matches.csv                    # Extracted real match data
├── requirements.txt                    # Python dependencies
└── README.md                           # This file
```

## 🔧 Data Processing Pipeline

### 1. Data Extraction (`extract_real_matches.py`)
- Reads complete JSON files from S3
- Extracts match details (teams, scores, dates, venues)
- Converts to CSV format for Athena processing
- Uploads to S3 for pipeline consumption

### 2. External Tables (`create_external_tables.py`)
- Creates Athena external tables to read CSV data
- Configures proper data types and formats
- Enables SQL queries on the raw match data

### 3. Airflow Pipeline (`ucl_master_pipeline.py`)
- **Data Ingestion**: Processes raw JSON files
- **Teams Dimension**: Creates team master data
- **Players Dimension**: Creates player rosters
- **Matches Fact**: Processes real match results
- **Standings Fact**: Calculates standings from match results

### 4. SQL Processing (`scripts/sql/`)
- **Teams**: Team information and metadata
- **Players**: Player details and team associations
- **Matches**: Real match results with scores and details
- **Standings**: Calculated league standings with points, wins, losses

## 📈 Output Tables

### `dim_teams`
- Team ID, Name, Abbreviation
- Season year information
- Team metadata

### `dim_players`
- Player ID, Name, Position
- Jersey numbers
- Team associations

### `fact_matches`
- Match ID, Date, Teams
- Final scores and results
- Match status and venues
- 1,797 real matches (2015-2025)

### `fact_standings`
- Team standings by season
- Points, wins, draws, losses
- Goals for/against, goal difference
- Calculated from actual match results

## 🔍 Data Quality

- ✅ **Real Data**: All 1,797 matches are from actual Champions League games
- ✅ **Complete Coverage**: 2015-2025 seasons included
- ✅ **Accurate Scores**: Real match results with correct scores
- ✅ **Validated**: Data integrity checks throughout pipeline
- ✅ **Consistent**: Standardized format across all tables

## 📊 Analytics Ready

The processed data is ready for:
- **Tableau**: Direct connection to Athena tables
- **Power BI**: Query processed Parquet files
- **SQL Analysis**: Direct querying via Athena
- **Python/R**: Access via AWS SDK

## 🛠️ Troubleshooting

### Common Issues

1. **External Tables Missing**
   - Run `python create_external_tables.py`
   - Check AWS credentials

2. **No Match Data**
   - Verify `real_matches.csv` exists in S3
   - Check boolean format in SQL (use 'True' not 'true')

3. **Pipeline Failures**
   - Check Airflow logs for specific errors
   - Verify S3 permissions
   - Ensure Athena database exists

## 🎯 Next Steps

1. **Data Visualization**: Connect to Tableau/Power BI
2. **Advanced Analytics**: Add ML models for predictions
3. **Real-time Updates**: Add current season data ingestion
4. **API Integration**: Create REST API for match data

## 📝 Notes

- All timestamps are in UTC
- Match data includes only completed games
- Standings calculated using standard football scoring (3 points for win, 1 for draw)
- Team IDs are consistent across all tables

---

**Last Updated**: July 2025  
**Data Version**: 1,797 matches (2015-2025)  
**Status**: ✅ Production Ready
