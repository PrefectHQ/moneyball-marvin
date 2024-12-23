import statsapi
import csv
from datetime import datetime
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from prefect import flow, task
from prefect.blocks.system import Secret

secret_block_sf_password = Secret.load("snowflake-password")
secret_block_sf_account = Secret.load("snowflake-account")

sf_password = secret_block_sf_password.get()
sf_account = secret_block_sf_account.get()


@task
def export_roster_to_csv(team_id=120, output_dir="."):
    # Get raw roster data
    roster_data = statsapi.get('team_roster', {'teamId': team_id})
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"nationals_roster.csv"
    
    # Define the fields we want to extract
    fieldnames = ['jerseyNumber', 'position', 'status', 'fullName', 'id']
    
    # Write to CSV file
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write player data
        for player in roster_data['roster']:
            row = {
                'jerseyNumber': player['jerseyNumber'],
                'position': player['position']['abbreviation'],
                'status': player['status']['description'],
                'fullName': player['person']['fullName'],
                'id': player['person']['id']
            }
            writer.writerow(row)
    
    return filename, roster_data

@task
def export_schedule_to_csv(team_id=120, output_dir="."):
    # Get 2024 season schedule
    schedule_data = statsapi.schedule(
        team=team_id, 
        start_date='2024-03-28', 
        end_date='2024-11-02'
    )
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"nationals_schedule.csv"
    
    # Define the fields for our CSV
    fieldnames = [
        'game_date',
        'game_id',
        'home_team',
        'home_team_id',
        'away_team',
        'away_team_id',
        'venue'
    ]
    
    # Write to CSV file
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write game data
        for game in schedule_data:
            row = {
                'game_date': game['game_date'],
                'game_id': game['game_id'],
                'home_team': game['home_name'],
                'home_team_id': game['home_id'],
                'away_team': game['away_name'],
                'away_team_id': game['away_id'],
                'venue': game['venue_name']
            }
            writer.writerow(row)
    
    return filename, schedule_data

@task
def process_batting_stats(boxscore, schedule_game_id, team_type):
    batting_records = []
    
    # Get batters array (skip the header row)
    batters = boxscore.get(f'{team_type}Batters', [])[1:]  # Skip first element which is header
    team_info = boxscore['teamInfo'][team_type]
    
    for batter in batters:
        if isinstance(batter, dict):  # Only process actual batter entries
            record = {
                'game_id': schedule_game_id,  # Use the schedule's game ID
                'game_date': boxscore['gameId'].split('/')[0:3],  # Extract date from boxscore ID
                'team_id': team_info['id'],
                'team_name': team_info['teamName'],
                'player_id': batter.get('personId', ''),
                'player_name': batter.get('name', ''),
                'position': batter.get('position', ''),
                'batting_order': batter.get('battingOrder', ''),
                'ab': batter.get('ab', ''),
                'r': batter.get('r', ''),
                'h': batter.get('h', ''),
                'doubles': batter.get('doubles', ''),
                'triples': batter.get('triples', ''),
                'hr': batter.get('hr', ''),
                'rbi': batter.get('rbi', ''),
                'bb': batter.get('bb', ''),
                'k': batter.get('k', ''),
                'lob': batter.get('lob', ''),
                'avg': batter.get('avg', ''),
                'ops': batter.get('ops', ''),
                'obp': batter.get('obp', ''),
                'slg': batter.get('slg', '')
            }
            batting_records.append(record)
    
    return batting_records

@task
def process_pitching_stats(boxscore, schedule_game_id, team_type):
    pitching_records = []
    
    # Get pitchers array (skip the header row)
    pitchers = boxscore.get(f'{team_type}Pitchers', [])[1:]  # Skip first element which is header
    team_info = boxscore['teamInfo'][team_type]
    
    for pitcher in pitchers:
        if isinstance(pitcher, dict):  # Only process actual pitcher entries
            record = {
                'game_id': schedule_game_id,  # Use the schedule's game ID
                'game_date': boxscore['gameId'].split('/')[0:3],  # Extract date from boxscore ID
                'team_id': team_info['id'],
                'team_name': team_info['teamName'],
                'player_name': pitcher.get('name', ''),
                'player_id': pitcher.get('personId', ''),
                'note': pitcher.get('note', ''),
                'ip': pitcher.get('ip', ''),
                'h': pitcher.get('h', ''),
                'r': pitcher.get('r', ''),
                'er': pitcher.get('er', ''),
                'bb': pitcher.get('bb', ''),
                'k': pitcher.get('k', ''),
                'hr': pitcher.get('hr', ''),
                'era': pitcher.get('era', ''),
                'pitches': pitcher.get('p', ''),
                'strikes': pitcher.get('s', '')
            }
            pitching_records.append(record)
    
    return pitching_records

@task
def collect_season_boxscores(schedule_file, exclude_game_id=744798):
    # Read the schedule CSV
    df = pd.read_csv(schedule_file)
    
    # Remove the specific game ID
    df = df[df['game_id'] != exclude_game_id]
    
    # Initialize lists for batting and pitching data
    all_batting = []
    all_pitching = []
    
    # Collect data for each game
    for index, row in df.iterrows():
        game_id = row['game_id']
        game_date = row['game_date']
        
        try:
            boxscore = statsapi.boxscore_data(game_id)
            
            # Process home and away teams
            for team_type in ['home', 'away']:
                all_batting.extend(process_batting_stats(boxscore, game_id, team_type))
                all_pitching.extend(process_pitching_stats(boxscore, game_id, team_type))
            
            print(f"Processed game {game_id} ({game_date})")
            
        except Exception as e:
            print(f"Error processing game {game_id}: {str(e)}")
    
    # Convert to DataFrames
    batting_df = pd.DataFrame(all_batting)
    pitching_df = pd.DataFrame(all_pitching)
    
    # Save to CSV files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batting_filename = f"nationals_season_batting.csv"
    pitching_filename = f"nationals_season_pitching.csv"
    
    batting_df.to_csv(batting_filename, index=False)
    pitching_df.to_csv(pitching_filename, index=False)
    
    return batting_filename, pitching_filename, batting_df, pitching_df

@task
def upload_csv_to_snowflake(csv_path, table_name, database, schema, 
                           account, user, password, warehouse, role=None):
    """
    Upload a CSV file to a Snowflake table.
    
    Parameters:
    csv_path (str): Path to the CSV file
    table_name (str): Name of the target Snowflake table
    database (str): Snowflake database name
    schema (str): Snowflake schema name
    account (str): Snowflake account identifier
    user (str): Snowflake username
    password (str): Snowflake password
    warehouse (str): Snowflake warehouse name
    role (str, optional): Snowflake role name
    
    Returns:
    tuple: (success_rows, num_chunks)
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        print(f"Successfully read CSV with {len(df)} rows")
        
        # Create Snowflake connection
        conn = connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        # Explicitly set the context
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.execute(f"USE WAREHOUSE {warehouse}")
        
        print("Connected to Snowflake successfully")
        
        # Write the dataframe to Snowflake
        success, nchunks, _, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True
        )
        
        print(f"Successfully uploaded {success} rows in {nchunks} chunks")
        
        return success, nchunks
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()
            print("Snowflake connection closed")

@flow(log_prints=True)
def run_stuff():
    # Get Roster
    filename, raw_data = export_roster_to_csv()
    print(f"CSV file has been created: {filename}")
    print("\nRaw data dictionary:", raw_data)
    # Get Schedule
    filename, raw_data = export_schedule_to_csv()
    print(f"CSV file has been created: {filename}")
    print(f"Total games in schedule: {len(raw_data)}")

    batting_file, pitching_file, batting_df, pitching_df = collect_season_boxscores('nationals_schedule.csv')
    print(f"\nFiles created:")
    print(f"Batting stats: {batting_file}")
    print(f"Pitching stats: {pitching_file}")

    # Print summary statistics
    print(f"\nTotal batting records: {len(batting_df)}")
    print(f"Total pitching records: {len(pitching_df)}")

    config = {
    'csv_path': 'nationals_roster.csv',
    'table_name': 'roster',
    'database': 'MLB_PROJECT',
    'schema': 'SEASON_DATA',
    'account': sf_account,
    'user': 'SJOHNSON',
    'password': sf_password,
    'warehouse': 'COMPUTE_WH'
    }

    success_rows, num_chunks = upload_csv_to_snowflake(**config)

    config = {
    'csv_path': 'nationals_schedule.csv',
    'table_name': 'schedule',
    'database': 'MLB_PROJECT',
    'schema': 'SEASON_DATA',
    'account': sf_account,
    'user': 'SJOHNSON',
    'password': sf_password,
    'warehouse': 'COMPUTE_WH'
    }

    success_rows, num_chunks = upload_csv_to_snowflake(**config)

    config = {
    'csv_path': 'nationals_season_batting.csv',
    'table_name': 'batting_season_stats',
    'database': 'MLB_PROJECT',
    'schema': 'SEASON_DATA',
    'account': sf_account,
    'user': 'SJOHNSON',
    'password': sf_password,
    'warehouse': 'COMPUTE_WH'
    }

    success_rows, num_chunks = upload_csv_to_snowflake(**config)

    config = {
    'csv_path': 'nationals_season_pitching.csv',
    'table_name': 'pitching_season_stats',
    'database': 'MLB_PROJECT',
    'schema': 'SEASON_DATA',
    'account': sf_account,
    'user': 'SJOHNSON',
    'password': sf_password,
    'warehouse': 'COMPUTE_WH'
    }

    success_rows, num_chunks = upload_csv_to_snowflake(**config)

if __name__ == "__main__":
    run_stuff.serve(name="historical-data-grab")
    
    

