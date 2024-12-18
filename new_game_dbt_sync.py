import statsapi
from prefect import flow, task
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import requests
import time
from typing import Optional, Dict
import json
import pandas as pd
import logging
from enum import Enum
from datetime import datetime
from prefect.blocks.system import Secret

secret_block_sf_password = Secret.load("snowflake-password")
secret_block_sf_account = Secret.load("snowflake-account")
secret_block_dbt_api = Secret.load("dbt-api-key")

dbt_api = secret_block_dbt_api.get()
sf_password = secret_block_sf_password.get()
sf_account = secret_block_sf_account.get()


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
def process_single_game_boxscore(game_id=744798):
    # Get the boxscore data
    boxscore = statsapi.boxscore_data(game_id)
    
    # Initialize lists for batting and pitching data
    all_batting = []
    all_pitching = []
    
    
    
    # Process home and away teams
    for team_type in ['home', 'away']:
        all_batting.extend(process_batting_stats(boxscore, game_id, team_type))
        all_pitching.extend(process_pitching_stats(boxscore, game_id, team_type))
    
    # Convert to DataFrames
    batting_df = pd.DataFrame(all_batting)
    pitching_df = pd.DataFrame(all_pitching)
    
    # Save to CSV files
    batting_filename = f"batting_{game_id}.csv"
    pitching_filename = f"pitching_{game_id}.csv"
    
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


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DbtCloudJobStatus(Enum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30

    @classmethod
    def get_status_name(cls, status_code: int) -> str:
        try:
            return cls(status_code).name
        except ValueError:
            return f"UNKNOWN({status_code})"

class DbtRunError(Exception):
    """Custom exception for dbt run errors"""
    def __init__(self, status: str, message: str, run_details: Dict):
        self.status = status
        self.message = message
        self.run_details = run_details
        super().__init__(f"dbt run failed with status {status}: {message}")

class DbtCloudClient:
    def __init__(self, api_token: str, account_id: str, raise_on_error: bool = True):
        """
        Initialize the dbt Cloud client
        
        Args:
            api_token: Your dbt Cloud API token
            account_id: Your dbt Cloud account ID
            raise_on_error: If True, raises exception on non-success status
        """
        self.base_url = "https://cloud.getdbt.com/api/v2"
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json"
        }
        self.account_id = account_id
        self.raise_on_error = raise_on_error

    @task
    def trigger_job(self, job_id: int, cause: str = "API trigger") -> dict:
        """
        Trigger a dbt Cloud job
        
        Args:
            job_id: The ID of the job to trigger
            cause: A description of why the job was triggered
            
        Returns:
            dict: Response from the API containing the run details
        """
        url = f"{self.base_url}/accounts/{self.account_id}/jobs/{job_id}/run/"
        payload = {
            "cause": cause
        }
        
        logger.info(f"Triggering job {job_id}")
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        
        run_data = response.json()
        run_id = run_data['data']['id']
        logger.info(f"Job triggered successfully. Run ID: {run_id}")
        return run_data

    @task
    def get_run_status(self, run_id: int) -> dict:
        """
        Get the status of a specific run
        
        Args:
            run_id: The ID of the run to check
            
        Returns:
            dict: Response from the API containing the run status
        """
        url = f"{self.base_url}/accounts/{self.account_id}/runs/{run_id}/"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    @task
    def get_run_artifacts(self, run_id: int) -> dict:
        """
        Get the artifacts (logs, etc.) from a run
        
        Args:
            run_id: The ID of the run
            
        Returns:
            dict: Response containing run artifacts
        """
        url = f"{self.base_url}/accounts/{self.account_id}/runs/{run_id}/artifacts/"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    @task
    def format_run_details(self, run_data: Dict) -> Dict:
        """
        Format run details into a more readable structure
        """
        data = run_data['data']
        status_code = data.get('status', 0)
        
        details = {
            'run_id': data.get('id'),
            'status_code': status_code,
            'status': DbtCloudJobStatus.get_status_name(status_code),
            'started_at': data.get('started_at'),
            'finished_at': data.get('finished_at'),
            'duration_seconds': self._calculate_duration(data.get('started_at'), data.get('finished_at')),
            'environment_id': data.get('environment_id'),
            'dbt_version': data.get('dbt_version'),
            'job_id': data.get('job_id'),
            'error_msg': data.get('status_message'),
            'is_error': status_code == DbtCloudJobStatus.ERROR.value,
            'is_cancelled': status_code == DbtCloudJobStatus.CANCELLED.value
        }

        # Add additional error details if available
        if details['is_error']:
            try:
                artifacts = self.get_run_artifacts(details['run_id'])
                details['error_logs'] = artifacts.get('data', {}).get('logs', 'No logs available')
            except:
                details['error_logs'] = 'Failed to fetch error logs'

        return details

    @task
    def _calculate_duration(self, started_at: str, finished_at: str) -> Optional[float]:
        """Calculate the duration of a run in seconds"""
        if not started_at or not finished_at:
            return None
        
        try:
            start = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
            end = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
            return (end - start).total_seconds()
        except (ValueError, TypeError):
            return None

    @task
    def wait_for_job_completion(self, run_id: int, polling_interval: int = 30, timeout: Optional[int] = None) -> dict:
        """
        Wait for a job to complete, polling at specified intervals
        
        Args:
            run_id: The ID of the run to monitor
            polling_interval: Seconds to wait between status checks
            timeout: Maximum seconds to wait for completion (None for no timeout)
            
        Returns:
            dict: Final run status
        Raises:
            DbtRunError: If run fails and raise_on_error is True
        """
        start_time = time.time()
        completed_statuses = {DbtCloudJobStatus.SUCCESS.value, DbtCloudJobStatus.ERROR.value, DbtCloudJobStatus.CANCELLED.value}
        
        while True:
            try:
                status_response = self.get_run_status(run_id)
                run_details = self.format_run_details(status_response)
                status_code = status_response['data']['status']
                
                logger.info(f"Current status: {run_details['status']} ({status_code})")
                
                if status_code in completed_statuses:
                    if status_code != DbtCloudJobStatus.SUCCESS.value:
                        error_msg = run_details.get('error_msg', 'No error message available')
                        logger.error(f"Job failed with status {run_details['status']}: {error_msg}")
                        
                        if self.raise_on_error:
                            raise DbtRunError(run_details['status'], error_msg, run_details)
                    
                    return run_details
                
                if timeout and (time.time() - start_time > timeout):
                    raise TimeoutError(f"Job did not complete within {timeout} seconds")
                
                logger.info(f"Job still running. Waiting {polling_interval} seconds before next check...")
                time.sleep(polling_interval)
                
            except DbtRunError:
                raise
            except Exception as e:
                logger.error(f"Error checking run status: {e}")
                raise

@flow(log_prints=True)
def new_game_data_and_dbt_run():
    
    #Grab boxscore of new game
    batting_file, pitching_file, batting_df, pitching_df = process_single_game_boxscore()
    print(f"\nFiles created:")
    print(f"Batting stats: {batting_file}")
    print(f"Pitching stats: {pitching_file}")

    # Print summary statistics
    print(f"\nTotal batting records: {len(batting_df)}")
    print(f"Total pitching records: {len(pitching_df)}")
    
    #upload data to snowflake
    config = {
    'csv_path': 'batting_744798.csv',
    'table_name': 'batting_last_game',
    'database': 'MLB_PROJECT',
    'schema': 'SEASON_DATA',
    'account': sf_account,
    'user': 'SJOHNSON',
    'password': sf_password,
    'warehouse': 'COMPUTE_WH'
    }

    success_rows, num_chunks = upload_csv_to_snowflake(**config)

    #upload data to snowflake
    config = {
    'csv_path': 'pitching_744798.csv',
    'table_name': 'pitching_last_game',
    'database': 'MLB_PROJECT',
    'schema': 'SEASON_DATA',
    'account': sf_account,
    'user': 'SJOHNSON',
    'password': sf_password,
    'warehouse': 'COMPUTE_WH'
    }

    success_rows, num_chunks = upload_csv_to_snowflake(**config)
    
    
    # run dbt job 
    api_token = dbt_api
    account_id = "36394"  # Your account ID
    job_id = 778141      # Your job ID
    

    client = DbtCloudClient(api_token, account_id, raise_on_error=True)
    
    try:
        # Trigger the job
        run_response = client.trigger_job(job_id, cause="Testing job trigger")
        run_id = run_response['data']['id']
        
        # Wait for completion with a 10-minute timeout
        final_status = client.wait_for_job_completion(run_id, polling_interval=30, timeout=600)
        
        # Print final status details
        print("\nFinal job details:")
        print(f"Run ID: {final_status['run_id']}")
        print(f"Status: {final_status['status']}")
        print(f"Started at: {final_status['started_at']}")
        print(f"Finished at: {final_status['finished_at']}")
        if final_status['duration_seconds']:
            print(f"Duration: {final_status['duration_seconds']:.1f} seconds")
        print(f"dbt version: {final_status['dbt_version']}")
        
        # Print error details if present
        if final_status['is_error']:
            print("\nError Details:")
            print(f"Error message: {final_status['error_msg']}")
            print("\nError logs:")
            print(final_status['error_logs'])
        elif final_status['is_cancelled']:
            print("\nJob was cancelled")
        
    except DbtRunError as e:
        logger.error(f"dbt run failed: {e.message}")
        logger.error(f"Run details: {e.run_details}")
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
    except TimeoutError as e:
        logger.error(f"Timeout error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")



def main():
    new_game_data_and_dbt_run()

if __name__ == "__main__":
    main()