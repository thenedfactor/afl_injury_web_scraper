import requests
from bs4 import BeautifulSoup
import pandas as pd
from os import getcwd
import yaml
from sqlalchemy import create_engine, String, Date, TIMESTAMP, DateTime
import pyodbc
from datetime import datetime, timezone

def get_keys():

    folder_path = getcwd()

    # Load the YAML file into a Python dictionary
    with open(folder_path + r"\keys.yml", "r") as f:
        config = yaml.safe_load(f)

    # Access the database host and port
    db_host = config["database"]["host"]
    db_name = config["database"]["database"]

    return {
        'db_host': db_host,
        'db_name': db_name
    }

def get_latest_injuries_df():
    '''
    Accesses the current AFL injury list page (URL below), scrapes player injury data from the
    HTML response, and stores the data in a Pandas dataframe. Returns the dataframe containing
    player injury information.
    '''

    # URL of the AFL injury list
    url = "https://www.afl.com.au/matches/injury-list"

    # Send a GET request to the URL
    response = requests.get(url)

    # Create BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table containing the injury data
    tables = soup.find_all("table")

    # Initialize lists to store player names and injury details
    player_names = []
    injury_details = []
    estimated_return_times = []
    updated_dates = []

    for table in tables:
        # First, get the updated date from the last row of each table
        updated_date = table.find_all("tr")[-1].find_all("td")[0].text.strip().split(': ')[1]
        # Iterate over each row in the table (excluding the header row)
        for row in table.find_all("tr")[1:-1]:
            # Get player name from the first column
            cells = row.find_all("td")
            player_name = cells[0].text.strip()
            player_names.append(player_name)
            
            # Get injury details from the second column
            injury_detail = cells[1].text.strip()
            injury_details.append(injury_detail)

            # Get estimated return time from the third column
            estimated_return_time = cells[2].text.strip()
            estimated_return_times.append(estimated_return_time)

            # Finally, add the updated date for each entry
            updated_dates.append(updated_date)
        
    # Now we can create a dataframe containing all the information
    df = pd.DataFrame()

    df["player"] = player_names
    df["injury"] = injury_details
    df["estimated_return_time"] = estimated_return_times
    df["updated_date"] = updated_dates

    # Convert the updated_date column into an actual date type
    df["updated_date"] = pd.to_datetime(df['updated_date'], format='%B %d, %Y')

    # Add a column with DB upload timestamp
    df["upload_timestamp"] = pd.to_datetime(datetime.now(timezone.utc))

    return df

def upload_data_to_db(
    df,
    db_host,
    db_name,
    table_name,
    schema
):
    engine = create_engine(
            'mssql+pyodbc://{}/{}?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes'.format(db_host, db_name)
        )

    df.to_sql(
        table_name,
        schema=schema,
        if_exists='append',
        con=engine,
        chunksize=2048,
        dtype={
            'player': String,
            'injury': String,
            'estimated_return_time': String,
            'updated_date': Date,
            'upload_timestamp': DateTime
        }
    )

    return None
if __name__ == '__main__':

    keys = get_keys()
    df = get_latest_injuries_df()
    print(df.head())
    print(df.info())
    upload_data_to_db(
        df,
        db_host=keys['db_host'],
        db_name=keys['db_name'],
        table_name='afl_player_injuries',
        schema='dbo'
    )
