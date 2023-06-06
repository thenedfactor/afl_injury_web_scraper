from web_scraper_functions import *

keys = get_keys()
df = get_latest_injuries_df("https://www.afl.com.au/matches/injury-list")
upload_data_to_db(
    df,
    db_host=keys['db_host'],
    db_name=keys['db_name'],
    table_name='afl_player_injuries',
    schema='dbo'
)
