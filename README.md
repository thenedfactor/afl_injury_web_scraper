# afl_injury_web_scraper

## Purpose
This codebase is designed to scrape current AFL player injury data from the AFL website (https://www.afl.com.au/matches/injury-list) and upload the results to a local database.

The resulting dataset can then be used to enrich game-by-game data to determine when injuries took place and how long players are injured for. The idea is to then build an automated data pipeline to uncover factors that contribute most significantly to player injury, some of which may be:
- Match venue;
- Player percent of time on ground;
- Player age; and
- Player injury history.

## Method
- Scrape the above website for current player injury data (Python).
- Upload the results to a database table (Python --> SQL).
- Compare injury data to match data to create a dataset containing injury time, type and duration for specific players (SQL).
- Explore the results using visualisation tools (TBC).
- Run ML algorithms on the data to find patterns (Python).
