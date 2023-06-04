library(dplyr)
library(fitzRoy)
library(RODBC)
library(yaml)

conn <- odbcDriverConnect(connection = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=master;Trusted_Connection=yes")
df <- fetch_fixture_afl(season = 2023, comp = "AFLM")

player_stats_df <- fetch_player_stats(season = 2023, source = "fryzigg")
player_details_df <- fetch_player_details(team = "Carlton", current = TRUE, source = "AFL")

print(summary(player_details_df))
print(player_stats_df)

odbcClose(conn)

get_keys <- function() {

    current_directory <- getwd()
    keys <- yaml::yaml.load_file(paste0(current_directory, '/keys.yml'))
    return(keys$database)
}

get_fryzigg_player_stats_snapshot <- function(
    table_name,
    keys,
    competition,
    start_season,
    end_season
) {
    # Retrieves a snapshot of all player match statistics for the specified competition and seasons,
    # and uploads the results to the database specified in the keys.yml file.
    # Currently only configured to upload to a local Microsoft SQL Server.
    # Used to initialise database table. In prod, an incremental-type approach should be used.
    
    # Initialise the dataframe with the start season data
    player_data_df <- fetch_player_stats(season = start_season, comp = competition, source = "fryzigg")
    # Append to the dataframe for following seasons (up to end_season)
    season_after_start_season <- start_season + 1
    for (season in season_after_start_season:end_season) {
        player_data_df <- bind_rows(
            player_data_df,
            fetch_player_stats(season = season, comp = competition, source = "fryzigg")
        )
    }

    # Convert all columns to character type to ensure successful upload
    # TODO: assess proper types and convert here
    player_data_df <- mutate_all(player_data_df, as.character)

    # Establish the database connection
    conn <- odbcDriverConnect(connection = paste0(
        "Driver={ODBC Driver 17 for SQL Server}",
        ";Server=", keys$host,
        ";Database=", keys$database,
        ";Trusted_Connection=yes"
        )
    )

    # Upload the table
    sqlSave(conn, player_data_df, tablename = table_name, rownames = FALSE, append = FALSE)
    odbcClose(conn)
}

print(get_keys()$host)
get_fryzigg_player_stats_snapshot(
    "afl_player_statistics",
    get_keys(),
    "AFLM",
    2021,
    2023
)
