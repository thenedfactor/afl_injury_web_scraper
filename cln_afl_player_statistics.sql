-- Create a new table called '[TableName]' in schema '[dbo]'
-- Drop the table if it already exists
IF OBJECT_ID('[dbo].[cln_afl_player_statistics]', 'U') IS NOT NULL
DROP TABLE [dbo].[cln_afl_player_statistics]
GO
-- Create the table in the specified schema
CREATE TABLE [dbo].[cln_afl_player_statistics]
(
	   [venue_name] VARCHAR(255)
      ,[match_id] VARCHAR(255)
      ,[match_date] DATE
      ,[match_local_time] VARCHAR(255)
	  ,[match_start_timestamp] DATETIME
      ,[match_round] VARCHAR(255)
      ,[match_weather_temp_c] INT
      ,[match_weather_type] VARCHAR(255)
      ,[player_id] VARCHAR(255)
      ,[player_first_name] VARCHAR(255)
      ,[player_last_name] VARCHAR(255)
	  ,[player_full_name] VARCHAR(255)
	  ,[player_team] VARCHAR(255)
      ,[player_height_cm] INT
      ,[player_weight_kg] INT
      ,[kicks] INT
      ,[marks] INT
      ,[handballs] INT
      ,[disposals] INT
      ,[effective_disposals] INT
      ,[disposal_efficiency_percentage] FLOAT
      ,[goals] INT
      ,[behinds] INT
      ,[hitouts] INT
      ,[tackles] INT
      ,[rebounds] INT
      ,[inside_fifties] INT
      ,[clearances] INT
      ,[clangers] INT
      ,[free_kicks_for] INT
      ,[free_kicks_against] INT
      ,[brownlow_votes] INT
      ,[contested_possessions] INT
      ,[uncontested_possessions] INT
      ,[contested_marks] INT
      ,[marks_inside_fifty] INT
      ,[one_percenters] INT
      ,[bounces] INT
      ,[goal_assists] INT
      ,[time_on_ground_percentage] INT
      ,[afl_fantasy_score] INT
      ,[supercoach_score] INT
      ,[centre_clearances] INT
      ,[stoppage_clearances] INT
      ,[score_involvements] INT
      ,[metres_gained] INT
      ,[turnovers] INT
      ,[intercepts] INT
      ,[tackles_inside_fifty] INT
      ,[contest_def_losses] INT
      ,[contest_def_one_on_ones] INT
      ,[contest_off_one_on_ones] INT
      ,[contest_off_wins] INT
      ,[def_half_pressure_acts] INT
      ,[effective_kicks] INT
      ,[f50_ground_ball_gets] INT
      ,[ground_ball_gets] INT
      ,[hitouts_to_advantage] INT
      ,[hitout_win_percentage] FLOAT
      ,[intercept_marks] INT
      ,[marks_on_lead] INT
      ,[pressure_acts] INT
      ,[rating_points] FLOAT
      ,[ruck_contests] INT
      ,[score_launches] INT
      ,[shots_at_goal] INT
      ,[spoils] INT
      ,[subbed] BIT
      ,[player_position] VARCHAR(255)
);
INSERT INTO [dbo].[cln_afl_player_statistics]
    SELECT
        CAST([venue_name] AS VARCHAR(255)) venue_name
      ,CAST([match_id] AS VARCHAR(255)) match_id
      ,CAST([match_date] AS DATE) match_date
      ,CAST([match_local_time] AS VARCHAR(255)) match_local_time
	  ,CAST(CONCAT([match_date], ' ', [match_local_time]) AS DATETIME) match_start_timestamp
      ,[match_round]
      ,CAST([match_weather_temp_c] AS INT)
      ,[match_weather_type]
      ,[player_id]
      ,[player_first_name]
      ,[player_last_name]
	  ,CONCAT([player_first_name], ' ', [player_last_name]) player_full_name
	  ,[player_team]
      ,CAST([player_height_cm] AS INT)
      ,CAST([player_weight_kg] AS INT) 
      ,CAST([kicks] AS INT) 
      ,CAST([marks] AS INT) 
      ,CAST([handballs] AS INT) 
      ,CAST([disposals] AS INT) 
      ,CAST([effective_disposals] AS INT) 
      ,ROUND(CAST([effective_disposals] AS FLOAT)/CAST((CASE WHEN [disposals] = '0' THEN NULL ELSE [disposals] END) AS FLOAT), 4) disposal_efficiency_percentage
      ,CAST([goals] AS INT) 
      ,CAST([behinds] AS INT) 
      ,CAST([hitouts] AS INT) 
      ,CAST([tackles] AS INT) 
      ,CAST([rebounds] AS INT) 
      ,CAST([inside_fifties] AS INT) 
      ,CAST([clearances] AS INT) 
      ,CAST([clangers] AS INT) 
      ,CAST([free_kicks_for] AS INT) 
      ,CAST([free_kicks_against] AS INT) 
      ,CAST([brownlow_votes] AS INT) 
      ,CAST([contested_possessions] AS INT) 
      ,CAST([uncontested_possessions] AS INT) 
      ,CAST([contested_marks] AS INT) 
      ,CAST([marks_inside_fifty] AS INT) 
      ,CAST([one_percenters] AS INT) 
      ,CAST([bounces] AS INT) 
      ,CAST([goal_assists] AS INT) 
      ,CAST([time_on_ground_percentage] AS INT) 
      ,CAST([afl_fantasy_score] AS INT) 
      ,CAST([supercoach_score] AS INT) 
      ,CAST([centre_clearances] AS INT) 
      ,CAST([stoppage_clearances] AS INT) 
      ,CAST([score_involvements] AS INT) 
      ,CAST([metres_gained] AS INT) 
      ,CAST([turnovers] AS INT) 
      ,CAST([intercepts] AS INT) 
      ,CAST([tackles_inside_fifty] AS INT) 
      ,CAST([contest_def_losses] AS INT) 
      ,CAST([contest_def_one_on_ones] AS INT) 
      ,CAST([contest_off_one_on_ones] AS INT) 
      ,CAST([contest_off_wins] AS INT) 
      ,CAST([def_half_pressure_acts] AS INT) 
      ,CAST([effective_kicks] AS INT) 
      ,CAST([f50_ground_ball_gets] AS INT) 
      ,CAST([ground_ball_gets] AS INT) 
      ,CAST([hitouts_to_advantage] AS INT) 
      ,CAST([hitout_win_percentage] AS FLOAT)/100 hitout_win_percentage 
      ,CAST([intercept_marks] AS INT) 
      ,CAST([marks_on_lead] AS INT) 
      ,CAST([pressure_acts] AS INT) 
      ,CAST([rating_points] AS FLOAT)
      ,CAST([ruck_contests] AS INT) 
      ,CAST([score_launches] AS INT) 
      ,CAST([shots_at_goal] AS INT) 
      ,CAST([spoils] AS INT) 
      ,CAST((CASE WHEN [subbed] = 'TRUE' THEN 1 ELSE 0 END) AS BIT) subbed
      ,CAST([player_position] AS VARCHAR(255)) player_position
    FROM [dbo].[afl_player_statistics]

GO