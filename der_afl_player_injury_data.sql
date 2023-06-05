-- Create a new table called '[TableName]' in schema '[dbo]'
-- Drop the table if it already exists
IF OBJECT_ID('[dbo].[der_afl_player_injury_data]', 'U') IS NOT NULL
DROP TABLE [dbo].[der_afl_player_injury_data]
GO
-- Create the table in the specified schema
CREATE TABLE [dbo].[der_afl_player_injury_data]
(
	[player] VARCHAR(255),
	[player_team] VARCHAR(255),
	[return_date] DATE,
	[last_match_date_before_injury] DATE,
	[return_round] INT,
	[days_between_matches] INT,
	[rounds_missed] INT,
	[injury_group] INT,
	[injury] VARCHAR(255),
	[min_updated_date] DATE,
	[max_updated_date] DATE
);
WITH injuries AS (
	SELECT
		  [player]
		  ,[injury]
		  ,[estimated_return_time]
		  ,[updated_date]
		  ,[upload_timestamp]
		  ,LAG([updated_date], 1) OVER (PARTITION BY [player], [injury] ORDER BY [upload_timestamp] ASC) lag_updated_date
		  ,LEAD([updated_date], 1) OVER (PARTITION BY [player], [injury] ORDER BY [upload_timestamp] ASC) lead_updated_date
	FROM [dbo].[afl_player_injuries]
)
, flag_cte AS (
	SELECT
		*
		, CASE
			WHEN ([lag_updated_date] IS NULL) THEN 1
			WHEN (DATEDIFF(DAY, [lag_updated_date], [updated_date]) >= 14) THEN 1 
			ELSE 0 
		END new_injury_flag
		, DATEDIFF(DAY, [lag_updated_date], [updated_date]) days_between_injury_entries
	FROM injuries
)
-- SELECT * FROM flag_cte
, injury_group_cte AS (
	SELECT
		*,
		SUM([new_injury_flag]) OVER (
			PARTITION BY [player], [injury] ORDER BY [updated_date] ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) injury_group
	FROM flag_cte
)
--SELECT * FROM injury_group_cte
, injury_grouped_data AS (
	SELECT
		[injury_group],
		[player],
		[injury],
		MIN([updated_date]) min_updated_date,
		MAX([updated_date]) max_updated_date
	FROM injury_group_cte
	GROUP BY [injury_group], [player], [injury]
)
--SELECT * FROM injury_grouped_data
--WITH prepare_player_stats AS (
, prepare_player_stats AS (
	SELECT 
		[player_full_name],
		[player_team],
		[match_date],
		[match_round],
		LAG([match_date], 1) OVER (PARTITION BY [player_full_name] ORDER BY [match_date] ASC) lag_match_date,
		MAX(
			CASE WHEN ISNUMERIC([match_round]) = 1 THEN CAST([match_round] AS INT) ELSE NULL END
		) OVER (PARTITION BY YEAR([match_date])) last_round_of_season
	FROM [dbo].[cln_afl_player_statistics]
)
, player_stats AS (
	SELECT
		[player_full_name],
		[player_team],
		[match_date],
		[lag_match_date],
		DATEDIFF(DAY, [lag_match_date], [match_date]) days_between_matches,
		CASE
			WHEN [match_round] = 'Finals Week 1' THEN [last_round_of_season] + 1
			WHEN [match_round] = 'Semi Finals' THEN [last_round_of_season] + 2
			WHEN [match_round] = 'Preliminary Finals' THEN [last_round_of_season] + 3
			WHEN [match_round] = 'Grand Final' THEN [last_round_of_season] + 4
			ELSE [match_round]
		END cln_match_round,
		YEAR([match_date]) season
	FROM prepare_player_stats
	WHERE [match_date] >= '2023-01-01'
)
--SELECT * FROM player_stats
, lag_round_cte AS (
	SELECT
		*,
		LAG([cln_match_round], 1) OVER (PARTITION BY [player_full_name], [season] ORDER BY [match_date] ASC) lag_match_round,
		LAG([season], 1) OVER (PARTITION BY [player_full_name] ORDER BY [match_date] ASC) lag_season
	FROM player_stats
)
--SELECT * FROM lag_round_cte
, flag_missing_games_cte AS (
	SELECT
		*,
		CASE
			WHEN [cln_match_round] > [lag_match_round] + 1 THEN 1
			ELSE 0
		END missing_games_flag
	FROM lag_round_cte
)
--SELECT * FROM flag_missing_games_cte
, filtered_cte AS (
	SELECT
		[player_full_name],
		[player_team],
		[lag_match_date] last_match_date_before_injury,
		[match_date] return_date,
		[days_between_matches],
		[cln_match_round] - [lag_match_round] + 1 rounds_missed,
		[cln_match_round] return_round,
		[season]
	FROM flag_missing_games_cte
	WHERE [missing_games_flag] = 1
)
--SELECT * FROM filtered_cte
, joined_cte AS (
	SELECT
		injuries.player,
		matches.player_team,
		matches.return_date,
		matches.last_match_date_before_injury,
		matches.return_round,
		matches.days_between_matches,
		matches.rounds_missed,
		injuries.injury_group,
		injuries.injury,
		injuries.min_updated_date,
		injuries.max_updated_date
	FROM filtered_cte matches
	INNER JOIN injury_grouped_data injuries
	ON (
		(matches.player_full_name = injuries.player) AND
		(matches.last_match_date_before_injury < injuries.min_updated_date) AND
		(matches.return_date > injuries.max_updated_date)
	)
)
INSERT INTO [dbo].[der_afl_player_injury_data]
SELECT * FROM joined_cte
GO

--SELECT DISTINCT [match_round] FROM [dbo].[cln_afl_player_statistics]