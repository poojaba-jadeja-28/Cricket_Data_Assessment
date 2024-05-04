------------------question-A------------------------------------
SELECT strftime('%Y', match_date) AS year, gender, winner, COUNT(*) AS total_matches
FROM match_results
WHERE winner != 'Unknown' 
    AND winner NOT LIKE '%No result%' 
    AND winner NOT LIKE '%tied%' 
    AND winner NOT LIKE '%DLS%'
GROUP BY year, gender, winner
order  by winner,gender;


--------------------question-B------------------------------------
SELECT 
    gender,
    team,
    win_percentage
FROM (
    SELECT 
        TW.gender,
        TW.team,
        (TW.total_wins * 100.0) / TM.total_matches AS win_percentage,
        RANK() OVER (PARTITION BY TW.gender ORDER BY (TW.total_wins * 100.0) / TM.total_matches DESC) AS rank
    FROM (
        SELECT 
            gender,
            winner AS team,
            COUNT(*) AS total_wins
        FROM 
            match_results
        WHERE 
            strftime('%Y', match_date) = '2019'
            AND gender IN ('male', 'female')
            AND winner != 'Unknown'
            AND winner NOT LIKE '%No result%'
            AND winner NOT LIKE '%tied%'
            AND winner NOT LIKE '%DLS%'
        GROUP BY 
            gender, winner
    ) TW
    JOIN (
        SELECT 
            gender,
            COUNT(*) AS total_matches
        FROM 
            match_results
        WHERE 
            strftime('%Y', match_date) = '2019'
            AND gender IN ('male', 'female')
        GROUP BY 
            gender
    ) TM ON TW.gender = TM.gender
) AS WinPercentages
WHERE 
    rank = 1;
	

--------------------question-C------------------------------------	
SELECT
    player_name,
    total_runs,
    total_balls_faced,
    (total_runs_scored / total_balls_faced_float) * 100 AS strike_rate
FROM (
    SELECT
        md.batter AS player_name,
        COUNT(*) AS total_runs,
        COUNT(CASE WHEN md.extras = 0 THEN 1 END) AS total_balls_faced,
        SUM(CASE WHEN md.extras = 0 THEN md.batter_runs ELSE 0 END) AS total_runs_scored,
        (COUNT(CASE WHEN md.extras = 0 THEN 1 END) * 1.0) AS total_balls_faced_float
    FROM
        match_results mr
    JOIN
        innings_data md ON mr.match_id = md.match_id
    WHERE
        strftime('%Y', mr.match_date) = '2019'
    GROUP BY
        md.batter
) AS PlayerStats
ORDER BY
    strike_rate DESC
LIMIT
    1;

