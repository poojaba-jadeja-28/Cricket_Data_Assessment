import sqlite3

def highest_strike_rate_batsmen(conn):
    query = """
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
            10;
    """

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

def main():
    db_name = "cricket.db"
    conn = sqlite3.connect(db_name)
    print("Connected to SQLite database")

    top_batsmen = highest_strike_rate_batsmen(conn)
    print("Top Batsmen with Highest Strike Rate in 2019:")
    for player in top_batsmen:
        print(player)

    conn.close()
    print("Connection to SQLite database closed")

if __name__ == "__main__":
    main()
