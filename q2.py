import sqlite3

def get_highest_win_percentage(conn, year, gender):
    c = conn.cursor()
    c.execute('''
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
                    strftime('%Y', match_date) = ? 
                    AND gender = ?
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
                    strftime('%Y', match_date) = ? 
                    AND gender = ?
                GROUP BY 
                    gender
            ) TM ON TW.gender = TM.gender
        ) AS WinPercentages
        WHERE 
            rank = 1;
    ''', (year, gender, year, gender))
    result = c.fetchone()
    return result[1], result[2]

def main():
    db_file = "cricket.db"
    conn = sqlite3.connect(db_file)
    print("Connected to SQLite database")

    year = "2019"
    male_highest_team, male_highest_percentage = get_highest_win_percentage(conn, year, "male")
    female_highest_team, female_highest_percentage = get_highest_win_percentage(conn, year, "female")

    print(f"Highest win percentage for male teams in {year}: {male_highest_team} ({male_highest_percentage:.2f}%)")
    print(f"Highest win percentage for female teams in {year}: {female_highest_team} ({female_highest_percentage:.2f}%)")

    conn.close()
    print("Connection to SQLite database closed")

if __name__ == "__main__":
    main()
