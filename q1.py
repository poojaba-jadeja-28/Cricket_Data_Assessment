import sqlite3

def main():
    db_file = "cricket.db"
    conn = sqlite3.connect(db_file)
    print("Connected to SQLite database")

    c = conn.cursor()
    c.execute('''SELECT strftime('%Y', match_date) AS year, gender, winner, COUNT(*) AS total_matches
                 FROM match_results
                 WHERE winner != 'Unknown' 
                     AND winner NOT LIKE '%No result%' 
                     AND winner NOT LIKE '%tied%' 
                     AND winner NOT LIKE '%DLS%'
                 GROUP BY year, gender, winner''')
    rows = c.fetchall()

    print("Year | Gender | Winner | Total Matches")
    print("---------------------------------------")

    for row in rows:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")

    conn.close()
    print("Connection to SQLite database closed")

if __name__ == "__main__":
    main()
