import os
import json
import sqlite3
import requests
from zipfile import ZipFile
from io import BytesIO

def create_tables(conn):
    c = conn.cursor()
    
    # Create table for match results
    c.execute('''CREATE TABLE IF NOT EXISTS match_results (
                    match_id INTEGER PRIMARY KEY,
                    match_date TEXT,
                    city TEXT,
                    event_name TEXT,
                    match_number INTEGER,
                    gender TEXT,
                    match_type TEXT,
                    match_type_number INTEGER,
                    winner TEXT,
                    winning_margin INTEGER,
                    overs INTEGER,
                    season TEXT,
                    team_type TEXT,
                    toss_decision TEXT,
                    toss_winner TEXT,
                    venue TEXT
                )''')

    # Create table for innings data
    c.execute('''CREATE TABLE IF NOT EXISTS innings_data (
                    match_id INTEGER,
                    team TEXT,
                    over INTEGER,
                    batter TEXT,
                    bowler TEXT,
                    non_striker TEXT,
                    batter_runs INTEGER,
                    extras INTEGER,
                    total_runs INTEGER,
                    FOREIGN KEY (match_id) REFERENCES match_results(match_id)
                )''')

    # Create table for player registry
    c.execute('''CREATE TABLE IF NOT EXISTS player_registry (
                    id TEXT PRIMARY KEY,
                    name TEXT
                )''')

    conn.commit()
    print("Tables created successfully")

def insert_match_results(conn, match_data):
    c = conn.cursor()
    info = match_data.get("info", {})
    event = info.get("event", {})
    match_number = event.get("match_number")
    winner = info.get("outcome", {}).get("winner", "Unknown")
    outcome = info.get("outcome", {})
    winning_margin = outcome.get("by", {}).get("wickets") if "by" in outcome else None
    c.execute('''INSERT INTO match_results (
                    match_date, city, event_name, match_number, gender, 
                    match_type, match_type_number, winner, winning_margin, 
                    overs, season, team_type, toss_decision, toss_winner, venue
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    info.get("dates", ["Unknown"])[0],
                    info.get("city", "Unknown"),
                    event.get("name", "Unknown"),
                    match_number,
                    info.get("gender"),
                    info.get("match_type"),
                    info.get("match_type_number"),
                    winner,
                    winning_margin,
                    info.get("overs"),
                    info.get("season"),
                    info.get("team_type"),
                    info.get("toss", {}).get("decision"),
                    info.get("toss", {}).get("winner"),
                    info.get("venue")
                ))
    conn.commit()

def insert_innings_data(conn, match_id, innings_data):
    c = conn.cursor()
    for inning in innings_data:
        team = inning["team"]
        overs = inning["overs"]
        for over_data in overs:
            over_number = over_data["over"]
            deliveries = over_data["deliveries"]
            for delivery in deliveries:
                batter = delivery["batter"]
                bowler = delivery["bowler"]
                non_striker = delivery["non_striker"]
                batter_runs = delivery["runs"]["batter"]
                extras = delivery["runs"]["extras"]
                total_runs = delivery["runs"]["total"]
                c.execute('''INSERT INTO innings_data (
                                match_id, team, over, batter, bowler, 
                                non_striker, batter_runs, extras, total_runs
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (match_id, team, over_number, batter, bowler, non_striker, batter_runs, extras, total_runs))
    conn.commit()

def process_json_files_from_zip(zip_url, conn):
    response = requests.get(zip_url)
    file_counter = 1
    if response.status_code == 200:
        zip_file = ZipFile(BytesIO(response.content))
        for file in zip_file.namelist():
            if file.endswith(".json"):
                file_counter += 1
                with zip_file.open(file) as json_file:
                    match_data = json.load(json_file)
                    insert_match_results(conn, match_data)
                    match_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    insert_innings_data(conn, match_id, match_data["innings"])
        print("Data processed successfully.")
    else:
        print("Failed to fetch data from the URL.")
    return file_counter

def main():
    db_name = "cricket.db"
    conn = sqlite3.connect(db_name)
    print("Connected to SQLite database")

    create_tables(conn)
    
    # Specify the URL from which to fetch the ZIP file containing JSON data
    male_zip_url = "https://cricsheet.org/downloads/odis_male_json.zip"
    female_zip_url = "https://cricsheet.org/downloads/odis_female_json.zip"
    
    # Process JSON data from the ZIP files and get the count of files processed
    male_files_loaded = process_json_files_from_zip(male_zip_url, conn)
    female_files_loaded = process_json_files_from_zip(female_zip_url, conn)

    print(f"Total {male_files_loaded} male files and {female_files_loaded} female files processed.")

    conn.close()
    print("Connection to SQLite database closed")

if __name__ == "__main__":
    main()
