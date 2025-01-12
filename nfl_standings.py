import requests
import pandas as pd
import sqlite3
import random
import time
import os
from bs4 import BeautifulSoup, Comment

columns = ('team_id', 'season', 'wins', 'losses', 'ties', 'points_forced', 'points_allowed', 'point_differential')
standings = pd.DataFrame(columns=columns)

os.chdir('path/to/your/database') #change this to where your SQLite database is located
conn = sqlite3.connect('nfldb.sqlite')
cur = conn.cursor()

teams = cur.execute('''SELECT (location || ' ' || name) AS team, id
                    FROM teams
                    UNION ALL
                    SELECT (location || ' ' || name) AS team, current_team_id
                    FROM team_aliases''')

team_dict = dict()
for line in teams:
    team_dict[line[0]] = line[1]

count = 0

for year in range(1932, 2025):
    url = f"https://www.pro-football-reference.com/years/{year}"
    
    print('Grabbing the NFL standings for', year, '\n')
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    tables = soup.find_all("table")
    for table in tables:
        for tr in table.select('tr'):
            th = tr.select_one('th a')
            if th == None:
                continue
            name = str(th).split('<')[1].split('>')[1]
            team_id = team_dict.get(name)
            
            row_data = [td.get_text(strip=True) for td in tr.select('td')]
            wins = int(row_data[0])
            losses = int(row_data[1])
            if len(row_data) == 11:
                ties = 0
                points_forced = int(row_data[3])
                points_allowed =int(row_data[4])
            if len(row_data) == 12:
                ties = int((row_data)[2])
                points_forced = int(row_data[4])
                points_allowed =int(row_data[5])
            point_differential = points_forced - points_allowed
            row = {
                'team_id': team_id,
                'season': year,
                'wins': wins,
                'losses': losses,
                'ties': ties,
                'points_forced': points_forced,
                'points_allowed': points_allowed,
                'point_differential': point_differential
            }
            standings = pd.concat([standings, pd.DataFrame([row])], ignore_index=True)

        
    print ('Pausing to obey rate limits.\n')
    time.sleep(random.uniform(7, 10))

cur.executescript('''
        DROP TABLE IF EXISTS season_records;

        CREATE TABLE season_records (
        team_id INTEGER,
        season INTEGER,
        wins INTEGER,
        losses INTEGER,
        ties INTEGER,
        points_forced INTEGER,
        points_allowed INTEGER,
        point_differential INTEGER,
        FOREIGN KEY (team_id) REFERENCES teams(team_id),
        PRIMARY KEY (team_id, season))''')

print('Importing all standings to the season_records table now!\n')
standings.to_sql('season_records', conn, if_exists='append', index=False)
conn.commit
conn.close

print('All standings have been imported!')
