import requests
import pandas as pd
import sqlite3
import random
import time
import os
from bs4 import BeautifulSoup, Comment

columns = ['team_id', 'season', 'games', 'rush_attempts', 'rush_yards', 'rush_tds', 'rush_expected_points', 
           'pass_attempts', 'completions', 'pass_yards',  'pass_tds', 'interceptions', 'passes_defended', 
           'sacks', 'sack_yards', 'qb_hits', 'tfl', 'pass_expected_points']
defense_stats = pd.DataFrame(columns=columns)

os.chdir('/path/to/database/location') #change this to where your SQLite database is located
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

for year in range(1932, 2024):  # This will loop through 1932 to 2023
    url = f"https://www.pro-football-reference.com/years/{year}/opp.htm"
    time.sleep(random.uniform(6, 10))
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    table = soup.select_one('#all_rushing').find_next(string=lambda t: isinstance(t, Comment))
    #the table is in a comment in the html; this locates it
    table = BeautifulSoup(table, 'html.parser')
    print('Pulling team rushing defense data for', year,)
    rush_columns = ['team_id', 'season', 'games', 'rush_attempts', 'rush_yards', 'rush_tds', 'rush_expected_points']
    rushing_stats = pd.DataFrame(columns=rush_columns)
    for tr in table.select('tr'):
        row_data = [td.get_text(strip=True) for td in tr.select('td')]
        if len(row_data) < 1: 
            continue
        team_name = row_data[0]
        if team_name not in team_dict:
            continue
        rush_team_id = team_dict.get(team_name)
        games = row_data[1]
        if len(games) < 1:
            games = None
        rush_attempts = row_data[2]
        if len(rush_attempts) < 1:
            rush_attempts = None
        rush_yards = row_data[3]
        if len(rush_yards) < 1:
            rush_yards = None
        rush_tds = row_data[4]
        if len(rush_tds) < 1:
            rush_tds = None
        try:
            rush_expected_points = row_data[7]
        except:
            rush_expected_points = None
        
        row = {
            'team_id': rush_team_id,
            'season': year,
            'games': games,
            'rush_attempts': rush_attempts,
            'rush_yards': rush_yards,
            'rush_tds': rush_tds,
            'rush_expected_points': rush_expected_points
        }
        rushing_stats = rushing_stats._append(row, ignore_index=True)

    
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    table = soup.select_one('#all_passing').find_next(string=lambda t: isinstance(t, Comment))
    print('Pulling team passing defense data for', year, '\n')
    pass_columns = ['team_id', 'games', 'season', 'pass_attempts', 'completions', 'pass_yards', 
                    'pass_tds', 'interceptions', 'passes_defended', 'sacks', 'sack_yards', 
                    'qb_hits', 'tfl', 'pass_expected_points']
    passing_stats = pd.DataFrame(columns=pass_columns)
    table = BeautifulSoup(table, 'html.parser')
    for tr in table.select('tr'):
        row_data = [td.get_text(strip=True) for td in tr.select('td')]
        if len(row_data) < 1: 
            continue
        
        team_name = row_data[0]
        if team_name not in team_dict: 
            continue
        team_id = team_dict.get(team_name)
        
        games = row_data[1]
        if len(games) < 1:
            games = None
        
        pass_attempts = row_data[3]
        if len(pass_attempts) < 1:
            pass_attempts = None
        
        completions = row_data[2]
        if len(completions) < 1:
            completions = None
        
        pass_yards = row_data[5]
        if len(pass_yards) < 1:
            pass_yards = None

        pass_tds = row_data[6]
        if len(pass_tds) < 1:
            pass_tds = None
    
        interceptions = row_data[8]
        if len(interceptions) < 1:
            interceptions = None

        if len(row_data) > 20:
            passes_defended = row_data[9]
            if len(passes_defended) < 1:
                passes_defended = None
        else:
            passes_defended = None    

        #the tables are different depending on the year
        #everything stat above is located at the same spot in the table for every year
        #The stats below are in different spots depending on the year
        #the if statements below account for that to make sure the proper data is loaded into the database

        if len(row_data) == 20:
            sacks = row_data[15]
            if len(sacks) < 1:
                sacks = None
            sack_yards = row_data[16]
            if len(sack_yards) < 1:
                sack_yards = None
            qb_hits = None
            tfl = None

        if len(row_data) >= 22:
            sacks = row_data[16]
            if len(sacks) < 1:
                sacks = None
            sack_yards = row_data[17]
            if len(sack_yards) < 1:
                sack_yards = None
            
        if len(row_data) == 22 or len(row_data) == 23:
            qb_hits = None
            tfl = row_data[18]
            if len(tfl) < 1:
                tfl = None
        
        if len(row_data) == 23:
            pass_expected_points = row_data[22]
            if len(pass_expected_points) < 1:
                pass_expected_points = None
        else:
            pass_expected_points = None
        
        if len(row_data) == 24:
            qb_hits = row_data[18]
            if len(qb_hits) < 1:
                qb_hits = None
            tfl = row_data[19]
            if len(tfl) < 1:
                tfl = None
            pass_expected_points = row_data[23]
            if len(pass_expected_points) < 1:
                pass_expected_points = None
        
        row = {
            'team_id': team_id,
            'games': games,
            'season': year,
            'pass_attempts': pass_attempts,
            'completions': completions,
            'pass_yards': pass_yards,
            'pass_tds': pass_tds,
            'interceptions': interceptions,
            'passes_defended': passes_defended,
            'sacks': sacks,
            'sack_yards': sack_yards,
            'qb_hits': qb_hits,
            'tfl': tfl,
            'pass_expected_points': pass_expected_points
        }
        passing_stats = passing_stats._append(row, ignore_index=True)
    
    year_combined_stats = pd.merge( #this comnines the rushing_stats and _passing stats dataframe into one dataframe
        rushing_stats, passing_stats,
        on=['team_id', 'season', 'games'],  # Common columns
        how='outer')  # Include all data, even if incomplete
    
    defense_stats = pd.concat([defense_stats, year_combined_stats], ignore_index=True)
    #that combined dataframe is now added to defense_stats
    #at the end of the program all of offense_stats will be added to the sql table

defense_stats = defense_stats.where(pd.notnull(defense_stats), None)
# every None will now be Null in the database

cur.executescript('''DROP TABLE IF EXISTS season_defensive_stats;
                CREATE TABLE season_defensive_stats (
                team_id INTEGER,
                season INTEGER,
                games INTEGER,
                rush_attempts INTEGER,
                rush_yards INTEGER,
                rush_tds INTEGER,
                rush_expected_points REAL,
                pass_attempts INTEGER,
                completions INTEGER,
                pass_yards INTEGER,
                pass_tds INTEGER,
                interceptions INTEGER,
                passes_defended INTEGER,
                sacks INTEGER,
                sack_yards INTEGER,
                qb_hits INTEGER,
                tfl INTEGER, 
                pass_expected_points REAL,
                FOREIGN KEY (team_id) REFERENCES teams(team_id),
                PRIMARY KEY (team_id, season));
                ''')

defense_stats.to_sql('season_defensive_stats', conn, if_exists='append', index=False) #this adds everything to the database in one commit

conn.close()

print('All team rushing and passing defense data has been uploaded to your database!')