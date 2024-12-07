print('getting offensive values from pro-football-reference')

import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import sqlite3
import random
import time
import os

columns = ['team_id', 'season', 'games', 'rush_attempts', 'rush_yards', 'rush_tds', 'fumbles', 'rush_expected_points', 
           'pass_attempts', 'completions', 'pass_yards', 'pass_tds', 'interceptions', 'sacks', 'sack_yards', 'fourth_quarter_comebacks', 'game_winning_drives', 'pass_expected_points']
offense_stats = pd.DataFrame(columns=columns)

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
    url = f"https://www.pro-football-reference.com/years/{year}/"
    time.sleep(random.uniform(7, 11)) #this obeys pro-football-reference's rate limits and make the traffic random to appear more human
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    table = soup.select_one('#all_rushing').find_next(string=lambda t: isinstance(t, Comment))
    #the table is in a comment in the html; this locates it
    print('Pulling rushing data for', year)
    rush_columns = ['team_id', 'season', 'games', 'rush_attempts', 'rush_yards', 'rush_tds', 'fumbles', 'rush_expected_points']
    rushing_stats = pd.DataFrame(columns=rush_columns) #dataframe to hold the year's rushing stats
    table = BeautifulSoup(table, 'html.parser') 
    for tr in table.select('tr'): #loops through each row of the table
        row_data = [td.get_text(strip=True) for td in tr.select('td')]
        if len(row_data) < 1: continue
        #format of the strings
        # Tm	                 G	   Att	   Yds	 TD	  Lng	Y/A	    Y/G	    Fmb	    EXP
        #['Kansas City Chiefs', '17', '417', '1784', '9', '48', '4.3', '104.9', '20', '-4.58']
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

        fumbles = row_data[8]
        if len(fumbles) < 1:
            fumbles = None
        try:
            rush_expected_points = row_data[9]
        except:
            rush_expected_points = None
        
        row = {
            'team_id': rush_team_id,
            'season' : year,
            'games': games,
            'rush_attempts': rush_attempts,
            'rush_yards': rush_yards,
            'rush_tds': rush_tds,
            'fumbles': fumbles,
            'rush_expected_points': rush_expected_points
        }
        rushing_stats = rushing_stats._append(row, ignore_index=True)
        #this adds each lines rushing stats data to the dataframe
    
    table = soup.select_one('#all_passing').find_next(string=lambda t: isinstance(t, Comment))
    time.sleep(1)
    print('Pulling passing data for', year, '\n')  
    pass_columns = ['team_id', 'games', 'season', 'pass_attempts', 'completions', 'pass_yards', 'pass_tds', 'interceptions', 'sacks', 'sack_yards', 'fourth_quarter_comebacks', 'game_winning_drives', 'pass_expected_points']
    passing_stats = pd.DataFrame(columns=pass_columns) #dataframe to hold this years passing stats
    table = BeautifulSoup(table, 'html.parser')
    for tr in table.select('tr'): #loops through each row in the table
        row_data = [td.get_text(strip=True) for td in tr.select('td')]
        if len(row_data) < 1: continue
        team_name = row_data[0]
        if team_name not in team_dict: continue
        pass_team_id = team_dict.get(team_name)
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
        
        pass_touchdowns = row_data[6]
        if len(pass_touchdowns) < 1:
            pass_touchdowns = None
        
        interceptions = row_data[8]
        if len(interceptions) < 1:
            interceptions = None
        
        sacks = row_data[16]
        if len(sacks) < 1:
            sacks = None
        
        sack_yards = row_data[17]
        if len(sack_yards) < 1:
            sack_yards = None
        try:
            fourth_quarter_comebacks = row_data[21]
            if len(fourth_quarter_comebacks) < 1:
                fourth_quarter_comebacks = None
        except:
            fourth_quarter_comebacks = None
        try:
            game_winning_drives = row_data[22]
            if len(game_winning_drives) < 1:
                game_winning_drives = None
        except:
            game_winning_drives = None
        
        try:
            pass_expected_points = row_data[23]
            if len(pass_expected_points) < 1:
                pass_expected_points = None
        except:
            pass_expected_points = None
        
        row = {
            'team_id': pass_team_id,
            'season': year,
            'games': games,
            'pass_attempts':  pass_attempts,
            'completions': completions, 
            'pass_yards': pass_yards, 
            'pass_tds': pass_touchdowns,
            'interceptions': interceptions, 
            'sacks': sacks,
            'sack_yards': sack_yards,
            'fourth_quarter_comebacks': fourth_quarter_comebacks,
            'game_winning_drives': game_winning_drives,
            'pass_expected_points': pass_expected_points

        }
        
        passing_stats = passing_stats._append(row, ignore_index=True)

    year_combined_stats = pd.merge( #this comnines the rushing_stats and _passing stats dataframe into one dataframe
        rushing_stats, passing_stats,
        on=['team_id', 'season', 'games'],  # Common columns
        how='outer')  # Include all data, even if incomplete
    
    offense_stats = pd.concat([offense_stats, year_combined_stats], ignore_index=True)
    #that combined dataframe is now added to offense_stats
    #at the end of the program all of offense_stats will be added to the sql table

offense_stats = offense_stats.where(pd.notnull(offense_stats), None)
# every None will now be Null in the database

cur.executescript('''DROP TABLE IF EXISTS season_offensive_stats;
                CREATE TABLE season_offensive_stats (
                team_id INTEGER,
                season INTEGER,
                games INTEGER,
                rush_attempts INTEGER,
                rush_yards INTEGER,
                rush_tds INTEGER,
                fumbles INTEGER,
                rush_expected_points REAL,
                pass_attempts INTEGER,
                completions INTEGER,
                pass_yards  INTEGER,
                pass_tds INTEGER,
                interceptions INTEGER,
                sacks INTEGER,
                sack_yards INTEGER,
                fourth_quarter_comebacks INTEGER,
                game_winning_drives INTEGER,
                pass_expected_points REAL,
                FOREIGN KEY (team_id) REFERENCES teams(team_id),
                PRIMARY KEY (team_id, season));''')

offense_stats.to_sql('season_offensive_stats', conn, if_exists='append', index=False) #this adds everything to the database in one commit

print('All rushing and passing data from 1932 to 2023 has been uploaded to your database!')