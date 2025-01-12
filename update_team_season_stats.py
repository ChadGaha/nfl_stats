import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import sqlite3
import os
import time

os.chdir('/Users/chadgahafer/Desktop/Scripts/databases') #change this to where your SQLite database is located
conn = sqlite3.connect('nfldb.sqlite')
cur = conn.cursor()

teams = cur.execute('''SELECT (location || ' ' || name) AS team, id
                    FROM teams;
                    ''')

team_dict = dict()
for line in teams:
    team_dict[line[0]] = line[1]

year = int(input('What year do you want to update?\n'))
update_count = 0


url = f"https://www.pro-football-reference.com/years/{year}/"
time.sleep(7) #this obeys pro-football-reference's rate limits and make the traffic random to appear more human
soup = BeautifulSoup(requests.get(url).content, 'html.parser')
table = soup.select_one('#all_rushing').find_next(string=lambda t: isinstance(t, Comment))
#the table is in a comment in the html; this locates it
print('Pulling updated rushing data for 2024')
table = BeautifulSoup(table, 'html.parser') 
for tr in table.select('tr'): #loops through each row of the table
    row_data = [td.get_text(strip=True) for td in tr.select('td')]
    if len(row_data) < 1: 
        continue
    team_name = row_data[0]
    if team_name not in team_dict: 
        continue
    rush_team_id = team_dict.get(team_name)
    # Tm	               G	Att	    Yds	    TD	Lng	    Y/A	   Y/G	   Fmb	  EXP
    #Kansas City Chiefs', '13', '356', '1438', '11', '34', '4.0', '110.6', '7', '21.44'

    season = 2024
    games = row_data[1]
    rush_attempts = row_data[2]
    rush_yards = row_data[3]
    rush_tds = row_data[4]
    fumbles = row_data[8]
    rush_expected_points = row_data[9]
    
    cur.execute('''UPDATE season_offensive_stats
                SET games = ?, rush_attempts = ?, rush_yards = ?, rush_tds = ?, fumbles = ? , rush_expected_points = ?
                WHERE team_id = ? AND season = ?;''', (games, rush_attempts, rush_yards, rush_tds, fumbles, rush_expected_points, rush_team_id, year))
    update_count += 1

time.sleep(1)
print('Pulling updated passing data for 2024')   
table = soup.select_one('#all_passing').find_next(string=lambda t: isinstance(t, Comment))
table = BeautifulSoup(table, 'html.parser') 
for tr in table.select('tr'):
    row_data = [td.get_text(strip=True) for td in tr.select('td')]
    if len(row_data) < 1: 
        continue
    team_name = row_data[0]
    if team_name not in team_dict: 
        continue
    pass_team_id = team_dict.get(team_name)
    pass_attempts = row_data[3]
    completions = row_data[2]
    pass_yards = row_data[5]
    pass_touchdowns = row_data[6]
    interceptions = row_data[8]
    sacks = row_data[16]
    sack_yards = row_data[17]
    fourth_quarter_comebacks = row_data[21]
    game_winning_drives = row_data[22]
    pass_expected_points = row_data[23]

    cur.execute('''UPDATE season_offensive_stats
                SET pass_attempts = ?, completions = ?, pass_yards = ?, pass_tds = ?, interceptions = ?, sacks = ?, sack_yards = ?, fourth_quarter_comebacks = ?, game_winning_drives = ?, pass_expected_points = ?
                WHERE team_id = ? AND season = ?;''', (pass_attempts, completions, pass_yards, pass_touchdowns, interceptions, sacks, sack_yards, fourth_quarter_comebacks, game_winning_drives, pass_expected_points, pass_team_id, year))
    update_count += 1
    
url = f"https://www.pro-football-reference.com/years/{year}/opp.htm"
time.sleep(8)
soup = BeautifulSoup(requests.get(url).content, 'html.parser')
table = soup.select_one('#all_rushing').find_next(string=lambda t: isinstance(t, Comment))
#the table is in a comment in the html; this locates it
table = BeautifulSoup(table, 'html.parser')
print('Pulling updated team rushing defense data for', year,)

for tr in table.select('tr'):
    row_data = [td.get_text(strip=True) for td in tr.select('td')]
    if len(row_data) < 1: 
        continue
    team_name = row_data[0]
    if team_name not in team_dict:
        continue
    rush_def_team_id = team_dict.get(team_name)
    games = row_data[1]
    rush_attempts_def = row_data[2]
    rush_yards_def = row_data[3]
    rush_tds_def = row_data[4]
    rush_expected_points_def = row_data[7]

    cur.execute('''UPDATE season_defensive_stats
                SET games = ?, rush_attempts = ?, rush_yards = ?, rush_tds = ?, rush_expected_points = ?
                WHERE team_id = ? AND season = ?''', (games, rush_attempts_def, rush_yards_def, rush_tds_def, rush_expected_points_def, rush_def_team_id, year))



soup = BeautifulSoup(requests.get(url).content, 'html.parser')
table = soup.select_one('#all_passing').find_next(string=lambda t: isinstance(t, Comment))
print('Pulling updated team passing defense data for', year, '\n')
table = BeautifulSoup(table, 'html.parser')
for tr in table.select('tr'):
    row_data = [td.get_text(strip=True) for td in tr.select('td')]
    if len(row_data) < 1: 
        continue
    team_name = row_data[0]
    if team_name not in team_dict: 
        continue
    pass_def_team_id = team_dict.get(team_name)
    pass_attempts_def = row_data[3]
    completions_def = row_data[2]
    pass_yards_def = row_data[5]
    pass_tds_def = row_data[6]
    interceptions_def = row_data[8]
    passes_defended_def = row_data[9]
    sacks_def = row_data[16]
    sack_yards_def = row_data[17]
    qb_hits_def = row_data[18]  
    tfl = row_data[19]
    pass_expected_points_def = row_data[23]

    cur.execute('''UPDATE season_defensive_stats
                SET pass_attempts = ?, completions = ?, pass_yards = ?, pass_tds = ?, interceptions = ?, passes_defended = ?, sacks = ?, sack_yards = ?, qb_hits = ?, tfl = ?, pass_expected_points = ?
                WHERE team_id = ? AND season = ?''', (pass_attempts_def, completions_def, pass_yards_def, pass_tds_def, interceptions_def, passes_defended_def, sacks_def, sack_yards_def, qb_hits_def, tfl, pass_expected_points_def, pass_def_team_id, year))

print(f"Rows updated: {update_count}")

team_updates = int(update_count) // 2
print(f"The amount of teams updated is:", team_updates)
conn.commit()

print('\nAll season stats have been updated for', year)

conn.close()
