#program to get old team names from pro-football reference into a csv
# creation of the team_aliases table in nfldb.sqlite
#the info from the csv is then loaded into the table team_aliases

import pandas as pd
import sqlite3

url = 'https://www.pro-football-reference.com/teams/index.htm'

tables = pd.read_html(url)

teams = tables[0]

teams.to_csv('old_teams.csv')
# I was not confident in reading and manipulating dataframes when I converted the table to a CSV
# If I were rewriting this code I would skip this intermiediary step, 
# but it allowed me to work through getting the info I wanted without constantly requesting the website

conn = sqlite3.connect('nfldb.sqlite')
cur = conn.cursor()

teams = cur.execute("SELECT (location || ' ' || name) AS team_names, id FROM teams")
#this query returns a value like "Kansas City Chiefs" under team_names

team_dict = dict()
team_names = list()
for line in teams:
    team_dict[line[0]] = line[1] 
#puts all current team_names with their id in the team_dict with key: value pairs team_name: id
    
#SQL Query to crerate the team_aliases table
cur.executescript('''DROP TABLE IF EXISTS team_aliases;
                  CREATE TABLE team_aliases (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  location TEXT,
                  name TEXT,
                  start_year INTEGER,
                  end_year INTEGER,
                  current_team_id INTEGER,
                  FOREIGN KEY (current_team_id) REFERENCES teams (id))
                  ''')

csv = open('old_teams.csv')
current_team_id = None
count = 0
for info in csv:
    count = count + 1
    if count == 1 or count == 2: 
        continue #have to skip the first two rows as they are headers
  
    info_split = info.split(',')
    name = info_split[1].strip()
    name_split = name.split() #gets the location of the team and mascot
    start = info_split[2] #year the alias started
    end = info_split[3] #year the alias ended
    if name in team_dict: #the csv is set up listing the current team first, goes through their old names, and then lists another current team
        current_team_id = team_dict.get(name) #this grabs the team id from the dictionary to match it
        continue #if no aliases are there because the team name is current, then it continues the loop!
    foreign_id = current_team_id
    if len(name_split) == 2: #most names follow the format of 'Baltimore Colts.' This grabs them and 
        loc = name_split[0].strip()
        name = name_split[1].strip()
    elif name_split[1] == 'Football':  # the Washington Football Team is the only mascot with a two part name in the CSV. I made this exception to grab them
        loc = name_split[0].strip()
        name = 'Football Team'
    else:
        loc = name_split[0].strip() + ' ' + name_split[1].strip()
        name = name_split[2].strip() #this grabs the rest of the teams, whose format follows 'St. Louis Rams.'
    
    cur.execute('''INSERT INTO team_aliases (location, name, start_year, end_year, current_team_id)
                VALUES (?, ?, ?, ?, ?)''', (loc, name, start, end, foreign_id))
    
    #this SQL query inserts each alias info into the team_aliases table

conn.commit()