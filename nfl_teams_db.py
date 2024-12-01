import sqlite3

conn = sqlite3.connect('nfldb.sqlite')
cur = conn.cursor()

cur.executescript('''DROP TABLE IF EXISTS teams;
            CREATE TABLE teams (
            id  INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            name TEXT,
            abbreviation TEXT,
            conference TEXT,
            division TEXT
            )''')

team_file = open('/Users/chadgahafer/Desktop/Scripts/nfl_teams.csv')

count = 0
for line in team_file:
    team_data = line.split(',')
    #['1', 'Arizona Cardinals', 'ARI', 'NFC', 'West\n'] this is the format of the csv data once each line is converted to a list
    count = count + 1
    if count == 1: continue #need the counter to avoid the header of the csv being input into the teams table
    whole_name = team_data[1].strip()
    name_split = whole_name.split()
    if len(name_split) == 2:
        loc = name_split[0].strip()
        name = name_split[1].strip()
    else:
        name = name_split[2].strip()
        loc = name_split[0].strip() + ' ' + name_split[1].strip()
    abbr = team_data[2].strip()
    conf = team_data[3].strip()
    div = team_data[4].strip()
    print(loc, name, abbr, conf, div)
    cur.execute('''INSERT OR IGNORE INTO teams (location, name, abbreviation, conference, division)
                VALUES (?, ?, ?, ?, ?)''', (loc, name, abbr, conf, div))

team_file.close()
conn.commit()