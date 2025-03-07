# nfl_stats
This repo allows the creation of a local SQLite database containing NFL statisitics. The databse is created and populated using Python, and the data itself is scraped from pr-football-reference.com.

To ensure proper indexing and primary/foreign key assignments, it is necessary to execute the scripts in the following order. More info about the individual scripts can be found in their respective files.

1. nfl_teams_db.py
2. old_team_aliases.py
3. team_offense_stats.py
4. team_defense_stats.py
5. nfl_standings.py

The other scripts can be used to update the databse with data from the present year.
