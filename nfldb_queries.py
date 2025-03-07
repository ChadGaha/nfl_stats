import sqlite3
import os
import pandas as pd

os.chdir('/Users/chadgahafer/Desktop/Scripts/databases')
conn = sqlite3.connect('nfldb.sqlite')
cur = conn.cursor()

def season_average_rush_attempts():

    ra_sql = '''SELECT season AS year, AVG(rush_attempts) AS rush_attempts
                        FROM season_offensive_stats
                        GROUP BY year;
                        '''

    df = pd.read_sql_query(ra_sql, conn)
    conn.close()
    return df

def offensive_season_averages_by_year():
    sql = '''SELECT season,
                AVG(games) AS games,
                AVG(rush_attempts) AS rush_attempts, 
                AVG(rush_yards) AS rush_yards,
                AVG(rush_tds) AS rush_tds,
                AVG(fumbles) AS fumbles,
                AVG(rush_expected_points) AS rush_expected_points,
                AVG(pass_attempts) AS pass_attempts,
                AVG(completions) AS completions ,
                AVG(s.pass_yards) AS net_pass_yards,
                AVG(s.pass_yards + s.sack_yards) AS gross_pass_yards,
                AVG(pass_tds) AS pass_touchdowns,
                AVG(interceptions) AS interceptions,
                AVG(sacks) AS sacks,
                AVG(sack_yards) AS sack_yards_lost,
                AVG( fourth_quarter_comebacks) AS fourth_quarter_comebacks,
                AVG(game_winning_drives)  AS game_winning_drives,
                AVG(pass_expected_points) AS pass_expected_points
            FROM season_offensive_stats
            GROUP BY season;'''
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

def team_offensive_averages_since_1970():
    sql = '''SELECT (t.location || ' ' || t.name) AS team,
                AVG(s.games) AS games,
                AVG(s.rush_attempts) AS rush_attempts, 
                AVG(s.rush_yards) AS rush_yards,
                AVG(s.rush_tds) AS rush_tds,
                AVG(s.fumbles) AS fumbles,
                AVG(s.rush_expected_points) AS rush_expected_points,
                AVG(s.pass_attempts) AS pass_attempts,
                AVG(s.completions) AS completions ,
                AVG(s.pass_yards) AS net_pass_yards,
                AVG(s.pass_yards + s.sack_yards) AS gross_pass_yards,
                AVG(s.pass_tds) AS pass_touchdowns,
                AVG(s.interceptions) AS interceptions,
                AVG(s.sacks) AS sacks,
                AVG(s.sack_yards) AS sack_yards_lost,
                AVG( s.fourth_quarter_comebacks) AS fourth_quarter_comebacks,
                AVG(s.game_winning_drives)  AS game_winning_drives,
                AVG(s.pass_expected_points) AS pass_expected_points
            FROM season_offensive_stats AS s
            LEFT JOIN teams AS t
                ON s.team_id = t.id
            WHERE season > 1969 
            GROUP BY team_id;'''
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

if __name__ == "__main__":
    x = season_average_rush_attempts()
    print(x)





