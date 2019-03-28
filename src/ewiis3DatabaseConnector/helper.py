import time

from ewiis3DatabaseConnector import execute_sql_query


####################################################
#### Time and game data
####################################################


def get_current_game_id_and_timeslot():
    game_id = None
    latest_timeslot = None
    try:
        sql_statement='SELECT * FROM ewiis3.timeslot ORDER BY timeslotId DESC LIMIT 1;'
        df_latest = execute_sql_query(sql_statement)
        latest_timeslot = df_latest['serialNumber'].values[0]
        game_id = df_latest['gameId'].values[0]
    except Exception as e:
        print('Error occured while requesting current game_id and latest timeslot from db.')
    return game_id, latest_timeslot


def load_all_game_ids():
    try:
        sql_statement = "SELECT DISTINCT(t.gameId) FROM ewiis3.timeslot t"
        df_imbalance = execute_sql_query(sql_statement)
        game_ids = list(df_imbalance['gameId'])
    except Exception as E:
        game_ids = []
    return game_ids
