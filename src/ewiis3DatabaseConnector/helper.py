import time
import pandas as pd

from ewiis3DatabaseConnector import execute_sql_query


####################################################
#### Time and game data
####################################################

# TODO: deprecated! should not be used anymore
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


def load_latest_timeslot_of_gameId(game_id):
    latest_timeslot = None
    try:
        sql_statement='SELECT * FROM ewiis3.timeslot t WHERE t.gameId ="{}" ORDER BY timeslotId DESC LIMIT 1;'.format(game_id)
        df_latest = execute_sql_query(sql_statement)
        latest_timeslot = df_latest['serialNumber'].values[0]
    except Exception as e:
        print('Error occured while requesting latest timeslot for gameId {} from db.'.format(game_id))
    return latest_timeslot


def load_finished_gameIds():
    try:
        sql_statement = 'SELECT DISTINCT(t.gameId) FROM ewiis3.finished_game t'
        df_finished_games = execute_sql_query(sql_statement)
        finished_gameIds = list(df_finished_games['gameId'])
    except Exception as e:
        print('Error occured while requesting finished gameIds from db.')
        finished_gameIds = []
    return finished_gameIds


def load_all_gameIds():
    try:
        sql_statement = 'SELECT DISTINCT(t.gameId) FROM ewiis3.timeslot t'
        df_all_gameIds = execute_sql_query(sql_statement)
        all_gameIds = list(df_all_gameIds['gameId'])
    except Exception as e:
        print('Error occured while requesting all gameIds from db.')
        all_gameIds = []
    return all_gameIds


def get_running_gameIds():
    finished_gameIds = load_finished_gameIds()
    all_gameIds = load_all_gameIds()
    gameIds_to_process = [gameId for gameId in all_gameIds if gameId not in finished_gameIds]
    return gameIds_to_process

