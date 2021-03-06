import pandas as pd

from ewiis3DatabaseConnector import execute_sql_query, connect_to_local_database, create_db_connection_engine


""" use load predictions instead
def load_grid_imbalance_prediction(game_id):
    try:
        sql_statement = 'SELECT t.* FROM ewiis3.imbalance_prediction t WHERE game_id="{}"'.format(game_id)
        df_imbalance = execute_sql_query(sql_statement)
    except Exception as E:
        df_imbalance = pd.DataFrame()
    return df_imbalance"""


""" use load predictions instead
def load_customer_prosumption_prediction(game_id):
    try:
        sql_statement = 'SELECT t.* FROM ewiis3.customer_prosumption_prediction t WHERE game_id="{}"'.format(game_id)
        df_customer_prosumption = execute_sql_query(sql_statement)
    except Exception as E:
        df_customer_prosumption = pd.DataFrame()
    return df_customer_prosumption"""


def load_grid_consumption_and_production_prediction(game_id):
    try:
        sql_statement = 'SELECT t.* FROM ewiis3.prediction t WHERE game_id="{}" AND target="grid" AND (type="consumption" OR type="production")'.format(game_id)
        df_prosumption_prediction = execute_sql_query(sql_statement)
    except Exception as E:
        df_prosumption_prediction = pd.DataFrame()
    return df_prosumption_prediction


def load_predictions(table_name, game_id, target=None, type=None):
    try:
        where_clause = ' WHERE'
        if game_id:
            where_clause = '{} game_id="{}"'.format(where_clause, game_id)
        if target:
            and_or_not = ''
            if where_clause.find('=') > -1:
                and_or_not = 'AND '
            where_clause = '{} {}target="{}"'.format(where_clause, and_or_not, target)
        if type:
            and_or_not = ''
            if where_clause.find('=') > -1:
                and_or_not = 'AND '
            where_clause = '{} {}type="{}"'.format(where_clause, and_or_not, type)
        if not game_id and not target and not type:
            where_clause = ''

        sql_statement = "SELECT * FROM {}{}".format(table_name, where_clause)
        df_predictions = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting `{}` table with target {} and type {} for game_id {}from db.'.format(table_name, target, type, game_id))
        df_predictions = pd.DataFrame()
    return df_predictions


def store_predictions(df_predictions, table_name):
    cnx = create_db_connection_engine()
    df_predictions.to_sql(name=table_name, schema='ewiis3', con=cnx, if_exists='append', index=False)


def store_price_intervals(df_intervals, game_id):
    try:
        conn = connect_to_local_database()
        conn.cursor()
        cur = conn.cursor()
        cur.execute('DELETE FROM ewiis3.wholesale_price_intervals WHERE game_id="{}"'.format(game_id))
        conn.commit()
        conn.close()

        cnx = create_db_connection_engine()
        df_intervals.to_sql(name='wholesale_price_intervals', schema='ewiis3', con=cnx, if_exists='append',
                                          index=False)
    except Exception as e:
        print('Error occured during storing price intervals.')
        print(e)