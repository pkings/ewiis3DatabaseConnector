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



""" use load predictions instead
def load_prosumption_prediction(game_id):
    try:
        sql_statement = 'SELECT t.* FROM ewiis3.prosumption_prediction t WHERE game_id="{}"'.format(game_id)
        df_prosumption_prediction = execute_sql_query(sql_statement)
    except Exception as E:
        df_prosumption_prediction = pd.DataFrame()
    return df_prosumption_prediction"""


def load_predictions(table_name, game_id):
    try:
        if game_id:
            where_clause = ' WHERE game_id="{}"'.format(game_id)
        else:
            where_clause = ''

        sql_statement = "SELECT * FROM {}{}".format(table_name, where_clause)
        df_predictions = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting `{}` table from db.'.format(table_name))
        df_predictions = pd.DataFrame()
    return df_predictions


def store_predictions(df_prosumption_predictions, table_name):
    cnx = create_db_connection_engine()
    df_prosumption_predictions.to_sql(name=table_name, schema='ewiis3', con=cnx, if_exists='append', index=False)


def store_price_intervals(df_intervals, game_id):
    try:
        conn = __connect_to_local_database()
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