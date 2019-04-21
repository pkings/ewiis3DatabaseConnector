import time

from ewiis3DatabaseConnector import execute_sql_query


def load_weather_report(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.tariff_specification t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_weather_report = execute_sql_query(sql_statement)
    return df_weather_report


def load_orderbooks(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.orderbook_order t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_orderbook = execute_sql_query(sql_statement)
    return df_orderbook


def load_full_table_cleared_trades(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.cleared_trade t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_cleared_trades = execute_sql_query(sql_statement)
    return df_cleared_trades


def load_tariff_transactions(game_id=None):
    sql_statement = "SELECT * FROM `tariff_transaktion`"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_tariff_transactions = execute_sql_query(sql_statement)
    return df_tariff_transactions


def load_balancing_transactions(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.balancing_transaction t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_balancing_transactions = execute_sql_query(sql_statement)
    return df_balancing_transactions


def load_capacity_transactions(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.capacity_transaction t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_capacity_transactions = execute_sql_query(sql_statement)
    return df_capacity_transactions


def load_tariff_specifications(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.tariff_specification t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_tariff_specification = execute_sql_query(sql_statement)
    return df_tariff_specification


def load_order_submits(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.order_submit t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_order_submits = execute_sql_query(sql_statement)
    return df_order_submits


def load_tariff_subscription_shares(game_id=None):
    sql_statement = "SELECT t.* FROM ewiis3.tariffSubscriptions t"
    if game_id:
        sql_statement = __append_game_id_where_clause(sql_statement, game_id)
    df_tariff_subscription_shares = execute_sql_query(sql_statement)
    return df_tariff_subscription_shares


def __append_game_id_where_clause(sql_statement, game_id):
    where_clause = ' WHERE gameId="{}"'.format(game_id)
    return "{}{}".format(sql_statement, where_clause)