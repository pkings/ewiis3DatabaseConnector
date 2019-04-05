import time

from ewiis3DatabaseConnector import execute_sql_query


def load_weather_report():
    sql_statement = "SELECT t.* FROM ewiis3.weather_report t"
    df_weather_report = execute_sql_query(sql_statement)
    return df_weather_report


def load_orderbooks():
    sql_statement = "SELECT t.* FROM ewiis3.orderbook_order t"
    df_orderbook = execute_sql_query(sql_statement)
    return df_orderbook


def load_full_table_cleared_trades():
    sql_statement = "SELECT t.* FROM ewiis3.cleared_trade t"
    df_cleared_trades = execute_sql_query(sql_statement)
    return df_cleared_trades


def load_tariff_transactions():
    sql_statement = "SELECT * FROM `tariff_transaktion`"
    df_tariff_transactions = execute_sql_query(sql_statement)
    return df_tariff_transactions


def load_balancing_transactions():
    sql_statement = "SELECT t.* FROM ewiis3.balancing_transaction t"
    df_balancing_transactions = execute_sql_query(sql_statement)
    return df_balancing_transactions


def load_capacity_transactions():
    sql_statement = "SELECT t.* FROM ewiis3.capacity_transaction t"
    df_capacity_transactions = execute_sql_query(sql_statement)
    return df_capacity_transactions


def load_tariff_specifications():
    sql_statement = "SELECT t.* FROM ewiis3.tariff_specification t"
    df_tariff_specification = execute_sql_query(sql_statement)
    return df_tariff_specification


def load_order_submits():
    sql_statement = "SELECT t.* FROM ewiis3.order_submit t"
    df_order_submits = execute_sql_query(sql_statement)
    return df_order_submits


def load_tariff_subscription_shares():
    sql_statement = "SELECT t.* FROM ewiis3.tariffSubscriptions t"
    df_tariff_subscription_shares = execute_sql_query(sql_statement)
    return df_tariff_subscription_shares
