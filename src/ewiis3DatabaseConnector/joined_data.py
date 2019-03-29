import time
import pandas as pd

from ewiis3DatabaseConnector import execute_sql_query


####################################################
#### Cleared Trades
####################################################


def load_cleared_trades(game_id):
    if game_id is None:
        return pd.DataFrame(), game_id
    try:
        sql_statement = 'SELECT ct.*, ts.isWeekend, ts.dayOfWeek, ts.slotInDay, ts.timestamp FROM (SELECT t.* FROM ewiis3.cleared_trade t WHERE gameId="{}") AS ct LEFT JOIN (SELECT * FROM ewiis3.timeslot WHERE timeslot.gameId="{}") AS ts ON ct.timeslot = ts.serialNumber'.format(game_id, game_id)
        df_cleared_trades = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting cleared trades from db.')
        df_cleared_trades = pd.DataFrame()
    return df_cleared_trades, game_id


####################################################
#### Balancing and Capacity
####################################################


def load_grid_imbalance(game_id):
    if game_id is None:
        return pd.DataFrame(), game_id

    start_time = time.time()
    try:
        sql_statement = 'SELECT prosumptin_meets_weather.*, ts.isWeekend, ts.dayOfWeek, ts.slotInDay FROM (SELECT dr.*, wr.cloudCover, wr.temperature, wr.windDirection, wr.windSpeed FROM (SELECT * FROM ewiis3.balance_report WHERE balance_report.gameId="{}") AS dr LEFT JOIN (SELECT * FROM ewiis3.weather_report WHERE weather_report.gameId="{}") AS wr ON dr.timeslotIndex = wr.timeslotIndex) AS prosumptin_meets_weather LEFT JOIN (SELECT * FROM ewiis3.timeslot WHERE timeslot.gameId="{}") AS ts ON prosumptin_meets_weather.timeslotIndex = ts.serialNumber;'.format(game_id, game_id, game_id)
        df_total_grid_imbalance = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting grid imbalances from db.')
        df_total_grid_imbalance = pd.DataFrame()
    print('Loading grid imbalance last: {} seconds.'.format(time.time() - start_time))
    return df_total_grid_imbalance, game_id


def load_balance_report(game_id=None):
    if game_id:
        where_clause = ' WHERE gameId="{}"'.format(game_id)
    else:
        where_clause = ''
    sql_statement = 'SELECT t.* FROM ewiis3.balance_report t{}'.format(where_clause)
    df_balance_report = execute_sql_query(sql_statement)
    return df_balance_report


####################################################
#### Tariffs
####################################################


def load_tariff_evaluation_metrics():
    sql_statement = "select powerType, txType, SUM(charge), AVG(currentSubscribedPopulation) from ewiis3.tariff_transaktion group by powerType, txType;"
    df_tariff_evaluation = execute_sql_query(sql_statement)
    return df_tariff_evaluation


def load_rates():
    sql_statement = "SELECT r.*, ts.brokerName, ts.gameId, ts.powerType FROM ewiis3.rate  r LEFT JOIN ewiis3.tariff_specification ts ON r.tariffSpecificationId = ts.tariffSpecificationId"
    df_rates = execute_sql_query(sql_statement)
    return df_rates


####################################################
#### Prosumptions
####################################################


def load_distribution_reports(game_id=None):
    if game_id:
        where_clause = ' WHERE gameId="{}"'.format(game_id)
    else:
        where_clause = ''
    sql_statement = 'SELECT t.* FROM ewiis3.distribution_report t{}'.format(where_clause)
    df_distribution_reports = execute_sql_query(sql_statement)
    return df_distribution_reports


def load_customer_prosumption(game_id=None):
    if game_id:
        where_clause = ' AND gameId="{}"'.format(game_id)
    else:
        where_clause = ''
    sql_statement = 'SELECT postedTimeslotIndex, SUM(kWH) FROM ewiis3.tariff_transaktion WHERE (txType = "CONSUME" OR txType = "PRODUCE"){} GROUP BY postedTimeslotIndex'.format(where_clause)
    df_customer_prosumption = execute_sql_query(sql_statement)
    return df_customer_prosumption


def load_customer_prosumption_with_weather_and_time(game_id):
    if game_id is None:
        return pd.DataFrame(), game_id

    start_time = time.time()
    try:
        sql_statement = 'SELECT * FROM (SELECT * FROM (SELECT postedTimeslotIndex, SUM(kWH) FROM ewiis3.tariff_transaktion WHERE gameId = "{}" AND (txType = "CONSUME" OR txType = "PRODUCE") GROUP BY postedTimeslotIndex) AS customer_prod_con LEFT JOIN (SELECT * FROM ewiis3.weather_report WHERE weather_report.gameId="{}") AS wr ON customer_prod_con.postedTimeslotIndex = wr.timeslotIndex ) AS prosumption_meets_weather LEFT JOIN (SELECT * FROM ewiis3.timeslot WHERE gameId = "{}") AS ts ON prosumption_meets_weather.postedTimeslotIndex = ts.serialNumber;'.format(game_id, game_id, game_id)
        df_customer_prosumption = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting customer prosumption from db.')
        df_customer_prosumption = pd.DataFrame()
    print('Loading customer prosumption last: {} seconds.'.format(time.time() - start_time))
    return df_customer_prosumption, game_id


def load_grid_consumption_and_production(game_id):
    if game_id is None:
        return pd.DataFrame(), game_id

    start_time = time.time()
    try:
        sql_statement = 'SELECT prosumptin_meets_weather.*, ts.isWeekend, ts.dayOfWeek, ts.slotInDay FROM (SELECT dr.*, wr.cloudCover, wr.temperature, wr.windDirection, wr.windSpeed FROM (SELECT * FROM ewiis3.distribution_report WHERE distribution_report.gameId="{}") AS dr LEFT JOIN (SELECT * FROM ewiis3.weather_report WHERE weather_report.gameId="{}") AS wr ON dr.timeslot = wr.timeslotIndex) AS prosumptin_meets_weather LEFT JOIN (SELECT * FROM ewiis3.timeslot WHERE timeslot.gameId="{}") AS ts ON prosumptin_meets_weather.timeslot = ts.serialNumber;'.format(game_id, game_id, game_id)
        df_total_grid_consumption_and_production = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting grid consumption and production from db.')
        df_total_grid_consumption_and_production = pd.DataFrame()
    print('Loading grid consumption and production last: {} seconds.'.format(time.time() - start_time))
    return df_total_grid_consumption_and_production, game_id
