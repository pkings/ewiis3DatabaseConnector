import time
import pandas as pd

from ewiis3DatabaseConnector import execute_sql_query, load_latest_timeslot_of_gameId


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


def load_grid_imbalance(game_id, limit=336):
    if game_id is None:
        return pd.DataFrame(), game_id

    try:
        sql_statement = """SELECT ts_meets_br.*, wr.temperature, wr.cloudCover, wr.windDirection, wr.windSpeed FROM
  (SELECT ts.gameId, br.timeslotIndex, ts.isWeekend, ts.dayOfWeek, ts.slotInDay, ts.timestamp, br.netImbalance  FROM
  (SELECT * FROM ewiis3.timeslot WHERE timeslot.gameId="{}" ORDER BY serialNumber DESC LIMIT {}) AS ts
  LEFT JOIN
  (SELECT * FROM ewiis3.balance_report WHERE balance_report.gameId="{}") AS br
  ON ts.serialNumber=br.timeslotIndex) AS ts_meets_br
LEFT JOIN
  (SELECT * FROM ewiis3.weather_report WHERE weather_report.gameId="{}") AS wr
  ON ts_meets_br.timeslotIndex = wr.timeslotIndex ORDER BY ts_meets_br.timeslotIndex ASC;""".format(game_id, limit, game_id, game_id)
        df_total_grid_imbalance = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting grid imbalances from db.')
        df_total_grid_imbalance = pd.DataFrame()
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


def load_tariff_specification_meets_avg_rates(game_id=None):
    if game_id:
        where_clause = ' WHERE gameId="{}"'.format(game_id)
    else:
        where_clause = ''
    sql_statement = """SELECT * FROM
        ((SELECT t.* FROM ewiis3.tariff_specification t) {} AS ts
        LEFT JOIN
        (SELECT AVG(minValueMoney), AVG(maxValueMoney), AVG(maxCurtailment), AVG(tierThreshold), AVG(downRegulationPayment), AVG(upRegulationPayment), COUNT(*) AS rateCount, t.tariffSpecificationId FROM ewiis3.rate t {} GROUP BY tariffSpecificationId) AS rateAnalysis
        ON ts.tariffSpecificationId=rateAnalysis.tariffSpecificationId)""".format(where_clause, where_clause)
    df_tariff_spec_avg_rates = execute_sql_query(sql_statement)
    return df_tariff_spec_avg_rates



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


def load_customer_prosumption_with_weather_and_time(game_id, limit=336):
    latest_timeslot_of_game = load_latest_timeslot_of_gameId(game_id)
    if game_id is None:
        return pd.DataFrame(), game_id

    try:
        sql_statement = """SELECT prosumption_meets_weather.*, ts.dayOfWeek, ts.isWeekend, ts.slotInDay FROM
(SELECT * FROM (SELECT postedTimeslotIndex, SUM(kWH) FROM ewiis3.tariff_transaktion WHERE gameId="{}" AND (txType="CONSUME" OR txType="PRODUCE") AND {} - tariff_transaktion.postedTimeslotIndex <= {} GROUP BY postedTimeslotIndex ORDER BY postedTimeslotIndex DESC LIMIT {}) AS customer_prod_con
LEFT JOIN
(SELECT * FROM ewiis3.weather_report WHERE weather_report.gameId="{}") AS wr ON customer_prod_con.postedTimeslotIndex = wr.timeslotIndex ) AS prosumption_meets_weather
  LEFT JOIN (SELECT * FROM ewiis3.timeslot WHERE gameId = "{}") AS ts ON prosumption_meets_weather.postedTimeslotIndex = ts.serialNumber ORDER BY postedTimeslotIndex ASC;""".format(game_id, latest_timeslot_of_game, limit, limit, game_id, game_id)
        df_customer_prosumption = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting customer prosumption from db.')
        df_customer_prosumption = pd.DataFrame()
    return df_customer_prosumption, game_id


def load_grid_consumption_and_production(game_id):
    if game_id is None:
        return pd.DataFrame(), game_id

    try:
        sql_statement = 'SELECT prosumptin_meets_weather.*, ts.isWeekend, ts.dayOfWeek, ts.slotInDay FROM (SELECT dr.*, wr.cloudCover, wr.temperature, wr.windDirection, wr.windSpeed FROM (SELECT * FROM ewiis3.distribution_report WHERE distribution_report.gameId="{}") AS dr LEFT JOIN (SELECT * FROM ewiis3.weather_report WHERE weather_report.gameId="{}") AS wr ON dr.timeslot = wr.timeslotIndex) AS prosumptin_meets_weather LEFT JOIN (SELECT * FROM ewiis3.timeslot WHERE timeslot.gameId="{}") AS ts ON prosumptin_meets_weather.timeslot = ts.serialNumber;'.format(game_id, game_id, game_id)
        df_total_grid_consumption_and_production = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting grid consumption and production from db.')
        df_total_grid_consumption_and_production = pd.DataFrame()
    return df_total_grid_consumption_and_production, game_id


def load_weather_forecast(game_id):
    if game_id is None:
        return pd.DataFrame(), game_id
    try:
        sql_statement = 'SELECT t.* FROM ewiis3.weather_forecast t WHERE gameId="{}" ORDER BY postedTimeslotIndex DESC LIMIT 24'.format(game_id)
        df_lates_weather_forecast = execute_sql_query(sql_statement)
    except Exception as e:
        print('Error occured while requesting weather forecast from db.')
        df_lates_weather_forecast = pd.DataFrame()
    return df_lates_weather_forecast, game_id
