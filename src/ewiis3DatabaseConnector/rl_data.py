import time
import pandas as pd

from ewiis3DatabaseConnector import execute_sql_query, load_latest_timeslot_of_gameId



def load_consumption_tariff_earnings(game_id, limit):
    sql_statement = """SELECT t.postedTimeslotIndex, SUM(t.charge) FROM ewiis3.tariff_transaktion t WHERE gameId="{}" AND powerType="CONSUMPTION" AND (txType="CONSUME" OR txType = "PERIODIC") GROUP BY postedTimeslotIndex ORDER BY postedTimeslotIndex DESC Limit {}""".format(game_id, limit)
    df_tariff_spec_avg_rates = execute_sql_query(sql_statement)
    return df_tariff_spec_avg_rates


def load_consumption_tariff_prosumption(game_id, limit):
    sql_statement = """SELECT t.postedTimeslotIndex, SUM(t.kWh) FROM ewiis3.tariff_transaktion t WHERE gameId="{}" AND powerType="CONSUMPTION" AND (txType="CONSUME" OR txType = "PRODUCE") GROUP BY postedTimeslotIndex ORDER BY postedTimeslotIndex DESC Limit {}""".format(game_id, limit)
    df_tariff_spec_avg_rates = execute_sql_query(sql_statement)
    return df_tariff_spec_avg_rates
