import pymysql
import pandas as pd
import logging
import time
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv
import os

# load up the .env entries as environment variables
load_dotenv(find_dotenv())

db_user = os.getenv("DB_USER")
db_pw = os.getenv("DB_PW")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_schema = os.getenv("DB_SCHEMA")



def connect_to_local_database():
    conn = pymysql.connect(host=db_host, user=db_user, passwd=db_pw, db=db_schema)
    return conn


def create_db_connection_engine():
    connection_string = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(db_user, db_pw, db_host, db_port, db_schema)
    cnx = create_engine(connection_string, echo=False)
    return cnx


def execute_sql_query(sql_query):
    conn = connect_to_local_database()
    df_mysql = pd.read_sql(sql_query, con=conn)
    conn.close()
    return df_mysql

"""
def load_consumption_and_production_data(max_timeslot=None):
    where_clause = '' if max_timeslot == None else ' WHERE `postedTimeslotIndex` <= {}'.format(max_timeslot)
    sql_statement = "SELECT * FROM `tariff_transaktion`{}".format(where_clause)
    df_tariff_transactions = execute_sql_query(sql_statement)
    filter_tx_type = ['CONSUME', 'PRODUCE']
    df_tariff_transactions = df_tariff_transactions[df_tariff_transactions['txType'].isin(filter_tx_type)]
    return df_tariff_transactions"""
