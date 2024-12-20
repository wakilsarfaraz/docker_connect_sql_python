# etl_pipeline/data_processing.py
import pyodbc
import pandas as pd
import logging

def execute_query(sql_file_path, connection_string):
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    return rows

def calculate_payments(sql_file_path, connection_string):
    logging.info("Starting to calculate payments summary.")
    rows = execute_query(sql_file_path, connection_string)
    df = pd.DataFrame((tuple(t) for t in rows), columns=['Records', 'Minimum', 'Maximum', 'Total', 'Average'])
    logging.info("Payments summary successfully retrieved.")
    return df

def calculate_duration(sql_file_path, connection_string):
    logging.info("Starting to calculate duration summary.")
    rows = execute_query(sql_file_path, connection_string)
    df = pd.DataFrame((tuple(t) for t in rows), columns=['Minimum', 'Maximum', 'Total', 'Average'])
    logging.info("Duration summary successfully retrieved.")
    return df

def calculate_profitable_actors(sql_file_path, connection_string):
    logging.info("Starting to calculate profitable actors.")
    rows = execute_query(sql_file_path, connection_string)
    df = pd.DataFrame((tuple(t) for t in rows), columns=['ActorID', 'FirstName', 'LastName', 'TotalSale'])
    logging.info("Profitable actors successfully retrieved.")
    return df
