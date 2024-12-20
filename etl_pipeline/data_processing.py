"""
data_processing.py

This module contains functions for processing data using SQL queries and converting 
results into pandas DataFrames for further analysis. 

Functions:
    - execute_query: Executes a SQL query from a file and returns the results.
    - calculate_payments: Calculates a summary of payments using SQL query results.
    - calculate_duration: Calculates a summary of film durations using SQL query results.
    - calculate_profitable_actors: Retrieves a summary of the most profitable actors 
      based on SQL query results.

Logging:
    - Logs the start and successful completion of each operation at the INFO level.
    - Logs errors encountered during SQL query execution or data processing.

Requirements:
    - pandas: For handling tabular data.
    - pyodbc: For connecting to the SQL database.
    - logging: For reporting the progress through logs.

Usage:
    Import the functions and pass the SQL file path and database connection string 
    to process data.
"""

import pyodbc
import pandas as pd
import logging

def execute_query(sql_file_path, connection_string):
    """
    Executes a SQL query from a file and returns the query results.

    This function reads a SQL query from the specified file, executes it using the provided 
    database connection string, and retrieves the results.

    Args:
        sql_file_path (str): Path to the SQL file containing the query.
        connection_string (str): Database connection string.

    Returns:
        list: A list of rows retrieved from the SQL query.

    Raises:
        pyodbc.Error: If there is an error while connecting to the database or executing the query.

    Examples:
        >>> rows = execute_query("queries/payments.sql", connection_string)
    """
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    return rows


def calculate_payments(sql_file_path, connection_string):
    """
    Calculates a summary of payments using a SQL query and returns a pandas DataFrame.

    This function executes a SQL query from the specified file, processes the query 
    results, and generates a summary DataFrame with the following columns:
    - Records
    - Minimum
    - Maximum
    - Total
    - Average

    Args:
        sql_file_path (str): Path to the SQL file containing the query.
        connection_string (str): Database connection string.

    Returns:
        pandas.DataFrame: A DataFrame containing the payments summary.

    Raises:
        pyodbc.Error: If there is an error while executing the query or connecting to the database.

    Examples:
        >>> payments_df = calculate_payments("queries/payments.sql", connection_string)
    """
    logging.info("Starting to calculate payments summary.")
    rows = execute_query(sql_file_path, connection_string)
    df = pd.DataFrame((tuple(t) for t in rows), columns=['Records', 'Minimum', 'Maximum', 'Total', 'Average'])
    logging.info("Payments summary successfully retrieved.")
    return df


def calculate_duration(sql_file_path, connection_string):
    """
    Calculates a summary of film durations using a SQL query and returns a pandas DataFrame.

    This function executes a SQL query from the specified file, processes the query 
    results, and generates a summary DataFrame with the following columns:
    - Minimum
    - Maximum
    - Total
    - Average

    Args:
        sql_file_path (str): Path to the SQL file containing the query.
        connection_string (str): Database connection string.

    Returns:
        pandas.DataFrame: A DataFrame containing the duration summary.

    Raises:
        pyodbc.Error: If there is an error while executing the query or connecting to the database.

    Examples:
        >>> duration_df = calculate_duration("queries/filmduration.sql", connection_string)
    """
    logging.info("Starting to calculate duration summary.")
    rows = execute_query(sql_file_path, connection_string)
    df = pd.DataFrame((tuple(t) for t in rows), columns=['Minimum', 'Maximum', 'Total', 'Average'])
    logging.info("Duration summary successfully retrieved.")
    return df


def calculate_profitable_actors(sql_file_path, connection_string):
    """
    Retrieves a summary of the most profitable actors using a SQL query and returns a pandas DataFrame.

    This function executes a SQL query from the specified file, processes the query 
    results, and generates a summary DataFrame with the following columns:
    - ActorID
    - FirstName
    - LastName
    - TotalSale

    Args:
        sql_file_path (str): Path to the SQL file containing the query.
        connection_string (str): Database connection string.

    Returns:
        pandas.DataFrame: A DataFrame containing the summary of profitable actors.

    Raises:
        pyodbc.Error: If there is an error while executing the query or connecting to the database.

    Examples:
        >>> profitable_actors_df = calculate_profitable_actors("queries/profitable_actors.sql", connection_string)
    """
    logging.info("Starting to calculate profitable actors.")
    rows = execute_query(sql_file_path, connection_string)
    df = pd.DataFrame((tuple(t) for t in rows), columns=['ActorID', 'FirstName', 'LastName', 'TotalSale'])
    logging.info("Profitable actors successfully retrieved.")
    return df

