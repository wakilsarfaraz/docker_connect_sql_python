"""
database_operations.py

This module provides utilities for managing database tables and writing data to the database.
It includes functionality for executing SQL scripts to drop and recreate tables and for 
inserting data from pandas DataFrames into specific tables.

Functions:
    - manage_tables: Drops and recreates database tables using specified SQL files.
    - write_dataframe_to_db: Inserts rows from a pandas DataFrame into a database table.

Logging:
    - Logs operations such as starting processes, success messages, and errors.

Requirements:
    - pyodbc: For database connectivity.
    - pandas: For handling tabular data to insert into the database.

Usage:
    Import the functions and use them with a valid database connection string to manage tables 
    or write data into them.
"""

import pyodbc
import logging

def manage_tables(connection_string):
    """
    Drops and recreates database tables by executing SQL scripts.

    This function reads SQL scripts from predefined file paths to drop existing tables 
    and create new ones. It ensures that the database schema is updated by executing 
    the scripts sequentially.

    Args:
        connection_string (str): A valid database connection string used to connect to 
                                 the database.

    Logging:
        - Logs the start of the table management process.
        - Logs success after recreating tables.
        - Logs errors encountered during execution.

    SQL File Structure:
        - Drop Table SQL Files:
            - 'sql_files/table_management/drop_payment_summary_table.sql'
            - 'sql_files/table_management/drop_duration_summary_table.sql'
            - 'sql_files/table_management/drop_profitable_actors_table.sql'
        - Create Table SQL Files:
            - 'sql_files/table_management/create_payment_summary_table.sql'
            - 'sql_files/table_management/create_duration_summary_table.sql'
            - 'sql_files/table_management/create_profitable_actors_table.sql'

    Raises:
        Exception: If any error occurs during the database operations.

    Examples:
        >>> manage_tables("Driver={ODBC Driver 18 for SQL Server};"
                          "Server=server_name;"
                          "Database=database_name;"
                          "Uid=username;Pwd=password;")
    """
    logging.info("Starting to manage tables in the database.")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        sql_files = {
            "drop": [
                'sql_files/table_management/drop_payment_summary_table.sql',
                'sql_files/table_management/drop_duration_summary_table.sql',
                'sql_files/table_management/drop_profitable_actors_table.sql'
            ],
            "create": [
                'sql_files/table_management/create_payment_summary_table.sql',
                'sql_files/table_management/create_duration_summary_table.sql',
                'sql_files/table_management/create_profitable_actors_table.sql'
            ]
        }
        for sql_type, files in sql_files.items():
            for file_path in files:
                with open(file_path, 'r') as file:
                    sql = file.read()
                    cursor.execute(sql)
        connection.commit()
        logging.info("Tables:\n\n                                         payment_summary_table\n"         
                     "                                         duration_summary_table\n"
                     "                                         profitable_actors_table \n\n"
                     "                                 have been recreated in the database.")
    except Exception as e:
        logging.error(f"Error managing tables: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()


def write_dataframe_to_db(dataframe, table_name, connection_string):
    """
    Inserts rows from a pandas DataFrame into a specified database table.

    This function takes a pandas DataFrame, converts its rows into SQL INSERT statements, 
    and writes them to the specified table in the database.

    Args:
        dataframe (pandas.DataFrame): The DataFrame containing the data to insert.
        table_name (str): The name of the database table to insert data into.
        connection_string (str): A valid database connection string used to connect to 
                                 the database.

    Logging:
        - Logs the start of the data insertion process.
        - Logs success after the data is written.
        - Logs errors encountered during execution.

    Raises:
        Exception: If any error occurs while inserting data into the database.

    Examples:
        >>> import pandas as pd
        >>> data = {'column1': [1, 2], 'column2': ['A', 'B']}
        >>> df = pd.DataFrame(data)
        >>> write_dataframe_to_db(df, "my_table", 
                                  "Driver={ODBC Driver 18 for SQL Server};"
                                  "Server=server_name;"
                                  "Database=database_name;"
                                  "Uid=username;Pwd=password;")
    """
    logging.info(f"Starting to write DataFrame to table: {table_name}")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        for _, row in dataframe.iterrows():
            placeholders = ', '.join(['?'] * len(row))
            columns = ', '.join(dataframe.columns)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        connection.commit()
        logging.info(f"Data successfully written to table: {table_name}.")
    except Exception as e:
        logging.error(f"Error writing to database table {table_name}: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
