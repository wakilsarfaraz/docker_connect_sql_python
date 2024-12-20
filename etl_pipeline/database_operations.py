# etl_pipeline/database_operations.py
import pyodbc
import logging

def manage_tables(connection_string):
    logging.info("Starting to manage tables in the database.")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        sql_files = {
            "drop": [
                'sqlFiles/tableManagement/drop_payment_summary_table.sql',
                'sqlFiles/tableManagement/drop_duration_summary_table.sql',
                'sqlFiles/tableManagement/drop_profitable_actors_table.sql'
            ],
            "create": [
                'sqlFiles/tableManagement/create_payment_summary_table.sql',
                'sqlFiles/tableManagement/create_duration_summary_table.sql',
                'sqlFiles/tableManagement/create_profitable_actors_table.sql'
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
