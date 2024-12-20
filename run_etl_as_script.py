import pyodbc
import pandas as pd
import os
import shutil
import logging
import getpass

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("etl_pipeline.log"),
                        logging.StreamHandler()
                    ])

def clear_folder(folder_path):
    '''
    docstring for clear_folder() first change
    '''
    logging.info(f"Starting to clear the contents of folder: {folder_path}")
    try:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        logging.info(f"Contents of folder {folder_path} have been cleared.")
    except Exception as e:
        logging.error(f"Error while clearing folder {folder_path}: {e}")


def manage_tables(connection_string):
    '''
    docstring for cmanage_tables() first change
    '''
    logging.info("Starting to manage tables in the database.")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        drop_payment_table_file = os.path.join('sqlFiles/tableManagement', 'drop_payment_summary_table.sql')
        drop_duration_table_file = os.path.join('sqlFiles/tableManagement', 'drop_duration_summary_table.sql')
        drop_profitable_table_file = os.path.join('sqlFiles/tableManagement','drop_profitable_actors_table.sql')
        create_payment_table_file = os.path.join('sqlFiles/tableManagement', 'create_payment_summary_table.sql')
        create_duration_table_file = os.path.join('sqlFiles/tableManagement', 'create_duration_summary_table.sql')
        create_profitable_actors_table_file = os.path.join('sqlFiles/tableManagement','create_profitable_actors_table.sql')
        def execute_sql_file(file_path):
            with open(file_path, 'r') as file:
                sql = file.read()
                cursor.execute(sql)
        execute_sql_file(drop_payment_table_file)
        execute_sql_file(drop_duration_table_file)
        execute_sql_file(drop_profitable_table_file)
        connection.commit()
        execute_sql_file(create_payment_table_file)
        execute_sql_file(create_duration_table_file)
        execute_sql_file(create_profitable_actors_table_file)
        connection.commit()
        logging.info("Tables:\n\n                                         payment_summary_table\n"         
                     "                                         duration_summary_table\n"
                     "                                         profitable_actors_table \n\n"
                     "                                 have been recreated in the database.")
    except pyodbc.Error as e:
        logging.error(f"Error managing tables: {e}")
    except FileNotFoundError as e:
        logging.error(f"SQL file not found: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
# manage_tables() function ends here   
         
def calculate_payments(sql_file_path, connection_string):
    '''
    docstring for calculate_payments() first change
    '''
    logging.info("Starting to calculate payments summary.")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        payments_summary = pd.DataFrame((tuple(t) for t in rows)) 
        payments_summary.columns = ['Records', 'Minimum', 'Maximum', 'Total', 'Average']
        logging.info("Payments summary successfully retrieved.")
    except pyodbc.Error as e:
        logging.error(f"Error executing payments query: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
    return payments_summary

def calculate_duration(sql_file_path, connection_string):
    logging.info("Starting to calculate duration summary.")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        duration_summary = pd.DataFrame((tuple(t) for t in rows)) 
        duration_summary.columns = ['Minimum', 'Maximum', 'Total', 'Average']
        logging.info("Duration summary successfully retrieved.")
    except pyodbc.Error as e:
        logging.error(f"Error executing duration query: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
    return duration_summary

def calculate_profitable_actors(sql_file_path, connection_string):
    logging.info("Starting to calculate profitable actors.")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        profitable_actors = pd.DataFrame((tuple(t) for t in rows)) 
        profitable_actors.columns = ['ActorID', 'FirstName', 'LastName', 'TotalSale']
        logging.info("Profitable actors successfully retrieved.")
    except pyodbc.Error as e:
        logging.error(f"Error executing duration query: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
    return profitable_actors

def write_dataframe_to_db(dataframe, table_name, connection_string):
    logging.info(f"Starting to write DataFrame to table: {table_name}")
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        # Insert rows into the database
        for index, row in dataframe.iterrows():
            placeholders = ', '.join(['?'] * len(row))
            columns = ', '.join(dataframe.columns)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        connection.commit()
        logging.info(f"Data successfully written to table: {table_name}.")
    except pyodbc.Error as e:
        logging.error(f"Error writing to database table {table_name}: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

def write_local_txt_output(dataframe, folder_path, file_name):
    logging.info(f"Starting to write DataFrame to text file: {file_name}")
    try:
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, file_name)
        dataframe.to_csv(file_path, sep='\t', index=False)
        logging.info(f"Processed data successfully written to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"An error occurred while writing to text file {file_name}: {e}")
        return None

# Main block starts here

if __name__ == "__main__":
    server = input("Please enter the SQL Server address (hint: starts with tcp and ends with .net): ").strip()
    username = input("Please enter your Username:").strip()
    password = getpass.getpass("Please enter your Password: ").strip()
    connection_string =   str(
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={server},1433;"
    f"Database=sakila;"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    target_folder = "reports"
    clear_folder(target_folder)
    manage_tables(connection_string)
    payments_df = calculate_payments("sqlFiles/queries/payments.sql", connection_string)
    duration_df = calculate_duration("sqlFiles/queries/filmduration.sql", connection_string)
    profitable_actors_df = calculate_profitable_actors("sqlFiles/queries/profitable_actors.sql", connection_string)
    write_dataframe_to_db(payments_df, "payment_summary_table", connection_string)
    write_dataframe_to_db(duration_df, "duration_summary_table", connection_string)
    write_dataframe_to_db(profitable_actors_df,"profitable_actors_table", connection_string)
    write_local_txt_output(payments_df, "reports", "payment_summary.txt")
    write_local_txt_output(duration_df, "reports", "duration_summary.txt")
    write_local_txt_output(profitable_actors_df, "reports", "profitable_actors.txt")