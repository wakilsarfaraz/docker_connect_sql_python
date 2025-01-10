import pyodbc
import pandas as pd
import os
import shutil
import logging
import getpass
from etl_pipeline import manage_notebook
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("etl_pipeline.log"),
                        logging.StreamHandler()
                    ])

def clear_folder(folder_path):
    """
    Clears the contents of a specified folder by deleting all files, subdirectories, 
    and symbolic links within it.

    This function recursively deletes all items inside the given folder path, including:
    - Regular files
    - Symbolic links
    - Subdirectories and their contents

    Args:
        folder_path (str): The path to the folder whose contents need to be cleared.

    Logging:
        - Logs the start of the clearing process at the INFO level.
        - Logs a success message when the folder is cleared at the INFO level.
        - Logs any errors encountered during the process at the ERROR level.

    Raises:
        Exception: If any error occurs while attempting to delete files, subdirectories, 
        or symbolic links.

    Examples:
        >>> clear_folder("/path/to/directory")
        INFO: Starting to clear the contents of folder: /path/to/directory
        INFO: Contents of folder /path/to/directory have been cleared.

    Note:
        - The function does not delete the folder itself, only its contents.
        - Ensure that the folder exists before calling this function. If the folder does not 
          exist, an error may be raised.
        - Use with caution as this action is irreversible.

    """
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
            - 'sqlFiles/tableManagement/drop_payment_summary_table.sql'
            - 'sqlFiles/tableManagement/drop_duration_summary_table.sql'
            - 'sqlFiles/tableManagement/drop_profitable_actors_table.sql'
        - Create Table SQL Files:
            - 'sqlFiles/tableManagement/create_payment_summary_table.sql'
            - 'sqlFiles/tableManagement/create_duration_summary_table.sql'
            - 'sqlFiles/tableManagement/create_profitable_actors_table.sql'

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
        drop_payment_table_file = os.path.join('sql_files/table_management', 'drop_payment_summary_table.sql')
        drop_duration_table_file = os.path.join('sql_files/table_management', 'drop_duration_summary_table.sql')
        drop_profitable_table_file = os.path.join('sql_files/table_management','drop_profitable_actors_table.sql')
        create_payment_table_file = os.path.join('sql_files/table_management', 'create_payment_summary_table.sql')
        create_duration_table_file = os.path.join('sql_files/table_management', 'create_duration_summary_table.sql')
        create_profitable_actors_table_file = os.path.join('sql_files/table_management','create_profitable_actors_table.sql')
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
    """
    Writes a pandas DataFrame to a local text file in tab-delimited format.

    This function ensures the target folder exists (creates it if necessary), then writes 
    the given DataFrame to a text file with tab-separated values. The resulting file is saved 
    in the specified folder with the provided file name.

    Args:
        dataframe (pandas.DataFrame): The DataFrame to write to the text file.
        folder_path (str): The path to the folder where the file will be saved. If the folder 
                           does not exist, it will be created.
        file_name (str): The name of the text file to create.

    Returns:
        str: The full path to the created file if the operation is successful.
        None: If an error occurs during the operation.

    Logging:
        - Logs the start of the file writing process at the INFO level.
        - Logs the successful completion of the operation at the INFO level.
        - Logs any errors encountered during the operation at the ERROR level.

    Raises:
        Exception: Logs any error that occurs during folder creation, file writing, or 
                   DataFrame processing.

    Examples:
        >>> import pandas as pd
        >>> data = {'Column1': [1, 2], 'Column2': ['A', 'B']}
        >>> df = pd.DataFrame(data)
        >>> write_local_txt_output(df, "output_folder", "output_file.txt")
        INFO: Starting to write DataFrame to text file: output_file.txt
        INFO: Processed data successfully written to output_folder/output_file.txt

    Notes:
        - The file is written in tab-delimited format (`sep='\t'`).
        - The index is not included in the output file (`index=False`).
        - Ensure the DataFrame contains valid data before calling this function.

    """
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
    payments_df = calculate_payments("sql_files/queries/payments.sql", connection_string)
    duration_df = calculate_duration("sql_files/queries/film_duration.sql", connection_string)
    profitable_actors_df = calculate_profitable_actors("sql_files/queries/profitable_actors.sql", connection_string)
    write_dataframe_to_db(payments_df, "payment_summary_table", connection_string)
    write_dataframe_to_db(duration_df, "duration_summary_table", connection_string)
    write_dataframe_to_db(profitable_actors_df,"profitable_actors_table", connection_string)
    write_local_txt_output(payments_df, "reports", "payment_summary.txt")
    write_local_txt_output(duration_df, "reports", "duration_summary.txt")
    write_local_txt_output(profitable_actors_df, "reports", "profitable_actors.txt")