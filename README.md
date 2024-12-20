# ETL Pipeline Documentation

This document is auto-generated from the codebase.

## data_processing.py

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

### Function: `execute_query`

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

### Function: `calculate_payments`

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

### Function: `calculate_duration`

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

### Function: `calculate_profitable_actors`

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

## database_operations.py

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

### Function: `manage_tables`

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

### Function: `write_dataframe_to_db`

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

## file_operations.py

file_operations.py

This module provides utilities for handling file operations, specifically writing 
pandas DataFrames to local text files. 

Functions:
    - write_local_txt_output: Writes a pandas DataFrame to a text file in tab-delimited format.

Logging:
    - Logs the start and successful completion of file writing operations.
    - Logs errors encountered during file creation or writing.

Requirements:
    - pandas: Required for handling DataFrame operations.
    - os: For folder and file path manipulations.
    - logging: For logging operations and errors.

Usage:
    Import the `write_local_txt_output` function to save a DataFrame as a text file in a specified folder.

Example:
    >>> import pandas as pd
    >>> from file_operations import write_local_txt_output
    >>> data = {'Column1': [1, 2], 'Column2': ['A', 'B']}
    >>> df = pd.DataFrame(data)
    >>> write_local_txt_output(df, "output_folder", "output_file.txt")

### Function: `write_local_txt_output`

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
    - The file is written in tab-delimited format (`sep='   '`).
    - The index is not included in the output file (`index=False`).
    - Ensure the DataFrame contains valid data before calling this function.

## clear_folder.py

No module-level documentation available.

### Function: `clear_folder`

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

