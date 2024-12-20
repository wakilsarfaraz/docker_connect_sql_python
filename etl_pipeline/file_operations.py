"""
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
"""

import os
import logging

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

