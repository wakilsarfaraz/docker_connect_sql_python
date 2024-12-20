# etl_pipeline/clear_folder.py
import os
import shutil
import logging

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

