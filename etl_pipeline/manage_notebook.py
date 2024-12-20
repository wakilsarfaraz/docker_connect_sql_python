import ast
import nbformat
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell

# File paths
notebook_name = "updated_notebook.ipynb"
etl_script_path = "run_etl_as_script.py"

def parse_etl_script_with_ast_and_main(file_path):
    sections = {
        "libraries_and_logging": None,
        "functions": {},
        "main_script": None,
    }

    with open(file_path, "r") as f:
        script = f.read()

    # Parse the script using AST
    tree = ast.parse(script)

    # Extract libraries and logging configuration (lines before the first function)
    with open(file_path, "r") as f:
        lines = f.readlines()

    # Capture libraries and logging configuration
    libraries = []
    for idx, line in enumerate(lines):
        if line.strip().startswith("def "):  # Stop at the first function
            sections["libraries_and_logging"] = "".join(libraries).strip()
            break
        libraries.append(line)

    # Extract all function definitions using AST
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            func_name = node.name
            func_start_line = node.lineno - 1
            func_end_line = getattr(node, "end_lineno", None) or len(lines)
            sections["functions"][func_name] = "".join(lines[func_start_line:func_end_line]).strip()

    # Detect the main block using the comment
    main_started = False
    main_block_lines = []

    for line in lines:
        stripped_line = line.strip()
        if stripped_line == "# Main block starts here":
            main_started = True  # Start capturing the main block
        if main_started:
            main_block_lines.append(line)

    if main_block_lines:
        sections["main_script"] = "".join(main_block_lines).strip()

    return sections

def create_or_update_notebook():
    # Parse the ETL script using the hybrid method
    print("Parsing ETL script using AST and comment-based main block detection...")
    sections = parse_etl_script_with_ast_and_main(etl_script_path)

    # Verify parsing results
    if not sections["libraries_and_logging"]:
        print("ERROR: Libraries and logging configuration not found.")
    else:
        print("Libraries and logging configuration parsed successfully.")

    if not sections["functions"]:
        print("ERROR: No functions found in the script.")
    else:
        print(f"Parsed {len(sections['functions'])} functions successfully.")

    if not sections["main_script"]:
        print("ERROR: Main block not found.")
    else:
        print("Main block parsed successfully.")

    # Load or create the notebook
    try:
        with open(notebook_name, "r") as f:
            notebook = nbformat.read(f, as_version=4)
    except (FileNotFoundError, nbformat.reader.NotJSONError):
        print(f"Notebook {notebook_name} not found or invalid. Creating a new notebook.")
        notebook = new_notebook()

    # Clear existing cells
    notebook.cells = []

    # Add notebook content dynamically
    markdown_cells = [
        """# Connecting to SQL Database using `pyodbc` Library in Python
This notebook is a step-by-step guide to help you learn how to 
use Python functions and libraries to connect to an SQL database 
using the `pyodbc` library. Each section of the notebook includes 
an explanation of the code, which you can run in order to understand 
the workflow step by step.
## Preparing the SQL Database
Before using Python, we need to decide which SQL database to connect to. 
For this exercise, we will use the A Cloud Guru (ACG) sandbox to create an SQL database. 
Once the database is set up, we will obtain the necessary credentials to connect to it using Python. 
A detailed guide on how to create the SQL database in the ACG sandbox is available in a GitHub 
repository called [DE-sql-learning-environment-Azure](https://github.com/Corndel/DE-sql-learning-environment-Azure). 
Following the instructions provided in the repository from step **1 Create the ACG Sandbox for SQL learning** to step **4 Create tables in Azure SQL database and insert data**, by the completion of which you will have created an SQL database called **sakila**. We will need the **connection strings** for this database in order to be able to connect to it using Python library `pyodbc`. 
## Steps to Acquire the Connection Strings
- Visit the SQL databases in the Azure portal (you can search for this service using the search 
box at the top and type SQL).

![step-1](images/step1.png)

- In the list of databases that appears, click on the single database 
**sakila (sakilayaq6nqkx2fcqo/sakila)**.

![step-2](images/step2.png)

- Click on **Settings** and then on **Connection Strings** in the drop down menu.

![step-3](images/step3.png)

- Go to the `ODBC`tab to see the connection string.

![step-4](images/step4.png)

- You will need to copy the underlined parts of the **Connection String** and store 
it somewhere safely to be used when running the pipeline.

![step-5](images/step5.png)

- Note that the password *{your_password_here}* is a placeholder within the connection strings, 
which needs to be replaced with the actual password of the database.

- When running the pipeline you will be prompted to enter these three pieces of information:

    * Server name (the longest underlined part in the above screenshot, which you will be different 
    every time you create sakila). 
    * Username which for this database is **corndeladmin**.
    * Passoword, which is **Password01**.

## Connecting to the **Sakila** Database and Creating an ETL Pipeline

This guide will walk you through the steps (using the `pyodbc` library) to connect to the 
SQL database we have just created. You will learn how to extract data from the database, manage 
tables within it, and write the processed data back into the database. Follow each step carefully 
to gain a clear understanding of how to build an ETL pipeline when you are using Python to work with 
SQL databases on virtual machines or on remote platforms online.

### The Python Libraries

#### 1. `pyodbc`
It is a Python library for connecting to SQL databases using ODBC drivers. It allows 
communication between Python and SQL databases, making it essential for running SQL queries 
and managing database connections.

---

#### 2. `pandas`
This is a Python library for data manipulation and analysis. It provides powerful 
tools to handle tabular data, such as reading, writing, and processing datasets extracted 
from the database.

---

#### 3. `os`
This is a standard Python library for interacting with the operating system. 
It enables tasks like file and directory manipulation, crucial for managing input/output 
files during the ETL process.

---

#### 4. `shutil`
It is a standard Python library for high-level file operations. It helps in efficiently 
clearing or organising folders by removing directories and their contents.

---

#### 5. `logging`
This library is used for tracking events during code execution. It provides detailed 
logs for debugging and monitoring the ETL pipeline, ensuring transparency and easier 
troubleshooting.

---

#### 6. `getpass`
The `getpass` library in Python securely prompts the user for sensitive information, 
such as passwords, without displaying the input on the screen. It is ideal for creating 
secure, interactive command-line applications.

Let's run the following cell to import all of these libraries.





""",
        """## Connecting to the **Sakila** Database and Creating an ETL Pipeline

This guide will walk you through the steps (using the `pyodbc` library) to connect to the SQL 
database we have just created. You will learn how to extract data from the database, manage 
tables within it, and write the processed data back into the database. Follow each step carefully 
to gain a clear understanding of how to build an ETL pipeline when you are using Python to work 
with SQL databases on virtual machines or on remote platforms online.


### The Python Libraries

#### 1. `pyodbc`
It is a Python library for connecting to SQL databases using ODBC drivers. 
It allows communication between Python and SQL databases, making it essential 
for running SQL queries and managing database connections.

---

#### 2. `pandas`
This is a Python library for data manipulation and analysis. It provides powerful tools 
to handle tabular data, such as reading, writing, and processing datasets extracted from 
the database.

---

#### 3. `os`
This is a standard Python library for interacting with the operating system. 
It enables tasks like file and directory manipulation, crucial for managing input/output 
files during the ETL process.

---

#### 4. `shutil`
It is a standard Python library for high-level file operations. It helps in efficiently 
clearing or organising folders by removing directories and their contents.

---

#### 5. `logging`
This library is used for tracking events during code execution. It provides detailed 
logs for debugging and monitoring the ETL pipeline, ensuring transparency and easier 
troubleshooting.

#### 5. `getpass`
The `getpass` library in Python securely prompts the user for sensitive information, 
such as passwords, without displaying the input on the screen. It is ideal for creating 
secure, interactive command-line applications.

Let's run the following cell to import all of these libraries. This code after the importation 
uses the `logging` library to set up logging for the ETL pipeline to track events and errors. 
It writes logs to a file named `etl_pipeline.log` and displays them on the console. The log 
messages include the time, log level, and the message for clear and detailed tracking 
of the pipeline’s progress.
""",
        """#### Function: `clear_folder`

This function removes all files and subdirectories within a specified folder. 
It iterates through each item in the folder, deleting files, symbolic links, and entire 
directories as needed. The process is logged for transparency, with messages indicating 
when the operation starts, completes, or encounters an error. This function is particularly 
useful for ensuring a clean workspace before running an ETL pipeline or similar processes. 
We will use this function to ensure that our target folder is cleared before the ETL output 
is written into it. Our target folder for this function is called `reports`, which will be 
used when we run the pipeline.""",
        """#### Function: `manage_tables`
The `manage_tables` function is responsible for resetting the structure of specific database tables. 
It connects to the database using the provided connection string and executes SQL scripts to drop 
existing tables (`payment_summary_table` and `duration_summary_table`) and recreate them. 
The function reads the SQL commands from files located in the `queries` folder, ensuring the 
database is prepared for new data. It handles errors, such as database connection issues or 
missing SQL files, and logs the process for transparency and troubleshooting.""",
        """#### Function: `calculate_payments`
The `calculate_payments` function reads an SQL query from a specified file, executes it on 
a database, and retrieves the results as a pandas DataFrame. The function connects to the 
database using a given connection string and processes the query to calculate a payments summary, 
including columns such as `Records`, `Minimum`, `Maximum`, `Total`, and `Average`. It logs progress 
and errors for transparency and closes the database connection after execution. The result is 
returned as a structured DataFrame for further analysis.""",
        """#### Function: `calculate_duration`

This function retrieves a summary of film durations from an SQL database by executing a query 
provided in an external SQL file. It connects to the database using the given connection string, 
reads the query from the specified file, and runs it. The results are then stored in a pandas 
DataFrame with columns: `Minimum`, `Maximum`, `Total`, and `Average`. Finally, it logs the process 
and ensures the database connection is closed.""",
        """#### Function: `calculate_profitable_actors`

The `calculate_profitable_actors` function retrieves a list of the most profitable actors from 
a database based on an SQL query. It connects to the database using the provided connection string, 
reads the SQL query from a file, and executes it to fetch the results. These results are converted 
into a pandas DataFrame with clear column names: `ActorID`, `FirstName`, `LastName`, and 
`TotalSale`. The function includes error handling to log any issues and ensures the database 
connection is closed afterwards to manage resources efficiently. This function is a great way 
for learners to experiment with combining Python, SQL, and pandas to process and analyse data 
effectively.""",
        """#### Function: `write_dataframe_to_db`

The `write_dataframe_to_db` function inserts the rows of a pandas DataFrame into a specified 
SQL database table. It connects to the database using the provided connection string, then 
iterates through each row of the DataFrame and executes an SQL `INSERT` statement to write the data. 
The function dynamically matches column names and values using placeholders to ensure compatibility. 
It also handles errors gracefully by logging issues and ensuring the database connection is properly 
closed after the operation.""",
        """#### Function: `write_local_txt_output`

This function saves the contents of a pandas DataFrame as a tab-separated text file in a specified 
folder. It first ensures the folder exists (creating it if necessary), then writes the DataFrame to 
a file with the given name. The file does not include row indices, making it cleaner for sharing or 
further processing. If successful, the function logs the file’s location and returns its path. 
In case of an error, it logs the issue and returns `None`, ensuring clear feedback for 
troubleshooting.""",
        """#### Running the pipeline

This section of the code serves as the entry point for the script. It begins by prompting the user 
to provide the SQL Server address, username, and password, ensuring secure input for the connection. 
A connection string is then constructed to establish communication with the `sakila` database using 
the specified SQL Server. The script prepares a target folder (`reports`) by clearing any existing 
content and processes the database tables using custom queries.

It performs the following tasks:
1. Executes the SQL query for payments and calculates summary data.
2. Executes the SQL query for film durations and calculates summary data.
3. Executes the SQL query for profitable actors and calculates a table of the most profitable actors.
4. Saves the results into three summary tables in the database: `payment_summary_table`, `duration_summary_table`, and `profitable_actors_table`.
5. Exports also the same results into local `.txt` files, stored in the `reports` folder.

The pipeline ensures that all relevant data is processed, stored, and made accessible for further use.
"""
    ]

    # Add libraries and logging
    print("Adding libraries and logging...")
    notebook.cells.append(new_markdown_cell(markdown_cells[0]))
    notebook.cells.append(new_markdown_cell(markdown_cells[1]))
    if sections["libraries_and_logging"]:
        notebook.cells.append(new_code_cell(sections["libraries_and_logging"]))

    # Add functions dynamically in the correct order
    function_names = [
        "clear_folder",
        "manage_tables",
        "calculate_payments",
        "calculate_duration",
        "calculate_profitable_actors",
        "write_dataframe_to_db",
        "write_local_txt_output",
    ]

    for idx, func_name in enumerate(function_names):
        print(f"Adding function: {func_name}...")
        markdown_title = markdown_cells[2 + idx]
        func_body = sections["functions"].get(func_name)
        if func_body:
            notebook.cells.append(new_markdown_cell(markdown_title))
            notebook.cells.append(new_code_cell(func_body))
        else:
            print(f"ERROR: Function '{func_name}' is missing.")

    # Add the main script block
    print("Adding main block...")
    notebook.cells.append(new_markdown_cell(markdown_cells[-1]))
    if sections["main_script"]:
        notebook.cells.append(new_code_cell(sections["main_script"]))
    else:
        print("ERROR: Main block is missing.")

    # Save the updated notebook
    with open(notebook_name, "w") as f:
        nbformat.write(notebook, f)

    print(f"The Jupyter Notebook {notebook_name} created or updated successfully.")

# Run the script
# create_or_update_notebook()
