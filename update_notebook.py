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
        notebook.cells = []  # Start with an empty notebook

    # Remove existing code cells to prevent duplication
    notebook.cells = [cell for cell in notebook.cells if cell.cell_type == "markdown"]

    # Add libraries and logging
    print("Adding libraries and logging...")
    libraries_code = sections["libraries_and_logging"]
    if libraries_code:
        notebook.cells.append(new_code_cell(libraries_code, metadata={"section": "libraries_and_logging"}))

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

    for func_name in function_names:
        print(f"Adding or updating function: {func_name}...")
        func_body = sections["functions"].get(func_name)
        if func_body:
            notebook.cells.append(new_code_cell(func_body, metadata={"section": func_name}))

    # Add or update the main script block
    print("Adding main block...")
    main_code = sections["main_script"]
    if main_code:
        notebook.cells.append(new_code_cell(main_code, metadata={"section": "main_script"}))

    # Save the updated notebook
    with open(notebook_name, "w") as f:
        nbformat.write(notebook, f)

    print(f"Notebook {notebook_name} created or updated successfully.")

# Run the script
create_or_update_notebook()
