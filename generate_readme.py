import os
import ast

def extract_docstrings(file_path):
    """Extract module and function-level docstrings from a Python file."""
    with open(file_path, "r") as file:
        tree = ast.parse(file.read())
    
    docs = {"module": ast.get_docstring(tree), "functions": []}
    
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):  # Extract function docstrings
            func_doc = ast.get_docstring(node)
            docs["functions"].append({
                "name": node.name,
                "doc": func_doc
            })
    return docs

def generate_readme(folder_path, output_file="README.md"):
    """Generate a README.md file from docstrings in the folder."""
    readme_content = "# ETL Pipeline Documentation\n\n"
    readme_content += "This document is auto-generated from the codebase.\n\n"

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".py") and not file.startswith("__"):
                file_path = os.path.join(root, file)
                docs = extract_docstrings(file_path)
                
                # Add module-level docstring
                readme_content += f"## {file}\n\n"
                if docs["module"]:
                    readme_content += f"{docs['module']}\n\n"
                else:
                    readme_content += "No module-level documentation available.\n\n"
                
                # Add function-level docstrings
                for func in docs["functions"]:
                    readme_content += f"### Function: `{func['name']}`\n\n"
                    if func["doc"]:
                        readme_content += f"{func['doc']}\n\n"
                    else:
                        readme_content += "No documentation available.\n\n"
    
    # Write to README.md
    with open(output_file, "w") as readme_file:
        readme_file.write(readme_content)
    print(f"README.md generated at {output_file}")

if __name__ == "__main__":
    generate_readme("etl_pipeline")
