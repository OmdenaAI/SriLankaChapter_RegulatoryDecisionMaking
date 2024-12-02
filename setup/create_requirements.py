import os
import ast
import subprocess

def get_imports_from_file(file_path):
    """Extract all imports from a Python file."""
    imports = set()
    with open(file_path, 'r', encoding='utf-8') as file:
        tree = ast.parse(file.read(), filename=file_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module)
    return imports

def get_all_python_files(directory):
    """Recursively find all Python files in a directory."""
    python_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files

import os
import subprocess

def get_all_python_files(directory):
    """Recursively find all Python files in a given directory."""
    python_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files

def get_imports_from_file(file_path):
    """Extract import statements from a Python file."""
    imports = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('import ') or line.startswith('from '):
                    # Normalize import to lowercase to avoid case sensitivity issues
                    imports.add(line.strip().split()[1].lower())
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    return imports

def generate_requirements_txt(directory):
    """Generate requirements.txt from imports in Python files and installed packages."""
    # Get all Python files in the directory and subdirectories
    python_files = get_all_python_files(directory)
    
    # Extract imports from all Python files
    all_imports = set()
    for file in python_files:
        imports = get_imports_from_file(file)
        all_imports.update(imports)
    
    # Use pip freeze to get the installed packages
    try:
        installed_packages = subprocess.check_output(['pip', 'freeze']).decode('utf-8').splitlines()
    except subprocess.CalledProcessError as e:
        print(f"Error running pip freeze: {e}")
        return
    
    # Create a list of packages that are both imported and installed
    requirements = []
    for package in installed_packages:
        package_name = package.split('==')[0].lower()  # Normalize to lowercase for comparison
        if package_name in [imp.lower() for imp in all_imports]:
            requirements.append(package)
    
    # Function to write requirements.txt in a given folder
    def write_requirements_txt(folder_path):
        """Write the requirements to a requirements.txt file."""
        requirements_path = os.path.join(folder_path, 'requirements.txt')
        with open(requirements_path, 'w', encoding='utf-8') as req_file:
            for package in sorted(requirements):
                req_file.write(package + '\n')
        print(f"requirements.txt has been generated in {folder_path}")

    # Traverse second-level subdirectories and generate requirements.txt in each
    for subdir in os.listdir(directory):
        subdir_path = os.path.join(directory, subdir)
        if os.path.isdir(subdir_path):
            for second_level_subdir in os.listdir(subdir_path):
                second_level_subdir_path = os.path.join(subdir_path, second_level_subdir)
                if os.path.isdir(second_level_subdir_path):
                    write_requirements_txt(second_level_subdir_path)

    # After generating in second-level subdirectories, generate the requirements.txt in the root directory
    write_requirements_txt(directory)

if __name__ == "__main__":
    # Get the parent directory of the script (root directory)
    parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # Call the function to generate requirements.txt in the root and second-level subfolders
    generate_requirements_txt(parent_directory)
