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

def generate_requirements_txt(directory):
    """Generate requirements.txt from imports in Python files."""
    # Get all Python files in the directory
    python_files = get_all_python_files(directory)
    
    # Extract imports from all Python files
    all_imports = set()
    for file in python_files:
        imports = get_imports_from_file(file)
        all_imports.update(imports)
    
    # Use pip freeze to get the installed packages
    installed_packages = subprocess.check_output(['pip', 'freeze']).decode('utf-8').splitlines()
    
    # Create a list of packages that are both imported and installed
    requirements = []
    for package in installed_packages:
        package_name = package.split('==')[0].lower()  # Normalize to lowercase for comparison
        if package_name in [imp.lower() for imp in all_imports]:
            requirements.append(package)
    
    # Function to write requirements.txt in a given folder
    def write_requirements_txt(folder_path):
        requirements_path = os.path.join(folder_path, 'requirements.txt')
        with open(requirements_path, 'w', encoding='utf-8') as req_file:
            for package in sorted(requirements):
                req_file.write(package + '\n')
        print(f"requirements.txt has been generated in {folder_path}")

    # Generate requirements.txt in the root directory
    write_requirements_txt(directory)

    # Now, get all second-level subfolders (immediate subfolders of the root)
    for subdir in os.listdir(directory):
        subdir_path = os.path.join(directory, subdir)
        if os.path.isdir(subdir_path):
            # Check if this is a second-level subfolder (it should be 2 levels deep from the root)
            for second_level_subdir in os.listdir(subdir_path):
                second_level_subdir_path = os.path.join(subdir_path, second_level_subdir)
                if os.path.isdir(second_level_subdir_path):
                    write_requirements_txt(second_level_subdir_path)

# Call the function to generate requirements.txt in the root and second-level subfolders
generate_requirements_txt('/path/to/your/project')  # Replace with your project directory path
