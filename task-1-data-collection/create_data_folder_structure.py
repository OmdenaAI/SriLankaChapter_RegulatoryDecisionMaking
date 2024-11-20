"""
This file creates a directory structure for the data folder where raw files and any csvs
created by the project will be stored.
"""
import os

# Function to create directories for v0.0 without creating any CSVs
def create_directories():
    # Create directories for data Task 1 and Task 2

    # NOTE: These are relative paths, so if this script file is moved then the following paths should also be change accordingly.
    script_path = os.path.dirname(__file__)
    task1_path = os.path.join(script_path, "../data/task1_raw_input")
    task2_path = os.path.join(script_path, "../data/task2_preprocessed_data")
    final_dataset_path = os.path.join(script_path, "../data/final_dataset")

    data_sources = ['data_source_physical_archive_manual',
                    'data_source_tri',
                    'data_source_lawnet',
                    'data_source_tbal',
                    'data_source_egz']

    # Create structure for v0_0 only
    os.makedirs(task1_path, exist_ok=True)
    for source in data_sources:
        os.makedirs(os.path.join(task1_path, source), exist_ok=True)
        os.makedirs(os.path.join(task1_path, source, 'v0_0', 'files'), exist_ok=True)

    # Create Week 2 structure for v0_0 only
    os.makedirs(task2_path, exist_ok=True)
    for source in data_sources:
        os.makedirs(os.path.join(task2_path, source), exist_ok=True)
        os.makedirs(os.path.join(task2_path, source, 'v0_0', 'files'), exist_ok=True)

    # Create final dataset structure for v0 only
    os.makedirs(final_dataset_path, exist_ok=True)
    os.makedirs(os.path.join(final_dataset_path, 'v0_LK_tea_dataset'), exist_ok=True)


if __name__ == "__main__":
    # Call the function to create the structure only for v0.0 without creating any CSVs
    create_directories()
