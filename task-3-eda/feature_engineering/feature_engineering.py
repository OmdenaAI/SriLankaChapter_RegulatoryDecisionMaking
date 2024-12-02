# -*- coding: utf-8 -*-
"""feature_engineering.py

Feature engineering: Update classes to regulatory (ACT + Regulation), circular, guideline and rename old classes 
such as ACT, Regulation, Circulers, Guideline to original_classes.

This script processes a dataset and updates class labels based on predefined mappings.
"""

import logging
import pandas as pd
from typing import Dict

# Set up logging for better error tracking
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def update_class(original_class: str, class_mapping: Dict[str, str]) -> str:
    """
    Update the 'class' based on the 'original_class' using a predefined mapping.

    Args:
        original_class (str): The original class string from the 'original_class' column.
        class_mapping (dict): A dictionary mapping old class names to new class names.

    Returns:
        str: The updated class name based on the original class.
    """
    try:
        # Iterate through the class_mapping to find the matching class
        for old_class, new_class in class_mapping.items():
            if old_class.lower() in original_class.lower():
                return new_class
        return ""  # Return an empty string if no matching class is found
    except Exception as e:
        logging.error(f"Error in update_class function: {e}")
        return ""  # Return an empty string in case of an error


def process_data(all_data: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the data by updating the 'class' column based on the 'original_class' column.

    Args:
        all_data (pd.DataFrame): The DataFrame containing the original data with the 'class' column.

    Returns:
        pd.DataFrame: The updated DataFrame with the new 'class' column.
    """
    try:
        # Rename the 'class' column to 'original_class'
        all_data.rename(columns={"class": "original_class"}, inplace=True)
        logging.info("Renamed 'class' column to 'original_class'")

        # Insert a new 'class' column immediately after 'original_class'
        class_column_index = all_data.columns.get_loc("original_class") + 1
        all_data.insert(class_column_index, "class", "")
        logging.info("Inserted empty 'class' column after 'original_class'")

        # Define a mapping of old class names to new class names
        class_mapping = {
            "ACT": "regulatory",
            "regulation": "regulatory",
            # "Circulers": "circular",
            # "Guideline": "guideline",
        }

        # Apply the function to update the 'class' column
        all_data["class"] = all_data["original_class"].apply(
            lambda x: update_class(x, class_mapping)
        )
        logging.info("Updated 'class' column based on 'original_class'")

        # Display the updated DataFrame
        logging.info(f"First few rows of the updated DataFrame:\n{all_data.head()}")
        logging.info(f"Unique values in 'class' column: {all_data['class'].unique()}")

        return all_data

    except KeyError as key_error:
        logging.error(f"Key error during data processing: {key_error}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during data processing: {e}")
        raise


def save_data(all_data: pd.DataFrame, path_to_save: str) -> None:
    """
    Saves the processed DataFrame to a CSV file.

    Args:
        all_data (pd.DataFrame): The DataFrame to be saved.
        path_to_save (str): The path where the CSV file should be saved.

    Returns:
        None
    """
    try:
        all_data.to_csv(path_to_save, index=False, quoting=1)
        logging.info(f"Saved the updated DataFrame to {path_to_save}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {path_to_save}: {e}")
        raise


def main(input_file: str, output_file: str) -> None:
    """
    Main function that orchestrates the feature engineering workflow: reading the data, processing it, and saving it.

    Args:
        input_file (str): Path to the input CSV file containing the raw data.
        output_file (str): Path to the output CSV file to save the processed data.

    Returns:
        None
    """
    try:
        # Load the data from the CSV file
        logging.info(f"Loading data from {input_file}")
        all_data = pd.read_csv(input_file)

        # Process the data to update the 'class' column
        all_data = process_data(all_data)

        # Save the updated DataFrame to a new CSV file
        save_data(all_data, output_file)

    except FileNotFoundError as fnf_error:
        logging.error(f"File not found: {fnf_error}")
    except pd.errors.EmptyDataError as empty_error:
        logging.error(f"Data loading error: {empty_error}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main function: {e}")


if __name__ == "__main__":
    # Example usage
    input_file = "/path/to/your/input_file.csv"  # Replace with actual input file path
    output_file = "/path/to/your/output_file.csv"  # Replace with actual output file path
    main(input_file, output_file)
