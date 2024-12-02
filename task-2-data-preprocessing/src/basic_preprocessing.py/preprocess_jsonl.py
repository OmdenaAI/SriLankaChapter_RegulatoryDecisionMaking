# -*- coding: utf-8 -*-
"""
basic_jsonl_processing

"""


import pandas as pd
import json
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")



def load_and_append_jsonl_files(folder_path):
    """
    Loads all JSONL files in a folder, reads them, and appends them into a single DataFrame.

    Renames 'text' to 'text_content' and 'title' to 'retrieved_title'.
    If the fields are missing, assigns None.

    Args:
        folder_path (str): Path to the folder containing the JSONL files.

    Returns:
        pd.DataFrame: A DataFrame containing all the combined data from JSONL files.
    """
    all_data_list = []  # Initialize an empty list to store data from all files

    for filename in os.listdir(folder_path):
        if filename.endswith(".jsonl"):  # Check if the file is a JSONL file
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r") as file:
                for line in file:
                    try:
                        data = json.loads(line)
                        # Rename columns, if they exist, and assign None if not found
                        data["text_content"] = data.get("text", None)
                        data["retrieved_title"] = data.get("title", None)

                        all_data_list.append(data)  # Append the data to the list
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON in file {filename}: {e}")
                    except Exception as e:
                        logging.error(f"Unexpected error processing file {filename}: {e}")

    # Create a DataFrame from the combined data
    all_data_df = pd.DataFrame(all_data_list)
    return all_data_df


# Example usage:
folder_path = "/content/drive/MyDrive/Omdena_Challenge/creating_new_LK_tea_dataset/acts_upto_2019/ACT"  # JSONL files directory
all_data_df = load_and_append_jsonl_files(folder_path)

# Display the first few rows of the combined DataFrame
logging.info("Data loaded successfully.")
logging.info(f"First few rows:\n{all_data_df.head()}")

all_data_df["pdf_or_text"] = "text"


def save_text_content_to_txt(row, index, name="document"):
    """
    Saves the text content of a row to a text file in the specified folder.

    Args:
        row (pd.Series): The row containing the text content and metadata.
        index (int): The index of the row.
        name (str): The base name for the output file. Default is 'document'.

    Returns:
        str: The output file path where the content was saved, or None if not saved.
    """
    file_type = row["pdf_or_text"]  # Should be 'text' for text files
    text_content = row["text_content"]  # The actual text content to be saved

    # Only process rows where PDF_or_text == 'Text'
    if file_type == "text" and text_content:
        # Define the folder path for ACT (use your specified folder path)
        act_folder = "/content/drive/MyDrive/Omdena_Challenge/new_LK_tea_dataset/ACT"

        # Construct the output file path in the ACT folder with a sequential name
        output_file_path = os.path.join(act_folder, f"{name}_{index + 1}.txt")  # Use 1-based index for naming

        # Ensure the ACT folder exists (although it should already exist, we ensure this step)
        if not os.path.exists(act_folder):
            os.makedirs(act_folder)

        # Save the text content to the .txt file
        try:
            with open(output_file_path, "w") as output_file:
                output_file.write(text_content)

            logging.info(f"Saved text content to: {output_file_path}")

            # Return the file path to update the dataset (optional)
            return output_file_path
        except Exception as e:
            logging.error(f"Error saving text content to {output_file_path}: {e}")
            return None
    else:
        return None


# Example usage: Iterate through DataFrame rows and save text content
all_data_df["text_path"] = all_data_df.apply(
    lambda row: save_text_content_to_txt(row, all_data_df.index.get_loc(row.name), "ACT"), axis=1
)

# Add filename column based on text_path
all_data_df["filename"] = all_data_df["text_path"].apply(
    lambda x: os.path.basename(x).split(".")[0] + ".txt" if isinstance(x, str) else None
)

# Assign 'id' if it doesn't exist already
if "id" not in all_data_df.columns:
    all_data_df["id"] = range(1, len(all_data_df) + 1)

# Creating new columns and initializing them with empty values
new_columns = [
    "class",
    "path",
    "data_origin",
    "retrieved_date_of_issuance",
    "issuing_authority",
]
for column in new_columns:
    all_data_df[column] = ""  # Initialize with empty strings

# Reorder the DataFrame
preferred_column_names = [
    "class",
    "filename",
    "path",
    "data_origin",
    "url",
    "retrieved_date_of_issuance",
    "retrieved_title",
    "issuing_authority",
    "text_path",
    "text_content",
]
all_data_df = all_data_df[preferred_column_names]

# Function to chunk long text
def split_long_text(text, max_length=50000):
    """
    Splits long text into smaller chunks to fit the maximum length.

    Args:
        text (str): The text content to split.
        max_length (int): The maximum allowed length for each chunk.

    Returns:
        list: A list of text chunks.
    """
    if isinstance(text, str) and len(text) > max_length:
        return [text[i:i + max_length] for i in range(0, len(text), max_length)]
    return [text]  # Return as a single item list if not long

# Apply the function to split long texts and wrap each chunk in quotes
all_data_df["text_content"] = all_data_df["text_content"].apply(
    lambda x: [f'"{chunk}"' for chunk in split_long_text(x)]
)

# Flatten the list of lists into a single string for CSV output
all_data_df["text_content"] = all_data_df["text_content"].apply(lambda x: ", ".join(x))

# Save the DataFrame to CSV with quoting enabled
csv_file_path = "/content/drive/MyDrive/Omdena_Challenge/creating_new_LK_tea_dataset/acts_upto_2019/new_LK_tea_dataset_acts.csv"
try:
    all_data_df.to_csv(csv_file_path, index=False, quoting=1)  # quoting=1 uses csv.QUOTE_NONNUMERIC
    logging.info(f"Data successfully saved to {csv_file_path}")
except Exception as e:
    logging.error(f"Error saving CSV file: {e}")
