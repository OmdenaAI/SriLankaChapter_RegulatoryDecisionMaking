import os
import sys
import logging
import csv
import asyncio
from datetime import datetime
import src.data_source_tri.tri_scraper
import src.data_source_tbal.tbal_scraper
import src.data_source_egz.egz_scraper

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Directory constants
RAW_INPUT_DIR = "/data/task1_raw_input"
VERSION_PREFIX = "v"
DATA_SOURCES = {
    "tri": os.path.join(RAW_INPUT_DIR, "data_source_tri"),
    "tbal": os.path.join(RAW_INPUT_DIR, "data_source_tbal"),
    "egz": os.path.join(RAW_INPUT_DIR, "data_source_egz"),
}

# Write the scraped data to a CSV file
def write_to_csv(contents, destination_file_relative_path):
    field_names = [
        'class', 'filename', 'PDF_or_text', 'path', 'data_origin', 'url', 'retrieved_date_of_issuance',
        'retrieved_title', 'issuing_authority',
    ]
    
    try:
        with open(destination_file_relative_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(contents)
        logging.info(f"CSV written to {destination_file_relative_path}")
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")
        raise

# Generate CSV filenames dynamically based on version
def generate_csv_filename(data_source, version_folder):
    return os.path.join(DATA_SOURCES[data_source], version_folder, f"{data_source}_{version_folder}_raw.csv")

# Create CSV for a specific data source and version folder
def create_csv_for_version(data_source, version_folder):
    version_folder_path = os.path.join(DATA_SOURCES[data_source], version_folder, "files")
    
    # Assuming the scraped data comes from the files in the "files" folder
    scraped_data = []  # This should contain the scraped data, extracted from the files in the "files" folder.
    
    # Example: populate scraped_data list based on the scraped files
    # scraped_data = [{
    #     'class': 'tri',
    #     'filename': 'file.pdf',
    #     'path': '/path/to/file.pdf',
    #     'url': 'https://example.com',
    #     'data_origin': 'tri',
    #     'retrieved_date_of_issuance': '2022-01-01',
    #     'issuing_authority': 'Tri',
    #     'retrieved_topic': 'Topic',
    #     'PDF_or_text': 'PDF'
    # }]
    
    csv_filename = generate_csv_filename(data_source, version_folder)
    write_to_csv(scraped_data, csv_filename)
    logging.info(f"CSV writing completed for {data_source} version {version_folder}")

# Function to create CSV for all versions of a data source
def create_all_csvs_for_data_source(data_source):
    data_source_path = DATA_SOURCES[data_source]
    existing_versions = [d for d in os.listdir(data_source_path) if d.startswith(VERSION_PREFIX)]
    existing_versions.sort(reverse=True)

    all_scraped_data = []
    
    if existing_versions:
        for version_folder in existing_versions:
            create_csv_for_version(data_source, version_folder)

            # Collecting data for combined CSV from each version
            version_folder_path = os.path.join(DATA_SOURCES[data_source], version_folder, "files")
            # Add scraped data to the all_scraped_data list (this part depends on your scraping implementation)
            # Example: Adding scraped data from the current version
            # all_scraped_data.extend(scraped_data)  # Assuming `scraped_data` is available from your scraping function
        
        # Write the combined CSV after processing all versions
        combined_csv_filename = os.path.join(DATA_SOURCES[data_source], f"{data_source}_combined.csv")
        write_to_csv(all_scraped_data, combined_csv_filename)
        logging.info(f"Combined CSV written to {combined_csv_filename}")
        
    else:
        logging.warning(f"No versions found for {data_source}.")

# Get the next version folder
def get_next_version(version_folder):
    base, version = version_folder.split("_")
    major, minor = map(int, version.split("."))
    next_version = f"{base}_{major + 1}_{minor}"
    return next_version

# Check for new files
def check_new_files(scraped_folder, existing_folder):
    existing_files = set(os.listdir(existing_folder))
    scraped_files = set(os.listdir(scraped_folder))
    new_files = scraped_files - existing_files
    return bool(new_files), new_files

# Create version folder and scrape data if new files are found
def create_version_folder_and_scrape(data_source, scrape_func, url):
    data_source_path = DATA_SOURCES[data_source]
    existing_versions = [d for d in os.listdir(data_source_path) if d.startswith(VERSION_PREFIX)]
    existing_versions.sort(reverse=True)

    if existing_versions:
        latest_version_folder = existing_versions[0]
    else:
        latest_version_folder = f"{VERSION_PREFIX}0_0"

    next_version = get_next_version(latest_version_folder)
    save_path = os.path.join(data_source_path, latest_version_folder, "files")
    new_files_found, new_files = check_new_files(save_path, os.path.join(data_source_path, next_version))

    if new_files_found:
        logging.info(f"New files found in {data_source}. Scraping data...")
        scraped_data = asyncio.run(scrape_func(url, save_path, next_version))
        logging.info(f"Scraping completed for {data_source} version {next_version}")
    else:
        logging.info(f"No new files for {data_source}. Skipping scraping.")

# Add the scraping functions (these should be implemented in the respective scraper files)
async def scrape_tri(url, destination_folder, version):
    # Implement scraping logic for TRI (using the tri_scraper module)
    return await src.data_source_tri.tri_scraper.scrape(url, destination_folder, version)

async def scrape_tba(url, destination_folder, version):
    # Implement scraping logic for TBA/L (using the tbal_scraper module)
    return await src.data_source_tbal.tbal_scraper.scrape(url, destination_folder, version)

async def scrape_exgazette(base_url, page_url, destination_folder):
    # Implement scraping logic for Ex Gazette (using the egz_scraper module)
    return await src.data_source_egz.egz_scraper.scrape(base_url, page_url, destination_folder)

# Main function to run the scrapers and generate CSVs
async def main():
    try:
        # First, scrape for TRI
        tri_url = 'https://www.tri.lk/view-all-publications/'  # URL for TRI
        tri_destination_data_folder = './scraped_data'  # Folder to save the PDFs
        results_tri = await scrape_tri(tri_url, tri_destination_data_folder, "v1_0")
        # Save results to CSV
        create_all_csvs_for_data_source("tri")
        logging.info(f"Scraped {len(results_tri)} records for TRI.")

        # Second, scrape for TBA/L
        scraping_url_tba = 'https://www.srilankateaboard.lk/analytical-laboratory-publications/'  # URL for TBA/L
        tba_destination_data_folder = './scraped_data'  # Folder to save the PDFs
        results_tba = await scrape_tba(scraping_url_tba, tba_destination_data_folder, "v1_0")
        # Save results to CSV
        create_all_csvs_for_data_source("tbal")
        logging.info(f"Scraped {len(results_tba)} records for TBA/L.")

        # Third, scrape for Ex Gazette
        base_url = 'http://documents.gov.lk/en'  # Base URL for the website
        page_url = 'http://documents.gov.lk/en/exgazette.php'  # URL of the Ex Gazette page
        destination_folder = './scraped_data'  # Folder to save downloaded PDFs
        results_exgazette = await scrape_exgazette(base_url, page_url, destination_folder)
        # Save results to CSV
        create_all_csvs_for_data_source("egz")
        logging.info(f"Scraped {len(results_exgazette)} records for Ex Gazette.")

    except Exception as e:
        logging.error(f"Error running scrapers: {e}")

if __name__ == "__main__":
    # Run all scrapers and CSV generation
    asyncio.run(main())


# -*- coding: utf-8 -*-
"""
basic_jsonl_processing

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1q0GLfVJfKqU5m2x-zFVtwqgp1EIHoobT
"""

from google.colab import drive
import pandas as pd
import json
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Mount Google Drive
drive.mount("/content/drive")


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
