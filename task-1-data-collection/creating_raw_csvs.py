"""
This file creates csvs for the raw data.
"""
import os
import csv
import json

SCRIPT_PATH = os.path.dirname(__file__)
CSV_PHYSICAL_ARCHIVES_RELATIVE_PATH = '..\\data\\task1_raw_input\\data_source_physical_archives_manual\\v0_0\\v0_0_LK_tea_phyarchive_raw.csv'
CSV_LAWNET_RELATIVE_PATH = '..\\data\\task1_raw_input\\data_source_lawnet\\v0_0\\v0_0_LK_tea_lawnet_raw.csv'
CSV_EGZ_RELATIVE_PATH = '..\\data\\task1_raw_input\\data_source_egz\\v0_0\\v0_0_LK_tea_egz_raw.csv'
CSV_TBAL_RELATIVE_PATH = '..\\data\\task1_raw_input\\data_source_tbal\\v0_0\\v0_0_LK_tea_tbal_raw.csv'
CSV_TRI_RELATIVE_PATH = '..\\data\\task1_raw_input\\data_source_tri\\v0_0\\v0_0_LK_tea_tri_raw.csv'
CSV_COMBINED_RELATIVE_PATH = '..\\data\\task1_raw_input\\v0_0_LK_combined_raw.csv'


def write_dict_to_csv(contents, destination_file_relative_path):
    output_csv = os.path.join(SCRIPT_PATH, destination_file_relative_path)
    field_names = ['class', 'filename', 'path', 'url', 'data_origin', 'retrieved_date_of_issuance', 'issuing_authority', 'retrieved_topic', 'PDF_or_text']
    with open(output_csv, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()  # Write the header row
        writer.writerows(contents)  # Write the data rows

def trim_absolute_path(file_path):
    trimmed_path = None
    file_path_mod = file_path.split('SriLankaChapter_RegulatoryDecisionMaking')
    if len(file_path_mod) > 1:
        trimmed_path = file_path_mod[1].replace('\\task-1-data-collection\..', '')
    return trimmed_path

def create_lawnet_files_from_json(destination_csv_relative_path):
    # Creates text files from information in the scraped jsons
    # Also creates lawnet_acts.csv
    all_data = []
    # Define the root folder
    root_folder = os.path.join(SCRIPT_PATH, '..\\data\\task1_raw_input\\data_source_lawnet\\v0_0\\files\\Act')
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.jsonl'):  # Check if the file is a JSONL file
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as file:
                    for line in file:
                        try:
                            data = json.loads(line)
                            all_data.append(data)  # Append the data to the list
                        except json.JSONDecodeError as e:
                            print(f'Error decoding JSON in file {file}: {e}')

    # create a csv with class, file name, path, data origin, issuing authority, url
    # putting issuing  authority as LAwnetb now, this is wrong
    file_infos = []
    for row in all_data:
        filename = f'ACT_{all_data.index(row) + 1}.txt'
        output_file_path = os.path.join(root_folder, filename)  # Use 1-based index for naming
        # Save the text content to the .txt file
        with open(output_file_path, 'w') as output_file:
            output_file.write(row['text'])
        output_file_path_short = trim_absolute_path(output_file_path)
        file_infos.append({'class': 'Act',
                          'filename': filename,
                          'path': output_file_path_short,
                          'url': row['url'],
                          'data_origin': 'scraped',
                          'retrieved_date_of_issuance': '',
                          'issuing_authority': 'Parliament of Sri Lanka ACT',
                          'retrieved_topic': '',
                          'PDF_or_text': 'Text'})

    # Write to a CSV file
    write_dict_to_csv(file_infos, destination_csv_relative_path)

def create_egz_csv(destination_csv_relative_path):
    file_infos = []
    # Define the root folder
    root_folder = os.path.join(SCRIPT_PATH, '..\\data\\task1_raw_input\\data_source_egz\\v0_0\\files\\Regulation')
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.pdf'):  # Extraordinary gazettes are pdfs
                file_path_short = trim_absolute_path(os.path.join(root, file))
                file_infos.append({'class': 'Regulation',
                          'filename': file,
                          'path': file_path_short,
                          'url': '',
                          'data_origin': 'scraped',
                          'retrieved_date_of_issuance': '',
                          'issuing_authority': 'Government of Sri Lanka egz',
                          'retrieved_topic': '',
                          'PDF_or_text': 'PDF'})

    # Write to a CSV file
    write_dict_to_csv(file_infos, destination_csv_relative_path)


def create_tbal_csv(destination_csv_relative_path):
    file_infos = []
    # Define the root folder
    root_folder = os.path.join(SCRIPT_PATH, '..\\data\\task1_raw_input\\data_source_tbal\\v0_0\\files\\Circulers')
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.pdf'):
                file_path_short = trim_absolute_path(os.path.join(root, file))
                file_infos.append({'class': 'Circulers',
                          'filename': file,
                          'path': file_path_short,
                          'url': '',
                          'data_origin': 'scraped',
                          'retrieved_date_of_issuance': '',
                          'issuing_authority': 'Tea Board Analytical Lab Circulers',
                          'retrieved_topic': '',
                          'PDF_or_text': 'PDF'})

    # Write to a CSV file
    write_dict_to_csv(file_infos, destination_csv_relative_path)

def create_tri_csv():
    pass  # this is done in tri_scraper.py

def create_phyarch_csv(destination_csv_relative_path):
    file_infos = []
    # Define the root folder
    root_folder = os.path.join(SCRIPT_PATH, '..\\data\\task1_raw_input\\data_source_physical_archives_manual\\v0_0\\files\\Circulers')
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            file_path_short = trim_absolute_path(os.path.join(root, file))
            file_infos.append({'class': 'Circulers',
                        'filename': file,
                        'path': file_path_short,
                        'url': '',
                        'data_origin': 'original_dataset',
                        'retrieved_date_of_issuance': '',
                        'issuing_authority': 'Tea Board Circulers',
                        'retrieved_topic': '',
                        'PDF_or_text': ''})   # PDF_or_text is left blank because it requires some more steps to decide this for the manual documents

    # Write to a CSV file
    write_dict_to_csv(file_infos, destination_csv_relative_path)


def combine_csv_files(file_list, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = None
        for file in file_list:
            with open(file, 'r', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                header = next(reader)
                if writer is None:
                    writer = csv.writer(outfile)
                    writer.writerow(header)
                writer.writerows(reader)
    print(f"Combined CSV file saved to: {output_file}")


if __name__ == "__main__":
    create_lawnet_files_from_json(CSV_LAWNET_RELATIVE_PATH)
    create_egz_csv(CSV_EGZ_RELATIVE_PATH)
    create_tbal_csv(CSV_TBAL_RELATIVE_PATH)
    create_phyarch_csv(CSV_PHYSICAL_ARCHIVES_RELATIVE_PATH)
    # TRI csv is created from tri_scraper.py

    # combine the files into one csv
    csv_paths = [
        os.path.join(SCRIPT_PATH, CSV_LAWNET_RELATIVE_PATH),
        os.path.join(SCRIPT_PATH, CSV_EGZ_RELATIVE_PATH),
        os.path.join(SCRIPT_PATH, CSV_TBAL_RELATIVE_PATH),
        os.path.join(SCRIPT_PATH, CSV_TRI_RELATIVE_PATH),
        os.path.join(SCRIPT_PATH, CSV_PHYSICAL_ARCHIVES_RELATIVE_PATH)
    ]
    destination_csv = os.path.join(SCRIPT_PATH, CSV_COMBINED_RELATIVE_PATH)

    combine_csv_files(csv_paths, destination_csv)
