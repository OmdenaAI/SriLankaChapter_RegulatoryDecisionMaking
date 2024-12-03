

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

# Sample function to save the DataFrame to CSV with markdown preservation
def save_dataframe_to_csv(df, filename):
    df.to_csv(filename,
              index=False,  # Do not write row indices
              quoting=csv.QUOTE_MINIMAL,  # Quote fields that contain special characters
              quotechar='"',  # Use double quotes to wrap fields
              escapechar='\\',  # Escape any quotes inside markdown content
              lineterminator='\n',  # Make sure to preserve newlines in markdown content
              encoding='utf-8')  # Ensure proper encoding to handle special characters in markdown

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

        act_folder = "/content/drive/MyDrive/Omdena_Challenge/new_LK_tea_dataset/ACT"

    except Exception as e:
        logging.error(f"Error running scrapers: {e}")

if __name__ == "__main__":
    # Run all scrapers and CSV generation
    asyncio.run(main())

    
# Directory constants
RAW_INPUT_DIR = "/data/task1_raw_input"
VERSION_PREFIX = "v"
DATA_SOURCES = {
    "tri": os.path.join(RAW_INPUT_DIR, "data_source_tri"),
    "tbal": os.path.join(RAW_INPUT_DIR, "data_source_tbal"),
    "egz": os.path.join(RAW_INPUT_DIR, "data_source_egz"),
}


   