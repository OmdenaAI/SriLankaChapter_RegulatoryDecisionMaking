import asyncio
import os
import aiohttp
import logging
from playwright.async_api import async_playwright
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Function to sanitize file names (removes unwanted characters for file system compatibility)
def sanitize_filename(filename):
    # Replace any character that isn't a letter, number, or basic punctuation with an underscore
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

# Function to create a publication dictionary with dynamic data
def create_publication(pdf_link, pdf_title, document_class, destination_folder):
    # Sanitize the title for use as a filename
    sanitized_title = sanitize_filename(pdf_title) if pdf_title else "Untitled"

    # Extract the date of issuance from the URL (or set to None if not available)
    retrieved_date_of_issuance = extract_date_from_url(pdf_link) if pdf_link else None

    # Static reference number (can be empty or None if not available)
    reference_number = None

    # Create and return the publication dictionary
    publication = {
        "class": document_class if document_class else None,
        "filename": sanitized_title + ".pdf",  # Use the sanitized title as the filename
        "url": pdf_link if pdf_link else None,
        "retrieved_date_of_issuance": retrieved_date_of_issuance,
        "retrieved_title": pdf_title if pdf_title else None,
        "reference_number": reference_number,  # Static field for reference number (or None)
    }

    return publication

# Function to add static data to the publication dictionary (e.g., source, category, keywords)
def add_static_data(publication):
    # Add static data fields (defaults to None if they can't be inferred)
    publication["source"] = "Tea Research Institute" if "Tea Research Institute" else None  # Static source
    publication["category"] = "Research" if "Research" else None  # Static category
    publication["keywords"] = ["Tea", "Research", "Publications"] if ["Tea", "Research", "Publications"] else None  # Static keywords

    # Add additional fields with default None if not available
    publication["created_by"] = "Automated Scraper" if "Automated Scraper" else None  # Static created by info
    publication["language"] = "English" if "English" else None  # Static language field
    
    return publication

# Function to create a folder for the document class if it doesn't exist
def create_folder(destination_folder, document_class):
    subfolder = document_class.lower() if document_class else "unknown"  # Ensure lowercase folder names
    subfolder_path = os.path.join(destination_folder, subfolder)
    os.makedirs(subfolder_path, exist_ok=True)
    return subfolder_path

# Function to download the PDF file and save it to the destination folder using aiohttp
async def download_pdf(pdf_link, destination_folder, filename):
    try:
        file_path = os.path.join(destination_folder, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        async with aiohttp.ClientSession() as session:
            async with session.get(pdf_link) as response:
                if response.status == 200:
                    with open(file_path, 'wb') as f:
                        f.write(await response.read())
                    logger.info(f"Downloaded {filename} to {file_path}")
                else:
                    logger.error(f"Failed to download {filename} from {pdf_link}")
    except Exception as e:
        logger.error(f"Error downloading {filename}: {e}")

# Function to extract date from the URL (YYYY/MM format)
def extract_date_from_url(url):
    match = re.search(r'/(\d{4})/(\d{2})/', url)
    if match:
        year = match.group(1)
        month = match.group(2)
        return f"{year}-{month}"
    return None  # Return None if no date can be extracted

# Function to scrape PDF links and other data from the Tea Board Analytical Lab website
async def get_pdf_links(tri_url, destination_data_folder):
    publications = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            logger.info(f"Scraping page: {tri_url}")
            await page.goto(tri_url)
            await page.wait_for_selector('div.download-file.pdf')  # Wait for download links to load

            # Extract all download links and other details (assuming they are in div.download-file.pdf a tags)
            download_files = await page.query_selector_all('div.download-file.pdf a')

            for file in download_files:
                pdf_link = await file.get_attribute('href')
                pdf_title = await file.inner_text()

                # Determine the document class (e.g., "circular", "guideline", etc.)
                document_class = "circular" if pdf_link else None  # Assuming document class is inferred from the link

                # Generate the publication with dynamic data
                publication = create_publication(pdf_link, pdf_title, document_class, destination_data_folder)

                # Add static data to the publication (e.g., source, category, keywords)
                publication = add_static_data(publication)

                # Store the data in the publications list
                publications.append(publication)

                # Create subfolder for the document class (create "unknown" folder if class is None)
                destination_folder_sub = create_folder(destination_data_folder, document_class)

                # Download the PDF file to the subfolder if a valid link is available
                if pdf_link:
                    await download_pdf(pdf_link, destination_folder_sub, publication["filename"])

            await browser.close()

        return publications
    except Exception as e:
        logger.error(f"Error getting PDF links: {e}")
        raise

# Function to scrape the website and save the data
async def scrape_website(scraping_url, destination_data_folder):
    try:
        destination_folder = os.path.join(destination_data_folder)
        
        # Scrape the PDF links and other data
        result = await get_pdf_links(scraping_url, destination_data_folder)
        logger.info(f"Number of results: {len(result)}")
        logger.info(f"Initial scraping done. Downloading into {destination_folder}")
        logger.info("Downloaded documents")
        return result
    except Exception as e:
        logger.error(f"Error scraping website: {e}")
        raise

# Entry point for the script (if running directly)
if __name__ == "__main__":
    # Replace with the actual website URL and the destination folder for saving files
    scraping_url = 'https://www.srilankateaboard.lk/analytical-laboratory-publications/'  # Example URL for TBA/L
    destination_data_folder = './scraped_data'  # Folder to save the PDFs
    results = asyncio.run(scrape_website(scraping_url, destination_data_folder))
    
    # Print the results
    if results:
        logger.info(f"Scraped {len(results)} records:")
        for result in results:
            logger.info(result)
    else:
        logger.info("No records found.")
