import asyncio
import os
import requests
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Function to download the PDF file and save it to the destination folder
async def download_pdf(pdf_link, destination_folder, filename):
    try:
        # Create the full file path
        file_path = os.path.join(destination_folder, filename)
        
        # Make sure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Download the PDF and save it
        response = await requests.get(pdf_link)
        
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded {filename} to {file_path}")
        else:
            logger.error(f"Failed to download {filename} from {pdf_link}")
    except Exception as e:
        logger.error(f"Error downloading {filename}: {e}")

# Function to extract date from URL (YYYY/MM)
def extract_date_from_url(url):
    # Use regex to extract the year and month from the URL
    match = re.search(r'/(\d{4})/(\d{2})/', url)
    if match:
        year = match.group(1)
        month = match.group(2)
        return f"{year}-{month}"
    return "Not Available"

# Function to scrape PDF links and other data from the Tea Board Analytical Lab (TBA/L) website
async def get_pdf_links(tri_url, destination_data_folder):
    publications = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            logger.info(f"Scraping page: {tri_url}")
            await page.goto(tri_url)
            await page.wait_for_timeout(5000)

            # Extract all download links and other details (assuming they are in div.download-file.pdf a tags)
            download_files = await page.query_selector_all('div.download-file.pdf a')

            for file in download_files:
                pdf_link = await file.get_attribute('href')
                pdf_title = await file.inner_text()

                # Extract the date of issuance from the URL
                retrieved_date_of_issuance = extract_date_from_url(pdf_link)

                publications.append(
                    {
                        "url": pdf_link,
                        "retrieved_date_of_issuance": retrieved_date_of_issuance,
                        "retrieved_title": pdf_title,
                    }
                )

                # Download the PDF to the appropriate folder (guideline or circular, for now assuming guidelines)
                # Let's assume all files go into the 'guideline' folder for now
                subfolder = "guideline"
                destination_folder_sub = os.path.join(destination_data_folder, subfolder)
                
                # Download the PDF file
                await download_pdf(pdf_link, destination_folder_sub, pdf_title + ".pdf")

            await browser.close()

        return publications
    except Exception as e:
        logger.error(f"Error getting PDF links: {e}")
        raise

# Function to scrape the website and save the data
async def scrape_website(tri_url, destination_data_folder):
    try:
        destination_folder = os.path.join(destination_data_folder)
        
        # Scrape the PDF links and other data
        result = await get_pdf_links(tri_url, destination_folder)
        logger.info(f"Number of results: {len(result)}}")
        logger.info(f"Initial scraping done. Downloading into {destination_folder}")
        logger.info("Downloaded documents")
        return 
    except Exception as e:
        logger.error(f"Error scraping website: {e}")
        raise

# Entry point for the script (if running directly)
if __name__ == "__main__":
    pass
