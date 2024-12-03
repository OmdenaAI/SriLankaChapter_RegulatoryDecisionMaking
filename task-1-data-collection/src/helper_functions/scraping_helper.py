import asyncio
import os
import re
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import aiohttp
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
import os
import sys
import logging
import csv
import asyncio
from datetime import datetime
from google.colab import drive
import pandas as pd
import json
import os
import logging
import src.data_source_tri.tri_scraper
import src.data_source_tbal.tbal_scraper
import src.data_source_egz.egz_scraper


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Function to create a publication entry
def create_publication_entry(document_class, file_type, filename, file_path, file_link, title, date_of_issuance, reference_number):
    return {
        "class": document_class if document_class else None,
        "file_type": file_type if file_type else None,
        "file_name": filename if filename else "Untitled.pdf",  # Default to "Untitled.pdf" if title is None
        "file_path": file_path if file_path else None,
        "url": file_link if file_link else None,
        "retrieved_date_of_issuance": date_of_issuance if date_of_issuance else None,
        "retrieved_title": title if title else None,
        "reference_number": reference_number if reference_number else None,  # Set None if reference number is unavailable
    }

    
# Function to create a subfolder based on document class
def create_subfolder(destination_folder, document_class):
    subfolder = document_class.lower() if document_class else "unknown"  # Default to "unknown" if class is None
    subfolder_path = os.path.join(destination_folder, subfolder)
    os.makedirs(subfolder_path, exist_ok=True)
    return subfolder_path

# Utility function to sanitize file names
def sanitize_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

# Extract date from the URL (YYYY/MM format)
def extract_date_from_url(url):
    match = re.search(r'/(\d{4})/(\d{2})/', url)
    if match:
        year = match.group(1)
        month = match.group(2)
        return f"{year}-{month}"
    return None


# Function to download PDF from URL and save it to the destination folder
async def download_pdf(file_link, file_path, file_name):
    try:
        # Make sure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Download the PDF asynchronously using aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(file_link) as response:
                if response.status == 200:
                    with open(file_path, 'wb') as f:
                        f.write(await response.read())
                    logger.info(f"Downloaded {file_name} to {file_path}")
                else:
                    logger.error(f"Failed to download {file_name} from {file_link}")
    except Exception as e:
        logger.error(f"Error downloading {file_name}: {e}")


# Function to fetch and parse robots.txt to check if scraping is allowed
async def fetch_robots_txt(base_url):
    robots_url = base_url + "/robots.txt"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(robots_url) as response:
                if response.status == 200:
                    robots_txt = await response.text()
                    return robots_txt
                else:
                    logger.warning(f"Could not fetch robots.txt from {robots_url}")
                    return None
    except Exception as e:
        logger.error(f"Error fetching robots.txt: {e}")
        return None

# Function to check if the URL is allowed to be scraped based on robots.txt
def is_url_allowed_by_robots(robots_txt, user_agent, url):
    rp = RobotFileParser()
    rp.parse(robots_txt.splitlines())
    return rp.can_fetch(user_agent, url)



    # Scrape the website and save the data
async def scrape_website(scraping_function, page_url, destination_data_folder, document_class):
    try:
        base_url = urlparse(page_url).scheme + "://" + urlparse(page_url).hostname

        # Fetch and parse robots.txt
        robots_txt = await fetch_robots_txt(base_url)

        if robots_txt:
            if not is_url_allowed_by_robots(robots_txt, "*", page_url):
                logger.warning(f"URL {page_url} is disallowed by robots.txt")
                return []

        # If robots.txt allows scraping, proceed with scraping
        result = await scraping_function(base_url, page_url, destination_data_folder, document_class)
        logger.info(f"Number of results: {len(result)}")
        logger.info(f"Initial scraping done. Downloading into {destination_data_folder}")
        logger.info("Downloaded documents")
        return result
    except Exception as e:
        logger.error(f"Error scraping website: {e}")
        raise


if __name__ == "__main__":
    # # Run all scrapers and CSV generation
    # asyncio.run(main())

 