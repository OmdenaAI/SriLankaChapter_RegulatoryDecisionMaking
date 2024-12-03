import asyncio
import os
import aiohttp
import logging
from playwright.async_api import async_playwright
import re
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)




# Scrape PDF links and other data
async def scrape_tbal(page_url, destination_data_folder, document_class):
    publications = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            logger.info(f"Scraping page: {page_url}")
            await page.goto(page_url)
            await page.wait_for_selector('div.download-file.pdf')

            download_files = await page.query_selector_all('div.download-file.pdf a')

            for file in download_files:
                pdf_link = await file.get_attribute('href')
                pdf_title = await file.inner_text()
                
                sanitized_title = sanitize_filename(pdf_title) if pdf_title else "Untitled"
                retrieved_date_of_issuance = extract_date_from_url(pdf_link) if pdf_link else None
                reference_number = None

                # Here, document_class is passed from the main function
                publication = create_publication(pdf_link, pdf_title, document_class, destination_data_folder)
                publication = add_static_data(publication)
                publications.append(publication)

                destination_folder_sub = await create_folder(destination_data_folder, document_class)

                if pdf_link:
                    await download_pdf(pdf_link, destination_folder_sub, publication["filename"])

            await browser.close()
        return publications
    except Exception as e:
        logger.error(f"Error getting PDF links: {e}")
        raise




# Entry point for the script
if __name__ == "__main__":
    # Define the scraping URL, destination folder, and the document class from the main entry point
    scraping_url = 'https://www.srilankateaboard.lk/analytical-laboratory-publications/'
    destination_data_folder = './scraped_data'

    # Define the document class from the main function
    document_class = "circular"  # You can change this to other values, like "guideline", based on your needs
    # Function to add static data to the publication entry (if any)
    def add_static_data(publication):
        publication["issuing_authority"] = "Government of Sri Lanka"
        publication["data_origin"] = "scraped"
        return publication

    # Run the scraper with the document class
    results = asyncio.run(scrape_website(scrape_tbal, scraping_url, destination_data_folder, document_class))

    if results:
        logger.info(f"Scraped {len(results)} records:")
        for result in results:
            logger.info(result)
    else:
        logger.info("No records found.")
