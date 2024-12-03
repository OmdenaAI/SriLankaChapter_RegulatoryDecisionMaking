import asyncio
import os
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import aiohttp
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Function to extract and save the relevant details from each table row
async def scrpae_egz(base_url, page_url, destination_folder, document_class):
    publications = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate to the Ex Gazette page
            logger.info(f"Scraping page: {page_url}")
            await page.goto(page_url)
            await page.wait_for_timeout(5000)

            # Click the "By Content" tab to reveal the content-based search
            await page.click('#lnkContent')
            await page.wait_for_timeout(3000)  # Wait for the tab content to load

            # Get the page content and parse it with BeautifulSoup
            soup = BeautifulSoup(await page.content(), 'html.parser')

            # Find the table rows containing the documents
            rows = soup.select('table#tblExg tr.tr')

            for row in rows:
                # Extract reference number, date, description (title), and the English PDF link
                reference_number = row.select_one('td[width="10%"]').get_text(strip=True) if row.select_one('td[width="10%"]') else None
                date_of_issuance = row.select('td[width="10%"]')[1].get_text(strip=True) if len(row.select('td[width="10%"]')) > 1 else None
                title = row.select_one('td[width="65%"]').get_text(strip=True) if row.select_one('td[width="65%"]') else None
                file_link_tag = row.select_one('a.btn.btn-xs.btn-primary[href$="E.pdf"]')

                # Ensure file_link_tag exists before proceeding
                if not file_link_tag:
                    continue

                # create information we need to use 
                file_name = file_link_tag['href'].split('/')[-1]  # Ensure filename extraction is clean
                file_type = file_name.split('.')[-1]
                
                # Prepare the subfolder based on the document class
                destination_folder_sub = create_subfolder(destination_folder, document_class)
                file_path = os.path.join(destination_folder_sub, file_name)
                file_link = base_url + file_link_tag['href']  # Construct full URL for the PDF

                # Create a publication entry
                publication = create_publication_entry(document_class, file_type, file_name, file_path, file_link, title, date_of_issuance, reference_number)

                # Add static data to the publication entry (if any)
                publication = add_static_data(publication)

                # Add the publication to the list
                publications.append(publication)

                # Download the PDF file asynchronously
                await download_pdf(file_link, file_path, file_name)

            await browser.close()

        return publications
    except Exception as e:
        logger.error(f"Error scraping the page: {e}")
        raise


# Entry point of the script
if __name__ == "__main__":
    
    page_url = 'http://documents.gov.lk/en/exgazette.php'  # URL of the Ex Gazette page
    destination_folder = './scraped_data'  # Folder to save downloaded PDFs
    document_class = "regulation"

    results = asyncio.run(scrape_website(scrpae_egz,page_url, destination_folder, document_class))
    # Function to add static data to the publication entry (if any)
    def add_static_data(publication):
        publication["issuing_authority"] = "Government of Sri Lanka"
        publication["data_origin"] = "scraped"
        return publication
    # Print the results
    if results:
        logger.info(f"Scraped {len(results)} records:")
        for result in results:
            logger.info(result)
    else:
        logger.info("No records found.")
