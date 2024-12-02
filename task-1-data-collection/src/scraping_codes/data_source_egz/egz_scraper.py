import asyncio
import os
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Function to create a publication entry
def create_publication_entry(pdf_link, pdf_title, pdf_date, document_class, reference_number):
    return {
        "class": document_class if document_class else None,
        "filename": f"{pdf_title}.pdf" if pdf_title else "Untitled.pdf",  # Default to "Untitled.pdf" if title is None
        "url": pdf_link if pdf_link else None,
        "retrieved_date_of_issuance": pdf_date if pdf_date else None,
        "retrieved_title": pdf_title if pdf_title else None,
        "reference_number": reference_number if reference_number else None,  # Set None if reference number is unavailable
    }

# Function to add static data to the publication entry (if any)
def add_static_data(publication):
    publication["source"] = "Government of Sri Lanka" if "Government of Sri Lanka" else None
    publication["category"] = "Regulation" if "Regulation" else None
    publication["keywords"] = ["Sri Lanka", "Government", "Gazette"] if ["Sri Lanka", "Government", "Gazette"] else None
    return publication

# Function to create a subfolder based on document class
def create_subfolder(destination_folder, document_class):
    subfolder = document_class.lower() if document_class else "unknown"  # Default to "unknown" if class is None
    subfolder_path = os.path.join(destination_folder, subfolder)
    os.makedirs(subfolder_path, exist_ok=True)
    return subfolder_path

# Function to download PDF from URL and save it to the destination folder
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

# Function to extract and save the relevant details from each table row
async def scrape_exgazette_page(page_url, base_url, destination_folder):
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
                pdf_link_tag = row.select_one('a.btn.btn-xs.btn-primary[href$="E.pdf"]')
                document_class = "regulation"

                if reference_number and date_of_issuance and title and pdf_link_tag:
                    pdf_link = base_url + pdf_link_tag['href']  # Construct full URL for the PDF

                    # Create a publication entry
                    publication = create_publication_entry(pdf_link, title, date_of_issuance, document_class, reference_number)
                    
                    # Add static data to the publication entry (if any)
                    publication = add_static_data(publication)

                    # Add the publication to the list
                    publications.append(publication)

                    # Prepare the subfolder based on the document class
                    destination_folder_sub = create_subfolder(destination_folder, document_class)
                    
                    # Download the PDF file
                    await download_pdf(pdf_link, destination_folder_sub, f"{reference_number}.pdf")

            await browser.close()

        return publications
    except Exception as e:
        logger.error(f"Error scraping the page: {e}")
        raise

# Main function to run the scraper
async def scrape_website(base_url, page_url, destination_folder):
    try:
        logger.info(f"Starting scraping for {page_url}")

        # Scrape the Ex Gazette page
        results = await scrape_exgazette_page(page_url, base_url, destination_folder)
        
        # Log the result
        logger.info(f"Scraping completed. Found {len(results)} documents.")
        logger.info("Documents have been downloaded.")
        return results

    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise

# Entry point of the script
if __name__ == "__main__":
    base_url = 'http://documents.gov.lk/en'  # Base URL for the website
    page_url = 'http://documents.gov.lk/en/exgazette.php'  # URL of the Ex Gazette page
    destination_folder = './scraped_data'  # Folder to save downloaded PDFs
    results = asyncio.run(scrape_website(base_url, page_url, destination_folder))

    # Print the results
    if results:
        logger.info(f"Scraped {len(results)} records:")
        for result in results:
            logger.info(result)
    else:
        logger.info("No records found.")
