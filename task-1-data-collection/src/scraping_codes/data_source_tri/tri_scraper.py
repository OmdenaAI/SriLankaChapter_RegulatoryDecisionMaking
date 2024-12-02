import asyncio
import os
import requests
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

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
        "filename": pdf_title + ".pdf" if pdf_title else "Untitled.pdf",  # Default to "Untitled.pdf" if title is None
        "url": pdf_link if pdf_link else None,
        "retrieved_date_of_issuance": pdf_date if pdf_date else None,
        "retrieved_title": pdf_title if pdf_title else None,
        "reference_number": reference_number if reference_number else None,  # Set None if reference number is unavailable
    }

# Function to create a subfolder if it doesn't exist
def create_subfolder(destination_folder, document_class):
    subfolder = document_class.lower() if document_class else "unknown"  # Default to "unknown" if class is None
    subfolder_path = os.path.join(destination_folder, subfolder)
    os.makedirs(subfolder_path, exist_ok=True)
    return subfolder_path

# Function to add static data to the publication entry (if any)
def add_static_data(publication):
    # Example of adding static data (you can expand this as needed)
    publication["source"] = "Tea Research Institute" if "Tea Research Institute" else None
    publication["category"] = "Research" if "Research" else None
    publication["keywords"] = ["Tea", "Research", "Publications"] if ["Tea", "Research", "Publications"] else None
    return publication

# Function to download the PDF file and save it to the respective folder
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

# Function to scrape PDF links from the website
async def get_pdf_links(tri_url, destination_data_folder):
    publications = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            table_selectors = [
                "table#footable_581",  # Circulars Table
                "table#footable_616",  # Guidelines Table
            ]
            nxt_sel = 'li.footable-page-nav[data-page="next"] a.footable-page-link'

            current_page = tri_url
            logger.info(f"Scraping page: {current_page}")
            await page.goto(current_page)
            await page.wait_for_timeout(5000)

            for table_selector in table_selectors:
                document_class = "circular" if table_selector == "table#footable_581" else "guideline"

                last_table_html = ""
                while True:
                    current_table_html = await page.inner_html(table_selector)
                    if current_table_html == last_table_html:
                        break  # No more pages
                    last_table_html = current_table_html

                    # Parse the HTML
                    soup = BeautifulSoup(current_table_html, "html.parser")

                    for row in soup.find_all("tr"):
                        # Extract the circular number for circulars
                        circular_number_tag = row.find("td", class_="ninja_column_0 ninja_clmn_nm_circular_no")
                        guideline_number_tag = row.find("td", class_="ninja_column_0 ninja_clmn_nm_guideline_no")

                        # Extract the link (title), link (href), and date
                        link_tag = row.find("a", href=True)
                        date_tag = row.find("td", class_="ninja_clmn_nm_issued_in")

                        if link_tag and date_tag:
                            pdf_link = link_tag["href"] if link_tag else None
                            pdf_title = link_tag.text.strip() if link_tag else None
                            pdf_date = date_tag.text.strip() if date_tag else None

                            # Initialize reference_number as None by default
                            reference_number = None

                            # If it's a circular, extract the circular number
                            if document_class == "circular" and circular_number_tag:
                                reference_number = circular_number_tag.text.strip() if circular_number_tag else None

                            # If it's a guideline, extract the reference number
                            if document_class == "guideline" and guideline_number_tag:
                                reference_number = guideline_number_tag.text.strip() if guideline_number_tag else None

                            # Create a publication entry
                            publication = create_publication_entry(pdf_link, pdf_title, pdf_date, document_class, reference_number)
                            
                            # Add static data to the publication entry (if any)
                            publication = add_static_data(publication)

                            # Add the publication to the list
                            publications.append(publication)

                            # Prepare the destination folder based on the document class
                            destination_folder_sub = create_subfolder(destination_data_folder, document_class)
                            
                            # Download the PDF file to the appropriate subfolder if the link exists
                            if pdf_link:
                                await download_pdf(pdf_link, destination_folder_sub, pdf_title + ".pdf")

                    # Check for next page
                    next_button = await page.query_selector(nxt_sel)
                    if next_button:
                        await next_button.click()
                        await page.wait_for_timeout(2000)
                    else:
                        break

            await browser.close()
        return publications
    except Exception as e:
        logger.error(f"Error getting PDF links: {e}")
        raise

# Function to scrape the website and save the data
async def scrape_website(scraping_url, destination_data_folder):
    try:
        destination_folder = os.path.join(destination_data_folder)
        
        # Scrape the PDF links
        result = await get_pdf_links(scraping_url, destination_folder)
        logger.info(f"Number of results: {len(result)}")
        logger.info(f"Initial scraping done. Downloading into {destination_folder}")
        logger.info("Downloaded documents from TRI")
        
        # Return the scraped data
        return result
    except Exception as e:
        logger.error(f"Error scraping website: {e}")
        raise

# Entry point for the script (if running directly)
if __name__ == "__main__":
    scraping_url = 'https://www.tri.lk/view-all-publications/'  # URL to scrape
    destination_data_folder = './scraped_data'  # Folder to save the PDFs
    # Run the scraper and capture the results
    results = asyncio.run(scrape_website(scraping_url, destination_data_folder))
    
    # Print the results
    if results:
        logger.info(f"Scraped {len(results)} records:")
        for result in results:
            logger.info(result)
    else:
        logger.info("No records found.")
