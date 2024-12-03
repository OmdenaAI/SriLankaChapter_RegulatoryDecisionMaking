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




# Function to scrape PDF links from the website
async def scrape_tri(tri_url, destination_data_folder, document_class):
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

            # Determine which table to use based on the document_class
            if document_class == "circular":
                table_selector = "table#footable_581"
            elif document_class == "guideline":
                table_selector = "table#footable_616"
            else:
                logger.error(f"Unsupported document class: {document_class}")
                return []

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



if __name__ == "__main__":
    page_url = 'https://www.tri.lk/view-all-publications/'  # URL to scrape
    destination_data_folder = './scraped_data'
    # You can change the document class here to either 'circular' or 'guideline'
    document_class = "circular"
    
    results = asyncio.run(scrape_website(scrpae_egz,scraping_url, destination_folder, document_class))
    # Function to add static data to the publication entry (if any)
    def add_static_data(publication):
        publication["issuing_authority"] = "Government of Sri Lanka"
        publication["data_origin"] = "scraped"
        return publication


    document_class = "guideline"
    
    results = asyncio.run(scrape_website(scrpae_egz,scraping_url, destination_folder, document_class))
    # Function to add static data to the publication entry (if any)
    def add_static_data(publication):
        publication["issuing_authority"] = "Government of Sri Lanka"
        publication["data_origin"] = "scraped"
        return publication

