"""
Original code by Memoona in https://colab.research.google.com/drive/1Jvf4Z2-mxKMvemg6GaxRuD53WPVnOFJD#scrollTo=CY4ndzlUsulN

pip install playwright
playwright install  # This installs the necessary browser binaries
"""

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import requests
import os

TRI_URL = 'https://www.tri.lk/view-all-publications/'
TESTING_MODE = True

async def scrape_website(tri_url):
    """
    Scrapes the specified website asynchronously and returns the scraped data.

    Args:
        url (str): The URL of the website to scrape.

    Returns:
        dict: A dictionary containing the scraped data.
    """
    async with async_playwright() as p:
        # Launch a headless browser
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Navigate to the URL
        await page.goto(tri_url)

        # Wait for the page to load completely
        await page.wait_for_timeout(5000)  # Adjust time as needed for loading

        # Get the page content
        html_content = await page.content()

        # Use BeautifulSoup to parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract all PDF links, names, and publication dates
        publications = []
        for row in soup.find_all('tr'):
            link_tag = row.find('a', href=True)
            date_tag = row.find('td', class_='ninja_clmn_nm_issued_in')

            if link_tag and date_tag:
                pdf_link = link_tag['href']  # PDF link
                pdf_name = link_tag.text.strip()  # PDF name
                pdf_date = date_tag.text.strip()  # PDF date

                publications.append({
                    'PDF Name': pdf_name,
                    'PDF Link': pdf_link,
                    'Publication Date': pdf_date
                })

        # Close the browser
        await browser.close()

        # Once we have the URLs for the documents, download the files

        # print(len(publications))
        return publications


def download_pdf_and_get_info(publications):
    # Function to download PDF and retrieve metadata

    results = []
    # Counter dictionary to keep track of filenames
    counter = {}
    script_path = os.path.dirname(__file__)
    relative_path_to_root = os.path.join(script_path, "../../../")
    # ANA: FIX
    destination_folder = relative_path_to_root + "data/task1_raw_input/data_source_tri/v0_0/files"
    if TESTING_MODE:
        destination_folder = relative_path_to_root + "data/task1_raw_input/data_source_tri/v0_0/"

    for row in publications:
        request_url = row['PDF Link']
        if not request_url.startswith('https://www.tri.lk'):
            request_url = 'https://www.tri.lk' + row['PDF Link']

        response = requests.get(request_url)

        if response.status_code == 200:
            # Extracting the filename from the URL
            pdf_name = request_url.split("/")[-1]

            # Increment counter for duplicate filenames
            if pdf_name in counter:
                counter[pdf_name] += 1
                pdf_name = f"{pdf_name[:-4]} - {counter[pdf_name]}.pdf"  # Append the counter before .pdf
            else:
                counter[pdf_name] = 1

            # Save the PDF
            pdf_path = os.path.join(destination_folder, pdf_name)
            with open(pdf_path, 'wb') as f:
                f.write(response.content)

            # Use Beautiful Soup to scrape the page for additional info
            soup = BeautifulSoup(response.content, 'html.parser')

            # Here you would define how to find publication date and other info
            # Assuming publication date is in a specific tag (modify as needed)
            publication_date = soup.find('meta', {'name': 'date'})['content'] if soup.find('meta', {'name': 'date'}) else 'N/A'

            if pdf_name:
                results.append({
                'pdf_name': pdf_name,
                'publication_date': publication_date
                })
        else:
            print(f"Failed to retrieve {request_url}")
        # ANA: REMOVE
        if TESTING_MODE:
            break
    return results


async def main():
    # Scrape the website and wait for it to finish
    result = await scrape_website(TRI_URL)
    # print(f"Scraped Data: {result}")

    print("Scraping done. Proceeding to the next steps...")
    downloaded_results = download_pdf_and_get_info(result)
    # print(downloaded_results)
    print("Downloaded documents from TRI")

# Entry point for the script
if __name__ == "__main__":
    asyncio.run(main())
    # Instead of asyncio.run, use the following within a Jupyter/IPython environment:
    # await run()  # This uses the existing event loop of the environment
