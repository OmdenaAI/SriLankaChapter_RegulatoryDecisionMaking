import os
import logging
from trafilatoria import extract
from scrapy import Spider, Request, CrawlerProcess
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Acts19562006Spider(Spider):
    """
    Scrapy Spider to scrape Acts from the official website (lawnet.gov.lk) for the years 1956-2006.

    This spider extracts the Act titles and their respective content, converting them to markdown format.

    Attributes:
        name (str): The name of the Spider.
        allowed_domains (list): List of allowed domains.
        start_urls (list): List of starting URLs for the spider to crawl.
    """
    name = "acts_1956_2006_spider"
    allowed_domains = ["lawnet.gov.lk", "localhost"]
    start_urls = ["https://www.lawnet.gov.lk/acts-1956-2006-official/"]

    def parse(self, response: Response):
        """
        Parse the main page and extract links for each Act. Crawl through pagination if necessary.

        Args:
            response (Response): The response object containing the page content.

        Yields:
            Request: Scrapes each Act and handles pagination.
        """
        try:
            # Extract links for each ACT and pass them to the item parsing function
            for link in LinkExtractor(
                restrict_xpaths='//*[@id="lcp_instance_0"]/li[*]/a'
            ).extract_links(response):
                yield Request(url=link.url, callback=self.parse_item)

            # Handle pagination (if applicable)
            next_page = response.xpath('.//a[text()=">>"]/@href').get()
            if next_page:
                yield Request(url=response.urljoin(next_page), callback=self.parse)

        except Exception as e:
            logging.error(f"Error during parsing: {e}")
            yield None

    def parse_item(self, response: Response):
        """
        Parse individual Act pages to extract the content and convert it to markdown.

        Args:
            response (Response): The response object containing the content of an individual Act.

        Yields:
            dict: A dictionary containing the scraped data (title, content, and URL).
        """
        try:
            # Extract the content from the post
            post_content = response.css(".post-content").get()

            if not post_content:
                self.logger.warning(f"No content found for URL: {response.url}")
                yield None
            else:
                # Extract the page title and convert the content to markdown
                page_title = response.xpath("//title/text()").get().strip()
                text = extract(post_content, output_format="markdown", with_metadata=False)

                # Return the scraped data
                yield {"text": text, "title": page_title, "url": response.url}

        except Exception as e:
            logging.error(f"Error during item parsing for {response.url}: {e}")
            yield None

    def save_to_file(self, scraped_data, document_class="ACT"):
        """
        Save the scraped data into a file within the specified document class directory.

        Args:
            scraped_data (dict): The scraped data to save.
            document_class (str): The directory name where the file will be saved.
        """
        try:
            # Define the directory path for the specified document class
            document_class_directory = document_class

            # Create the directory if it doesn't exist
            if not os.path.exists(document_class_directory):
                os.makedirs(document_class_directory)

            # Define the output file path
            output_file_path = os.path.join(document_class_directory, "output.json")

            # Write the data to a JSON file
            with open(output_file_path, "a") as f:
                f.write(scraped_data)
            logging.info(f"Saved data to {output_file_path}")

        except Exception as e:
            logging.error(f"Error while saving scraped data to file: {e}")
            return None


def main():
    """
    Main function to start the Scrapy crawling process.

    Starts the Scrapy `CrawlerProcess` and runs the `Acts19562006Spider` spider.
    """
    try:
        # Set the document class dynamically
        document_class = "ACT"  # Can be dynamically changed, such as 'Regulations'

        process = CrawlerProcess(
            settings={
                "FEEDS": {
                    f"{document_class}/output.json": {"format": "json"},
                },
            }
        )

        # Start the crawling process
        logging.info("Starting the Scrapy crawl process...")
        process.crawl(Acts19562006Spider)
        process.start()  # Start the crawling process

    except Exception as e:
        logging.error(f"Error in the main execution: {e}")


if __name__ == "__main__":
    main()
