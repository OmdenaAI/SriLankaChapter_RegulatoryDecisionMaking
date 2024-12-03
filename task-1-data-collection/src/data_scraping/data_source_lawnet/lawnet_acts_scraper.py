# import os
# import logging
# from trafilatoria import extract
# from scrapy import Spider, Request, CrawlerProcess
# from scrapy.http import Response
# from scrapy.linkextractors import LinkExtractor


# # Set up logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# class Acts19562006Spider(Spider):
#     """
#     Scrapy Spider to scrape Acts from the official website (lawnet.gov.lk) for the years 1956-2006.
#     This spider extracts the Act titles and their respective content, saving them as text files if they contain 'Tea' or 'tea'.
#     Attributes:
#         name (str): The name of the Spider.
#         allowed_domains (list): List of allowed domains.
#         start_urls (list): List of starting URLs for the spider to crawl.
#     """
#     name = "acts_1956_2006_spider"
#     allowed_domains = ["lawnet.gov.lk", "localhost"]
#     start_urls = ["https://www.lawnet.gov.lk/acts-1956-2006-official/"]

#     def parse(self, response: Response):
#         """
#         Parse the main page and extract links for each Act. Crawl through pagination if necessary.
#         Args:
#             response (Response): The response object containing the page content.
#         Yields:
#             Request: Scrapes each Act and handles pagination.
#         """
#         try:
#             # Extract links for each ACT and pass them to the item parsing function
#             for link in LinkExtractor(
#                 restrict_xpaths='//*[@id="lcp_instance_0"]/li[*]/a'
#             ).extract_links(response):
#                 yield Request(url=link.url, callback=self.parse_item)

#             # Handle pagination (if applicable)
#             next_page = response.xpath('.//a[text()=">>"]/@href').get()
#             if next_page:
#                 yield Request(url=response.urljoin(next_page), callback=self.parse)

#         except Exception as e:
#             logging.error(f"Error during parsing: {e}")
#             yield None

#     def parse_item(self, response: Response):
#         """
#         Parse individual Act pages to extract the content and save as a text file if they contain 'Tea' or 'tea'.
#         Args:
#             response (Response): The response object containing the content of an individual Act.
#         Yields:
#             dict: A dictionary containing the scraped data (title, content, and URL).
#         """
#         try:
#             # Extract the content from the post
#             post_content = response.css(".post-content").get()

#             if not post_content:
#                 self.logger.warning(f"No content found for URL: {response.url}")
#                 yield None
#             else:
#                 # Extract the page title
#                 page_title = response.xpath("//title/text()").get().strip()

#                 # Check if the title contains 'Tea' or 'tea' in the text content
#                 if "Tea" in page_title or "tea" in post_content.lower():
#                     # Save content directly to text file with title as filename
#                     file_path = self.save_text_content_to_file(post_content, page_title)

#                     # Return the scraped data
#                     yield {"text": post_content, "title": page_title, "url": response.url, "file_path": file_path}

#         except Exception as e:
#             logging.error(f"Error during item parsing for {response.url}: {e}")
#             yield None

#     def save_text_content_to_file(self, text_content, title):
#         """
#         Save the text content to a text file with the title as the filename.
#         Args:
#             text_content (str): The content to be saved.
#             title (str): The title to be used as the filename.
#         Returns:
#             str: The path where the file was saved.
#         """
#         try:
#             # Define the folder path for ACT (use your specified folder path)
#             act_folder = "/content/drive/MyDrive/Omdena_Challenge/new_LK_tea_dataset/ACT"

#             # Ensure the folder exists
#             if not os.path.exists(act_folder):
#                 os.makedirs(act_folder)

#             # Clean the title to make it a valid filename
#             safe_title = title.replace("/", "_").replace("\\", "_").replace(" ", "_")

#             # Define the output file path using the title as the filename
#             output_file_path = os.path.join(act_folder, f"{safe_title}.txt")

#             # Save the text content to the text file
#             with open(output_file_path, "w") as output_file:
#                 output_file.write(text_content)

#             logging.info(f"Saved text content to: {output_file_path}")
#             return output_file_path

#         except Exception as e:
#             logging.error(f"Error saving text content to file: {e}")
#             return None


# def main():
#     """
#     Main function to start the Scrapy crawling process.
#     Starts the Scrapy `CrawlerProcess` and runs the `Acts19562006Spider` spider.
#     """
#     try:
#         # Set the document class dynamically
#         document_class = "ACT"  
#         # No need for FEEDS if you're saving directly to files
#         process = CrawlerProcess()

#         # Start the crawling process
#         logging.info("Starting the Scrapy crawl process...")
#         process.crawl(Acts19562006Spider)
#         process.start()  # Start the crawling process

#     except Exception as e:
#         logging.error(f"Error in the main execution: {e}")


# if __name__ == "__main__":
#     main()
