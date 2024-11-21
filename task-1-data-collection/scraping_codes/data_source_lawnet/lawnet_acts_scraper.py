"""
Copied from https://dagshub.com/Omdena/SriLankaChapter_RegulatoryDecisionMaking/src/main/task-1-data-collection/Scraping%20all%20ACTs/actscraper
Code by Memoona / Roman

[TODO: Anamika] Add documentation and see how to run this on some other website
"""
import json
import logging
import requests
from trafilatura import extract
from typing import Collection, Any
from scrapy import Spider, Request, CrawlerProcess
from scrapy.http import JsonRequest, Response
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor


class Acts19562006Spider(Spider):
    name = 'acts_1956_2006_spider'
    allowed_domains = ['lawnet.gov.lk', 'localhost']
    start_urls = ['https://www.lawnet.gov.lk/acts-1956-2006-official/']

    def parse(self, response: Response):
        for link in LinkExtractor(restrict_xpaths='//*[@id="lcp_instance_0"]/li[*]/a').extract_links(response):
            yield Request(url=link.url, callback=self.parse_item)

        next_page = response.xpath('.//a[text()=">>"]/@href').get()
        if next_page:
            yield Request(url=response.urljoin(next_page), callback=self.parse)

    def parse_response(self, response):
        # Parse the JSON response
        title = response.meta.get('title')
        url = response.meta.get('url')
        print(response.text)
        self.logger.info(f"Scraped item from URL: {url}")
        yield {"text": response.json()["message"]["content"], "title": title, "url": url}

    def parse_item(self, response: Response):
        post_content = response.css('.post-content').get()

        if not post_content:
            self.logger.warning(f"No content found for URL: {response.url}")
        else:
            page_title = response.xpath('//title/text()').get().strip()
            text = extract(post_content, output_format="markdown", with_metadata=False)
            yield {"text": text, "title": page_title, "url": response.url}

def main():
    process = CrawlerProcess(settings={
        "FEEDS": {
            "output.json": {"format": "json"},
        },
    })

    process.crawl(Acts19562006Spider)
    process.start()  # Start the crawling process

if __name__ == "__main__":
    main()