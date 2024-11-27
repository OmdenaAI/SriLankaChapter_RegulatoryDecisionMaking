import sys
import os
import asyncio

# import the manually created tri_scraper module by adding it's location to the path.
module_path = os.path.join(os.path.dirname(__file__), "scraping_codes/data_source_tri/")
sys.path.append(module_path)
import tri_scraper  # noqa: E402


def main():
    # tri_scraper.test_module('https://www.tri.lk/view-all-publications/',
    # 'data/task1_raw_input/data_source_tri/v0_0/files')
    asyncio.run(
        tri_scraper.scrape_website(
            "https://www.tri.lk/view-all-publications/",
            "data\\task1_raw_input\\data_source_tri\\v0_0\\files\\"
        )
    )


# Entry point for the script
if __name__ == "__main__":
    main()
