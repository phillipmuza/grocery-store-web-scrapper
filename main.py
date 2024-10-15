"""
This script performs the following steps for each base URL:
1. Parse robots.txt to determine allowed URLs.
2. Fetch and parse the sitemap to extract and filter URLs based on robots.txt directives.
3. Crawl the filtered URLs asynchronously to extract product information.
4. Store the extracted data into a CSV file specific to each base URL.
"""

import asyncio
import os
from urllib.parse import urlparse, urljoin
from utils.robots_parser import RobotsParser
from utils.fetch_sitemap import SitemapFetcher
from crawl4ai.AsyncWebCrawler import crawl_extracted_links
import logging

def configure_logging():
    """
    Configures the logging settings for the crawler.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("crawler.log"),
            logging.StreamHandler()
        ]
    )

def get_base_urls():
    """
    Returns a list of base URLs to crawl.
    
    Returns:
        list: A list of base URLs as strings.
    """
    # Define your base URLs here
    return [
        "https://groceries.morrisons.com",
        # Add more base URLs as needed
    ]

def create_output_directory(directory="extracted_links"):
    """
    Creates the output directory if it doesn't exist.

    Args:
        directory (str, optional): The directory name. Defaults to "extracted_links".

    Returns:
        str: The path to the output directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created output directory: {directory}")
    else:
        logging.info(f"Output directory already exists: {directory}")
    return directory

def get_base_name(url):
    """
    Extracts a base name from the URL for naming the output CSV.
    
    Args:
        url (str): The base URL.
    
    Returns:
        str: A base name string.
    """
    return urlparse(url).netloc.replace('.', '_')

async def process_base_url(base_url, output_dir):
    """
    Processes a single base URL: parses robots.txt, fetches sitemap, crawls links, and saves data.
    
    Args:
        base_url (str): The base URL to process.
        output_dir (str): The directory to save the output CSV.
    """
    logging.info(f"\nStarting processing for base URL: {base_url}")

    # Step 1: Parse robots.txt
    robots_parser = RobotsParser(base_url)
    try:
        await robots_parser.parse()
        logging.info(f"Allowed paths extracted from robots.txt for {base_url}")
    except Exception as e:
        logging.error(f"Failed to parse robots.txt for {base_url}: {e}")
        return

    # Step 2: Fetch and parse sitemap
    # Use the sitemaps extracted from robots.txt if available
    if robots_parser.sitemaps:
        sitemap_urls = robots_parser.sitemaps
    else:
        # Fallback to default sitemap URLs if no Sitemaps listed in robots.txt
        sitemap_urls = [
            urljoin(base_url, "/sitemap.xml"),
            urljoin(base_url, "/sitemap_index.xml")
        ]

    logging.info(f"Sitemap URLs to be fetched for {base_url}: {sitemap_urls}")

    sitemap_fetcher = SitemapFetcher(
        base_url,
        disallowed_paths=robots_parser.disallowed_paths,
        sitemap_urls=sitemap_urls
    )
    try:
        links = await sitemap_fetcher.fetch_all_sitemaps()
        logging.info(f"Extracted {len(links)} links from sitemap for {base_url}")
    except Exception as e:
        logging.error(f"Failed to fetch or parse sitemap for {base_url}: {e}")
        return

    if not links:
        logging.warning(f"No links to crawl for {base_url}. Skipping.")
        return

    # Step 3: Crawl the links and extract product information
    output_csv = os.path.join(output_dir, f"{get_base_name(base_url)}_products.csv")
    await crawl_extracted_links(links, output_csv)
    logging.info(f"Completed processing for {base_url}. Data saved to {output_csv}")

async def main():
    """
    The main function orchestrating the crawling process.
    """
    configure_logging()
    base_urls = get_base_urls()
    output_dir = create_output_directory()
    
    tasks = []
    for base_url in base_urls:
        tasks.append(process_base_url(base_url, output_dir))
    
    await asyncio.gather(*tasks)
    logging.info("\nAll base URLs have been processed.")

if __name__ == "__main__":
    asyncio.run(main())