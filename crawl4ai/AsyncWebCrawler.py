import asyncio
import csv
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse
import json
import re

class CrawlerResult:
    def __init__(self, url, content, status):
        self.url = url
        self.content = content
        self.status = status

class AsyncWebCrawler:
    def __init__(self, verbose=False, max_concurrent=10):
        self.verbose = verbose
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

    async def __aenter__(self):
        timeout = ClientTimeout(total=30)
        self.session = ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def arun(self, url, retries=3):
        """
        Crawls a single URL with retries.

        Args:
            url (str): The URL to crawl.
            retries (int, optional): Number of retries for failed requests. Defaults to 3.

        Returns:
            CrawlerResult or Exception: The result of the crawl or an exception if all retries fail.
        """
        for attempt in range(1, retries + 1):
            try:
                async with self.semaphore:
                    async with self.session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as response:
                        if response.status == 200:
                            content = await response.text()
                            if self.verbose:
                                logging.info(f"Crawled URL: {url} with status {response.status}")
                            return CrawlerResult(url=url, content=content, status=response.status)
                        else:
                            if self.verbose:
                                logging.warning(f"Failed to crawl URL: {url} with status {response.status}")
                            raise Exception(f"HTTP {response.status} for URL: {url}")
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed for URL: {url} - {e}")
                if attempt < retries:
                    await asyncio.sleep(2)  # Wait before retrying
                else:
                    logging.error(f"All {retries} attempts failed for URL: {url}")
                    return Exception(f"Failed to crawl URL: {url} after {retries} attempts.")

# Define a set of image file extensions to ignore
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp'}

def is_image_url(url):
    return any(url.lower().endswith(ext) for ext in IMAGE_EXTENSIONS)

def crawl_url(url):
    if is_image_url(url):
        logging.info(f"Skipping image URL: {url}")
        return  # Skip crawling this URL

    try:
        response = fetch(url)
        structured_data = json.loads(response.text)
        # Process the structured data...
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding failed for {url}: {e}")

def extract_product_info(result):
    """
    Extracts product information from the crawler result.

    Args:
        result (CrawlerResult): The result returned by AsyncWebCrawler.

    Returns:
        dict: A dictionary containing product information.
    """
    try:
        soup = BeautifulSoup(result.content, 'html.parser')
        # Find the <script> tag with type 'application/ld+json'
        script_tag = soup.find('script', type='application/ld+json')
        if not script_tag:
            logging.warning(f"No structured data found for {result.url}")
            return {
                'title': 'N/A',
                'price': 'N/A',
                'description': 'N/A',
                'url': result.url
            }
        
        # Load the JSON content
        try:
            structured_data = json.loads(script_tag.string)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding failed for {result.url}: {e}")
            return {
                'title': 'N/A',
                'price': 'N/A',
                'description': 'N/A',
                'url': result.url
            }
        
        # Ensure the data is of type 'Product'
        if isinstance(structured_data, list):
            # Some pages might have a list of JSON-LD objects
            product_data = next((item for item in structured_data if item.get('@type') == 'Product'), None)
        else:
            product_data = structured_data if structured_data.get('@type') == 'Product' else None

        if not product_data:
            logging.warning(f"No Product type structured data found for {result.url}")
            return {
                'title': 'N/A',
                'price': 'N/A',
                'description': 'N/A',
                'url': result.url
            }
        
        # Extract the required fields
        title = product_data.get('name', 'N/A').strip() or 'N/A'
        description = product_data.get('description', 'N/A').strip() or 'N/A'
        
        # Extract price from offers
        offers = product_data.get('offers', {})
        price = offers.get('price', 'N/A').strip() or 'N/A'
        price_currency = offers.get('priceCurrency', 'N/A').strip() or 'N/A'
        if price != 'N/A' and price_currency != 'N/A':
            price = f"{price} {price_currency}"
        
        product = {
            'title': title,
            'price': price,
            'description': description,
            'url': result.url,
        }
        return product

    except Exception as e:
        logging.error(f"Error extracting product info from {result.url}: {e}")
        return None

async def crawl_extracted_links(links, output_file):
    """
    Crawls through a list of extracted links to extract product information.

    Args:
        links (list): A list of URLs to crawl.
        output_file (str): Path to the output CSV file where product info will be saved.
    """
    products = []
    async with AsyncWebCrawler(verbose=True, max_concurrent=10) as crawler:
        tasks = []
        for link in links:
            tasks.append(crawler.arun(url=link))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error crawling a link: {result}")
                continue
            # Extract product information from the result
            product_info = extract_product_info(result)
            if product_info:
                products.append(product_info)
    
    save_product_info_csv(products, output_file)

def save_product_info_csv(products, output_file):
    """
    Saves the extracted product information to a CSV file.

    Args:
        products (list): A list of dictionaries containing product information.
        output_file (str): Path to the output CSV file.
    """
    if not products:
        logging.warning(f"No product information to save for {output_file}.")
        return

    # Determine CSV headers from the keys of the first product dictionary
    headers = products[0].keys()

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(products)
        logging.info(f"Product information saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving product information: {e}")