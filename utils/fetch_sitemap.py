import asyncio
import logging
import aiohttp
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

class SitemapFetcher:
    def __init__(self, base_url, disallowed_paths=None, sitemap_urls=None):
        """
        Initializes the SitemapFetcher with the base URL, disallowed paths, and sitemap URLs.

        Args:
            base_url (str): The base URL of the website.
            disallowed_paths (list, optional): Paths that are disallowed by robots.txt. Defaults to None.
            sitemap_urls (list, optional): List of sitemap URLs to fetch. Defaults to None.
        """
        self.base_url = base_url
        self.sitemap_urls = sitemap_urls if sitemap_urls else []
        self.disallowed_paths = disallowed_paths if disallowed_paths else []
        self.namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

    async def fetch_sitemap(self, session, sitemap_url):
        """
        Fetches the sitemap XML content.

        Args:
            session (aiohttp.ClientSession): The HTTP session to use.
            sitemap_url (str): The URL of the sitemap.

        Returns:
            str: The content of the sitemap or empty string on failure.
        """
        try:
            async with session.get(sitemap_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30) as response:
                if response.status == 200:
                    content = await response.text()
                    logging.info(f"Successfully fetched sitemap: {sitemap_url}")
                    return content
                else:
                    logging.warning(f"Failed to fetch sitemap: {sitemap_url}. Status code: {response.status}")
                    return ""
        except Exception as e:
            logging.error(f"Error fetching sitemap {sitemap_url}: {e}")
            return ""

    def extract_links_from_sitemap(self, content):
        """
        Extracts URLs from the sitemap XML content.

        Args:
            content (str): The sitemap XML content.

        Returns:
            list: A list of extracted URLs.
        """
        links = []
        try:
            root = ET.fromstring(content)
            if root.tag.endswith('sitemapindex'):
                # Contains child sitemap URLs
                for sitemap in root.findall('ns:sitemap', self.namespace):
                    loc = sitemap.find('ns:loc', self.namespace)
                    if loc is not None and loc.text:
                        if self.is_allowed(loc.text):
                            links.append(loc.text)
            elif root.tag.endswith('urlset'):
                # Contains page URLs
                for url in root.findall('ns:url', self.namespace):
                    loc = url.find('ns:loc', self.namespace)
                    if loc is not None and loc.text:
                        if self.is_allowed(loc.text):
                            links.append(loc.text)
            logging.info(f"Extracted {len(links)} links from sitemap.")
            return links
        except ET.ParseError as e:
            logging.error(f"Error parsing sitemap XML: {e}")
            return []

    async def retrieve_sitemaps(self):
        """
        Retrieves all sitemap URLs from the provided Sitemap URLs.

        Returns:
            list: A list of sitemap XML contents.
        """
        sitemap_contents = []

        async with aiohttp.ClientSession() as session:
            for sitemap_url in self.sitemap_urls:
                content = await self.fetch_sitemap(session, sitemap_url)
                if content:
                    sitemap_contents.append(content)
        return sitemap_contents

    def is_allowed(self, url):
        """
        Determines if a URL is allowed to be crawled based on disallowed paths.

        Args:
            url (str): The URL to check.

        Returns:
            bool: True if allowed, False otherwise.
        """
        parsed_url = urlparse(url)
        path = parsed_url.path
        for disallowed in self.disallowed_paths:
            if path.startswith(disallowed):
                return False
        return True

    async def fetch_all_sitemaps(self):
        """
        Fetches and parses all sitemaps, including child sitemaps recursively.

        Returns:
            list: A list of URLs extracted from all sitemaps.
        """
        sitemap_contents = await self.retrieve_sitemaps()
        all_links = []
        async with aiohttp.ClientSession() as session:
            for content in sitemap_contents:
                sitemap_urls = self.extract_links_from_sitemap(content)
                if sitemap_urls:
                    # Check if sitemap_urls are actual URLs or URLs to child sitemaps
                    # Assuming that if a sitemap URL contains 'sitemap', it's a child sitemap
                    child_sitemaps = [url for url in sitemap_urls if 'sitemap' in url.lower()]
                    links = [url for url in sitemap_urls if 'sitemap' not in url.lower()]
                    all_links.extend(links)
                    if child_sitemaps:
                        for child_sitemap in child_sitemaps:
                            child_content = await self.fetch_sitemap(session, child_sitemap)
                            if child_content:
                                child_links = self.extract_links_from_sitemap(child_content)
                                all_links.extend(child_links)
        return all_links