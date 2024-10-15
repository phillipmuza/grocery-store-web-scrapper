import asyncio
import logging
import aiohttp
from urllib.parse import urljoin, urlparse

class RobotsParser:
    def __init__(self, base_url):
        """
        Initializes the RobotsParser with the base URL.

        Args:
            base_url (str): The base URL of the website.
        """
        self.base_url = base_url
        self.robots_txt_url = urljoin(self.base_url, "/robots.txt")
        self.allowed_paths = []
        self.disallowed_paths = []
        self.crawl_delay = 0
        self.sitemaps = []  # List to store Sitemap URLs

    async def fetch_robots_txt(self, session):
        """
        Fetches the robots.txt content.

        Args:
            session (aiohttp.ClientSession): The HTTP session to use.

        Returns:
            str: The content of robots.txt.
        """
        try:
            async with session.get(self.robots_txt_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive'
            }) as response:
                if response.status == 200:
                    content = await response.text()
                    logging.info(f"Successfully fetched robots.txt from {self.base_url}")
                    return content
                else:
                    logging.warning(f"robots.txt not found at {self.robots_txt_url}. Status code: {response.status}")
                    return ""
        except Exception as e:
            logging.error(f"Error fetching robots.txt from {self.robots_txt_url}: {e}")
            return ""

    async def parse(self):
        """
        Parses the robots.txt to extract allowed and disallowed paths, crawl delay, and sitemaps.
        """
        async with aiohttp.ClientSession() as session:
            content = await self.fetch_robots_txt(session)
            if content:
                self.parse_robots(content)

    def parse_robots(self, content):
        """
        Parses the content of robots.txt.

        Args:
            content (str): The content of robots.txt.
        """
        user_agent = None
        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.lower().startswith("user-agent:"):
                user_agent = line.split(":", 1)[1].strip()
            elif line.lower().startswith("disallow:") and user_agent == "*":
                path = line.split(":", 1)[1].strip()
                if path:
                    self.disallowed_paths.append(path)
            elif line.lower().startswith("allow:") and user_agent == "*":
                path = line.split(":", 1)[1].strip()
                if path:
                    self.allowed_paths.append(path)
            elif line.lower().startswith("crawl-delay:") and user_agent == "*":
                delay = line.split(":", 1)[1].strip()
                try:
                    self.crawl_delay = float(delay)
                    logging.info(f"Crawl delay set to {self.crawl_delay} seconds for {self.base_url}")
                except ValueError:
                    logging.warning(f"Invalid crawl-delay value: {delay} in robots.txt for {self.base_url}")
            elif line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                self.sitemaps.append(sitemap_url)
                logging.info(f"Found sitemap in robots.txt: {sitemap_url}")

    def is_allowed(self, url):
        """
        Determines if a URL is allowed to be crawled based on robots.txt.

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
        for allowed in self.allowed_paths:
            if path.startswith(allowed):
                return True
        return True  # If not disallowed, assume allowed