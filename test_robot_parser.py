import asyncio
from utils.robots_parser import RobotsParser

async def test():
    base_url = "https://groceries.morrisons.com"  # Replace with your target URL
    parser = RobotsParser(base_url)
    await parser.parse()
    print("Sitemaps found:", parser.sitemaps)

asyncio.run(test())