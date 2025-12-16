
from typing import Dict, List
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse
import json
import os
import jmespath
from loguru import logger as log
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv()
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

SCRAPFLY = ScrapflyClient(key=SCRAPFLY_API_KEY)
def parse_search(response: ScrapeApiResponse) -> List[Dict]:
    """parse search data from XHR calls"""
    # extract the xhr calls and extract the ones for search results
    _xhr_calls = response.scrape_result["browser_data"]["xhr_call"]
    search_calls = [c for c in _xhr_calls if "/api/search/general/full/" in c["url"]]
    search_data = []
    for search_call in search_calls:
        try:
            data = json.loads(search_call["response"]["body"])["data"]
        except Exception as e:
            log.error(f"Failed to parse search data from XHR call: {e}")
            continue
        search_data.extend(data)
    
    # parse all the data using jmespath
    parsed_search = []
    for item in search_data:
        if item["type"] == 1:  # get the item if it was item only
            result = jmespath.search(
                """{
                id: id,
                desc: desc,
                createTime: createTime,
                video: video,
                author: author,
                stats: stats,
                authorStats: authorStats
                }""",
                item["item"],
            )
            result["type"] = item["type"]
            parsed_search.append(result)
    return parsed_search


async def scrape_search(keyword: str) -> List[Dict]:
    """scrape tiktok search data by scrolling the search page"""
    # js code for scrolling down with maximum 15 scrolls. It stops at the end without using the full iterations
    js = """const scrollToEnd = (i = 0) => (window.innerHeight + window.scrollY >= document.body.scrollHeight || i >= 15) ? (console.log("Reached the bottom or maximum iterations. Stopping further iterations."), setTimeout(() => console.log("Waited 10 seconds after all iterations."), 10000)) : (window.scrollTo(0, document.body.scrollHeight), setTimeout(() => scrollToEnd(i + 1), 10000)); setTimeout(() => scrollToEnd(), 5000);"""
    url = f"https://www.tiktok.com/search?q={quote(keyword)}"
    log.info(f"scraping search page with the URL {url} for search data")
    response = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            url,
            asp=True,
            country="VN",
            wait_for_selector="//div[@data-e2e='search_top-item']",
            render_js=True,
            auto_scroll=True,
            rendering_wait=10000,
            js=js,
            debug=True,
        )
    )
    data = parse_search(response)
    log.success(f"scraped {len(data)} search results for keyword: {keyword}")
    return data