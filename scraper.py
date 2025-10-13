
import re
from typing import Dict, List
from zoneinfo import ZoneInfo
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse
import json
import uuid
import os
import datetime
import secrets
import jmespath
from loguru import logger as log
from urllib.parse import urlencode, quote, urlparse, parse_qs

from dotenv import load_dotenv

load_dotenv()
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY")

SCRAPFLY = ScrapflyClient(key=SCRAPFLY_API_KEY)
BASE_CONFIG = {
    "country": "JP",
}

def parse_search(response: ScrapeApiResponse) -> List[Dict]:
    try:
        data = json.loads(response.scrape_result["content"])
        search_data = data["data"]
    except Exception as e:
        log.error(f"Failed to parse JSON from search API response: {e}")
        return None

    parsed_search = []
    for item in search_data:
        if item["type"] == 1:
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

    has_more = data["has_more"]
    return parsed_search

async def obtain_session(url: str, keyword: str) -> str:
    session_id = str(uuid.uuid4().hex)
    response = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            url=url,
            proxy_pool="public_datacenter_pool",
            **BASE_CONFIG,
            render_js=True,
            rendering_stage="domcontentloaded",
            cost_budget=10,
            session=session_id
        )
    )
    if response.cost > 6:
        log.warning(f"❌ Chi phí: {response.cost} - Từ khóa: {keyword}")
    else:
        log.info(f"✅ Chi phí: {response.cost} - Từ khóa: {keyword}")
    return session_id


async def scrape_search(keyword: str, max_search: int, search_count: int = 12) -> List[Dict]:
    def generate_search_id():
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_hex_length = (32 - len(timestamp)) // 2  # calculate bytes needed
        random_hex = secrets.token_hex(random_hex_length).upper()
        random_id = timestamp + random_hex
        return random_id

    def form_api_url(cursor: int):
        base_url = "https://www.tiktok.com/api/search/general/full/?"
        params = {
            "keyword": keyword,
            "offset": cursor,  # the index to start from
            "search_id": generate_search_id(),
            "region": "VN",
            "priority_region": "VN",
            "tz_name": "Asia/Saigon",
            "app_language":"vi",
            "browser_language":"vi-VN",
            "webcast_language":"vi"
        }
        encoded = urlencode(params)
        encoded_fixed = encoded.replace('+', '%20')
        return base_url + encoded_fixed

    log.info("Đang thiết lập session gọi API tìm kiếm TikTok...")
    try:
        time_vn = datetime.datetime.now()
        unix_ts = int(time_vn.timestamp() * 1000)
        query = keyword.replace(" ", "%20")
        session_id = await obtain_session(url=f"https://www.tiktok.com/search?q={query}&t={unix_ts}", keyword=keyword) #quote(keyword)

        log.info("Đang thu thập dữ liệu từ batch đầu tiên của kết quả tìm kiếm.")
        first_page = await SCRAPFLY.async_scrape(
            ScrapeConfig(
                form_api_url(cursor=0),
                asp=True,
                proxy_pool="public_datacenter_pool",
                **BASE_CONFIG,
                rendering_stage="domcontentloaded",
                headers={
                    "content-type": "application/json",
                },
                session=session_id,
            )
        )
        if first_page.cost > 1:
            log.warning(f"Chi phí: {first_page.cost} - Từ khóa: {keyword}")
        else:
            log.info(f"Chi phí: {first_page.cost} - Từ khóa: {keyword}")
        search_data = parse_search(first_page)

        log.info(f"Đang thu thập dữ liệu các trang tìm kiếm, còn lại {max_search // search_count} trang nữa.")
        total_cost = 0
        _other_pages = [
            ScrapeConfig(
                form_api_url(cursor=cursor),
                asp=True,
                proxy_pool="public_datacenter_pool",  # hoặc residential_pool nếu muốn IP người dùng
                **BASE_CONFIG,
                rendering_stage="domcontentloaded",
                headers={"content-type": "application/json"},
                session=session_id
            )
            for cursor in range(search_count, max_search + search_count, search_count)
        ]
        async for response in SCRAPFLY.concurrent_scrape(_other_pages):
            cost = response.cost or 0
            total_cost += cost
            data = parse_search(response)
            if data is not None:
                search_data.extend(data)

        log.success(f"Đã thu thập được {len(search_data)} kết quả từ API tìm kiếm với từ khóa {keyword}")
        filtered = [v for v in search_data if has_vietnamese_chars(v["desc"])]
        return filtered
    except:
        return []

def has_vietnamese_chars(text: str) -> bool:
    vietnamese_regex = r"[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệ" \
                       r"ìíỉĩịòóỏõọôồốổỗộơờớởỡợ" \
                       r"ùúủũụưừứửữựỳýỷỹỵđ]"
    return bool(re.search(vietnamese_regex, text.lower()))