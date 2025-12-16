from datetime import datetime
from typing import Dict, Any, Optional
from enum import IntEnum
from dotenv import load_dotenv
import os
import time

class DocType(IntEnum):
    """Loại document"""
    POST = 1
    COMMENT = 2


class CrawlSource(IntEnum):
    """Nguồn crawl"""
    TIKTOK = 2


class AuthType(IntEnum):
    """Loại tác giả"""
    USER = 1


class SourceType(IntEnum):
    """Loại nguồn"""
    TIKTOK = 5


class TikTokPostFlattener:
    
    CRAWL_SOURCE_CODE = os.getenv("TIKTOK_CRAWL_SOURCE_CODE", "tt")
    CRAWL_BOT = os.getenv("TIKTOK_CRAWL_BOT", "tiktok_1")
    BASE_URL = os.getenv("TIKTOK_BASE_URL", "https://www.tiktok.com")
    
    def __init__(
        self, crawl_source: int = CrawlSource.TIKTOK.value, 
        crawl_source_code: str = "tt",
        crawl_bot: str = "tiktok_1"):
        self.crawl_source = crawl_source
        self.crawl_source_code = crawl_source_code
        self.crawl_bot = crawl_bot
    
    def flatten(self, data: Dict[str, Any]) -> Dict[str, Any]:
        author = data.get("author", {})
        stats = data.get("stats", {})
        post_id = data.get("id")
        unique_id = author.get("uniqueId", "")

        now = int(time.time())
        days_ago = now - 2 * 86400
        pubtime = data.get("createTime", 0)
        if pubtime < days_ago:
            return None
        
        return {
            "doc_type": DocType.POST.value,
            "crawl_source": self.crawl_source,
            "crawl_source_code": self.crawl_source_code,
            "pub_time": data.get("createTime", 0),
            "crawl_time": int(datetime.now().timestamp()),
            "subject_id": data.get("subject_id"),
            "title": data.get("title"),
            "description": data.get("description"),
            "content": data.get("desc"),
            "url": self._build_video_url(unique_id, post_id),
            "media_urls": "[]",
            "comments": stats.get("commentCount", 0),
            "shares": stats.get("shareCount", 0),
            "reactions": stats.get("diggCount", 0),
            "favors": int(stats.get("collectCount", 0) or 0),
            "views": stats.get("playCount", 0),
            "web_tags": "[]",
            "web_keywords": "[]",
            "auth_id": author.get("id", ""),
            "auth_name": author.get("nickname", ""),
            "auth_type": AuthType.USER.value,
            "auth_url": self._build_author_url(unique_id),
            "source_id": post_id,
            "source_type": SourceType.TIKTOK.value,
            "source_name": author.get("nickname", ""),
            "source_url": self._build_video_url(unique_id, post_id),
            "reply_to": None,
            "level": None,
            "sentiment": 0,
            "isPriority": False,
            "crawl_bot": self.crawl_bot
        }
    
    def _build_video_url(self, unique_id: str, post_id: Optional[str]) -> str:
        """Tạo URL video TikTok"""
        if not post_id:
            return ""
        return f"{self.BASE_URL}/@{unique_id}/video/{post_id}"
    
    def _build_author_url(self, unique_id: str) -> str:
        return f"{self.BASE_URL}/@{unique_id}"
    
    def flatten_batch(self, data_list: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        # return [self.flatten(data) for data in data_list]
        return [
            item for item in (self.flatten(data) for data in data_list)
            if item is not None
        ]