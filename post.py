from datetime import datetime
from typing import Dict, Any, Optional
from enum import IntEnum
from dotenv import load_dotenv
import os

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
            "source_name": None,
            "source_url": self._build_video_url(unique_id, post_id),
            "reply_to": None,
            "level": None,
            "sentiment": 0,
            "isPriority": False,
            "crawl_bot": self.crawl_bot,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }
    
    def _build_video_url(self, unique_id: str, post_id: Optional[str]) -> str:
        """Tạo URL video TikTok"""
        if not post_id:
            return ""
        return f"{self.BASE_URL}/@{unique_id}/video/{post_id}"
    
    def _build_author_url(self, unique_id: str) -> str:
        return f"{self.BASE_URL}/@{unique_id}"
    
    def flatten_batch(self, data_list: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        return [self.flatten(data) for data in data_list]


# ==================== Ví dụ sử dụng ====================

# if __name__ == "__main__":
#     # Khởi tạo flattener
#     flattener = TikTokPostFlattener()
    
#     # Dữ liệu mẫu
#     sample_tiktok_data = {
#         "id": "7234567890123456789",
#         "createTime": 1699999999,
#         "subject_id": "tech_review",
#         "title": "Amazing Tech Review",
#         "description": "Check out this new gadget",
#         "desc": "This is the full description of the video content",
#         "author": {
#             "id": "123456789",
#             "uniqueId": "techreviewer123",
#             "nickname": "Tech Reviewer"
#         },
#         "stats": {
#             "commentCount": 150,
#             "shareCount": 50,
#             "diggCount": 1000,
#             "collectCount": 200,
#             "playCount": 50000
#         }
#     }
    
#     # Flatten single post
#     flattened_post = flattener.flatten(sample_tiktok_data)
    
#     print("Flattened Post:")
#     for key, value in flattened_post.items():
#         print(f"  {key}: {value}")
    
#     print("\n" + "="*50 + "\n")
    
#     # Flatten batch
#     search_data = [
#         {
#             "id": "111",
#             "createTime": 1699999998,
#             "desc": "First video",
#             "author": {"id": "1", "uniqueId": "user1", "nickname": "User One"},
#             "stats": {"commentCount": 10, "shareCount": 5, "diggCount": 100, "playCount": 1000}
#         },
#         {
#             "id": "222",
#             "createTime": 1699999997,
#             "desc": "Second video",
#             "author": {"id": "2", "uniqueId": "user2", "nickname": "User Two"},
#             "stats": {"commentCount": 20, "shareCount": 10, "diggCount": 200, "playCount": 2000}
#         }
#     ]
    
#     # Sử dụng như code gốc của bạn
#     if len(search_data) > 0:
#         for document in search_data:
#             data = flattener.flatten(document)
#             print(f"Post ID: {data['source_id']}")
#             print(f"  Author: {data['auth_name']}")
#             print(f"  URL: {data['url']}")
#             print(f"  Views: {data['views']}")
#             print()
    
#     # Hoặc flatten toàn bộ batch
#     batch_result = flattener.flatten_batch(search_data)
#     print(f"\nFlattened {len(batch_result)} posts in batch")