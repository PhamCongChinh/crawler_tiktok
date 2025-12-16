import os
import httpx
from dotenv import load_dotenv
load_dotenv()
from loguru import logger as log
URL_UNCLASSIFIED = os.getenv("URL_UNCLASSIFIED", "")

async def postToESUnclassified(content: any) -> any:
    total = len(content)
    data = {
        "index": "not_classify_org_posts",
        "data": content,
        "upsert": True
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(URL_UNCLASSIFIED, json=data)
            log.info(f"Đã đẩy {total}")
            log.info(response)
        
    except httpx.HTTPStatusError as e:
        log.error(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
    except Exception as e:
        log.exception(f"[ERROR] Insert exception: {e}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
