import asyncio
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
from api import postToESUnclassified
from db.mongo import MongoDB
from post import TikTokPostFlattener
import scraper
import ast
import socket
from datetime import datetime, timezone
from loguru import logger as log

from dotenv import load_dotenv
load_dotenv()
ORG_ID = os.getenv("ORG_ID")
ORGS_ID = ast.literal_eval(ORG_ID)
STATUS = os.getenv("STATUS")
DELAY = int(os.getenv("DELAY"))

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

def get_server_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


MONITOR_URL = "http://222.254.14.6:8100/api/heartbeat/heartbeat"
BOT_ID = "bot_tiktok_live"
BOT_TYPE = "tiktok"
SERVER_IP = get_server_ip()
timestamp = int(time.time())

mongo = MongoDB()
flattener = TikTokPostFlattener()

async def main_job():
    try:
        log.info("🚀 Bắt đầu chạy job...")
        keywords = mongo.db["facebook_search_keywords"]

        for org in ORGS_ID:
            count = await keywords.count_documents({"org_id": org, "status": STATUS})
            log.info(f"[{org}] Tổng số keyword tìm thấy: {count}")

            keywords_list = await keywords.find(
                {"org_id": org, "status": STATUS}
            ).to_list(length=None)

            for keyword in keywords_list:
                try:
                    kw = keyword.get("keyword", "")
                    log.info(f"[{org}]🔍 Đang xử lý keyword: {kw}")

                    # Gọi scraper
                    search_data = await scraper.scrape_search(keyword=kw)
                    data = flattener.flatten_batch(search_data)
                    
                    log.info(f"[{org}] Tổng dữ liệu {len(data)}")
                    if (len(data) > 0):
                        await postToESUnclassified(data)
                        log.info(f"[{org}]✅ Đã gửi thành công keyword: {kw}")
                    else:
                        log.info(f"[{org}]✅ Không có dữ liệu")

                except Exception as inner_e:
                    log.error(f"❌ Lỗi khi xử lý keyword {keyword.get('keyword')}: {inner_e}")

                await asyncio.sleep(5)

            log.info("[{org}]🏁 Job hoàn tất!")

    except Exception as e:
        log.error(f"❌ Lỗi trong main_job: {e}")

async def main():
    await mongo.connect()
    log.info("✅ Đã kết nối MongoDB")

    await main_job()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        main_job,
        "interval",
        minutes=DELAY
    )
    scheduler.start()
    log.info("✅ Scheduler started. Waiting for jobs...")

    try:
        await asyncio.Event().wait()  # giữ chương trình chạy mãi
    finally:
        log.info("🧹 Đang đóng kết nối MongoDB...")
        await mongo.close()

async def send_heartbeat():
    while True:
        payload = {
            "botId": BOT_ID,
            "botType": BOT_TYPE,
            "serverIp": SERVER_IP,
            "lastPingAt": timestamp,
            "status": "RUNNING"
        }
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.post(
                    MONITOR_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                res = response.json()
                log.info(f"Bot Tiktok: {res.get('receivedStatus')}")

        except httpx.HTTPError as e:
            log.error("Error sending heartbeat:", e)

        await asyncio.sleep(5)

async def run_app():

    await asyncio.gather(
        main(),
        send_heartbeat()
    )

if __name__ == "__main__":
    try:
        # asyncio.run(main())
        asyncio.run(run_app())
    except KeyboardInterrupt:
        log.info("\n🛑 Dừng chương trình theo yêu cầu người dùng.")