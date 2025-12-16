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
BOT_ID = "213"
BOT_TYPE = "tiktok"
SERVER_IP = get_server_ip()
timestamp = int(time.time())

mongo = MongoDB()
flattener = TikTokPostFlattener()

async def main_job():
    try:
        print("🚀 Bắt đầu chạy job...")
        keywords = mongo.db["facebook_search_keywords"]

        for org in ORGS_ID:
            count = await keywords.count_documents({"org_id": org, "status": STATUS})
            print(f"[{org}]Tổng số keyword tìm thấy:", count)

            keywords_list = await keywords.find(
                {"org_id": org, "status": STATUS}
            ).to_list(length=None)
            # Duyệt danh sách keyword
            for keyword in keywords_list:
                try:
                    kw = keyword.get("keyword", "")
                    print(f"[{org}]🔍 Đang xử lý keyword: {kw}")

                    # Gọi scraper
                    search_data = await scraper.scrape_search(keyword=kw)
                    data = flattener.flatten_batch(search_data)
                    with open(output.joinpath("search.json"), "w", encoding="utf-8") as file:
                        json.dump(data, file, indent=2, ensure_ascii=False)
                    print(f"[{org}]Tổng dữ liệu {len(data)}")
                    if (len(data) > 0):
                        # Gửi dữ liệu lên Elasticsearch
                        await postToESUnclassified(data)
                        print(f"[{org}]✅ Đã gửi thành công keyword: {kw}")
                    else:
                        print(f"[{org}]✅ Không có dữ liệu")

                except Exception as inner_e:
                    print(f"❌ Lỗi khi xử lý keyword {keyword.get('keyword')}: {inner_e}")

                # Nghỉ giữa các lần xử lý để tránh bị rate-limit
                await asyncio.sleep(5)
                break
            print("[{org}]🏁 Job hoàn tất!")
            break
        # Đếm số keyword
        # count = await keywords.count_documents({"org_id": ORG_ID, "status": STATUS})
        # print("Tổng số keyword tìm thấy:", count)

        # # Lấy toàn bộ dữ liệu trước để tránh lỗi CursorNotFound
        # keywords_list = await keywords.find(
        #     {"org_id": ORG_ID, "status": STATUS}
        # ).to_list(length=None)

        # # Duyệt danh sách keyword
        # for keyword in keywords_list:
        #     try:
        #         kw = keyword.get("keyword", "")
        #         print(f"🔍 Đang xử lý keyword: {kw}")

        #         # Gọi scraper
        #         search_data = await scraper.scrape_search(keyword=kw, max_search=48)
        #         data = flattener.flatten_batch(search_data)

        #         # Gửi dữ liệu lên Elasticsearch
        #         await postToESUnclassified(data)
        #         print(f"✅ Đã gửi thành công keyword: {kw}")

        #     except Exception as inner_e:
        #         print(f"❌ Lỗi khi xử lý keyword {keyword.get('keyword')}: {inner_e}")

        #     # Nghỉ giữa các lần xử lý để tránh bị rate-limit
        #     await asyncio.sleep(10)

        # print("🏁 Job hoàn tất!")

    except Exception as e:
        print(f"❌ Lỗi trong main_job: {e}")

async def main():
    await mongo.connect()
    print("✅ Đã kết nối MongoDB")

    await main_job()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        main_job,
        "interval",
        minutes=DELAY
        # next_run_time=datetime.now()  # chạy ngay lần đầu
        # next_run_time=datetime.now() + timedelta(seconds=2)
    )
    scheduler.start()
    print("✅ Scheduler started. Waiting for jobs...")

    try:
        await asyncio.Event().wait()  # giữ chương trình chạy mãi
    finally:
        print("🧹 Đang đóng kết nối MongoDB...")
        await mongo.close()





payload = {
    "botId": BOT_ID,
    "botType": BOT_TYPE,
    "serverIp": SERVER_IP,
    "lastPingAt": timestamp,
    "status": "RUNNIG"
}

async def send_heartbeat():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(
                MONITOR_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            print("Heartbeat sent:", response.json())

    except httpx.HTTPError as e:
        print("Error sending heartbeat:", e)

async def run_app():

    await asyncio.gather(
        main(),        # bot crawl
        send_heartbeat()     # heartbeat 10s
    )

if __name__ == "__main__":
    try:
        # asyncio.run(main())
        asyncio.run(run_app())
    except KeyboardInterrupt:
        print("\n🛑 Dừng chương trình theo yêu cầu người dùng.")