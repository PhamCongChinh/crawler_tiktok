import asyncio
from datetime import datetime
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from api import postToESUnclassified
from db.mongo import MongoDB
from post import TikTokPostFlattener
import scraper

from dotenv import load_dotenv
load_dotenv()
ORG_ID = int(os.getenv("ORG_ID"))
STATUS = os.getenv("STATUS")
DELAY = int(os.getenv("DELAY"))

mongo = MongoDB()
flattener = TikTokPostFlattener()

async def main_job():
    try:
        print("🚀 Bắt đầu chạy job...")
        keywords = mongo.db["facebook_search_keywords"]
        count = await keywords.count_documents({"org_id": ORG_ID, "status": STATUS})
        print("Tổng số keyword tìm thấy:", count)
        async for keyword in keywords.find({"org_id": ORG_ID,"status": STATUS}):
            try:
                print(f"🔍 Đang xử lý keyword: {keyword.get('keyword')}")
                search_data = await scraper.scrape_search(
                    keyword=keyword.get("keyword",""),
                    max_search=12
                )
                data = flattener.flatten_batch(search_data)
                result = await postToESUnclassified(data)
                print(f"✅ Đã gửi thành công keyword: {keyword.get('keyword')}")
            except Exception as inner_e:
                print(f"❌ Lỗi khi xử lý keyword {keyword.get('keyword')}: {inner_e}")

            await asyncio.sleep(10)
        print("🏁 Job hoàn tất!")
    except Exception as e:
        print(f"❌ Lỗi trong main_job: {e}")

async def main():
    await mongo.connect()  # ✅ Chỉ connect 1 lần

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        main_job,
        "interval",
        minutes=DELAY,
        next_run_time=datetime.now()
    )
    scheduler.start()

    print("✅ Scheduler started. Waiting for jobs...")
    await asyncio.Event().wait()  # giữ chương trình chạy mãi

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(mongo.close())