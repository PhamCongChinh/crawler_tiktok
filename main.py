import asyncio
from datetime import datetime, timedelta
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from api import postToESUnclassified
from db.mongo import MongoDB
from post import TikTokPostFlattener
import scraper
import ast

from dotenv import load_dotenv
load_dotenv()
ORG_ID = os.getenv("ORG_ID")
ORGS_ID = ast.literal_eval(ORG_ID)
STATUS = os.getenv("STATUS")
DELAY = int(os.getenv("DELAY"))


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
                    search_data = await scraper.scrape_search(keyword=kw, max_search=12)
                    data = flattener.flatten_batch(search_data)
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

            print("[{org}]🏁 Job hoàn tất!")

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

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Dừng chương trình theo yêu cầu người dùng.")