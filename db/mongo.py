import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# 🔹 Load config từ .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

class MongoDB:
    def __init__(self):
        self._client: AsyncIOMotorClient | None = None
        self._db = None

    async def connect(self):
        if self._client is None:
            print(f"🔌 Kết nối MongoDB đến {MONGO_URI}/{MONGO_DB} ...")
            self._client = AsyncIOMotorClient(
                MONGO_URI,
                maxPoolSize=10,
                minPoolSize=1,
                connectTimeoutMS=20000,
                serverSelectionTimeoutMS=10000,
            )
            self._db = self._client[MONGO_DB]
            print("✅ Đã kết nối MongoDB thành công!")

    async def close(self):
        if self._client:
            self._client.close()
            print("❎ Đã đóng kết nối MongoDB.")
            self._client = None
            self._db = None

    @property
    def db(self):
        if self._db is None:
            raise Exception("⚠️ Database chưa được kết nối. Gọi connect() trước.")
        return self._db
