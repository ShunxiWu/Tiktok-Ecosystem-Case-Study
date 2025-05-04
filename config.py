import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

# 数据库配置
DB_NAME = "tiktok"
TWITTER_COLLECTION = "twitter"
UNHANDLED_COLLECTION = "unhandled_issues"
MISHANDLED_COLLECTION = "mishandled_issues"

# API配置
API_HOST = "twitter154.p.rapidapi.com"
SEARCH_URL = "https://twitter154.p.rapidapi.com/search/search"

# 搜索参数
SEARCH_PARAMS = {
    "section": "top",
    "start_date": "2025-01-01",
    "language": "en",
    "min_retweets": "20",
    "min_likes": "20",
    "limit": 20,
    "max_results": 100000
}
