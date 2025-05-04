import requests
import time
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def connect_mongodb():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI environment variable not set")
    return MongoClient(uri)

# Get API key from environment variable
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
if not RAPIDAPI_KEY:
    logger.error("RAPIDAPI_KEY environment variable not set")
    RAPIDAPI_KEY = ""  # Set to empty to avoid errors, but it won't work

headers = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "twitter154.p.rapidapi.com",
    "Content-Type": "application/json"
}

search_url = "https://twitter154.p.rapidapi.com/search/search"

query_categories = {
    "user_behavior_violations": [
        # 冒充和虚假身份
        "tiktok impersonation",
        "tiktok fake identity",
        "tiktok fake celebrity",
        "tiktok fake account",
        "tiktok catfishing",
        "tiktok identity theft",
        "tiktok fake influencer",
        "tiktok stolen identity",
        "tiktok fake profile",
        "tiktok impersonator account",
        
        # 霸凌和骚扰
        "tiktok bullying",
        "tiktok harassment",
        "tiktok cyberbullying",
        "tiktok online harassment",
        "tiktok hate comments",
        "tiktok toxic behavior",
        "tiktok abusive content",
        "tiktok threatening messages",
        "tiktok stalking",
        "tiktok doxxing",
        
        # 仇恨言论
        "tiktok hate speech",
        "tiktok racial slurs",
        "tiktok racist content",
        "tiktok homophobia",
        "tiktok transphobia",
        "tiktok antisemitism",
        "tiktok islamophobia",
        "tiktok xenophobia",
        "tiktok sexism",
        "tiktok discrimination",
        
        # 错误信息和阴谋论
        "tiktok misinformation",
        "tiktok conspiracy theories",
        "tiktok fake news",
        "tiktok covid misinformation",
        "tiktok vaccine misinformation",
        "tiktok election fraud",
        "tiktok flat earth",
        "tiktok qanon",
        "tiktok anti vax",
        "tiktok climate change denial",
        
        # 裸露和性内容
        "tiktok nudity",
        "tiktok sexually explicit",
        "tiktok inappropriate content",
        "tiktok adult content",
        "tiktok sexual harassment",
        "tiktok sexual exploitation",
        "tiktok revenge porn",
        "tiktok sexual solicitation",
        "tiktok grooming",
        "tiktok sexual abuse",
        
        # 暴力和血腥
        "tiktok violence",
        "tiktok gore",
        "tiktok graphic content",
        "tiktok animal cruelty",
        "tiktok self harm",
        "tiktok suicide content",
        "tiktok violent threats",
        "tiktok fight videos",
        "tiktok dangerous challenges",
        "tiktok extreme content",
        
        # 毒品和武器
        "tiktok drugs",
        "tiktok weapons",
        "tiktok drug promotion",
        "tiktok drug use",
        "tiktok illegal substances",
        "tiktok gun content",
        "tiktok weapon tutorials",
        "tiktok drug dealing",
        "tiktok substance abuse",
        "tiktok dangerous weapons",
        
        # 其他违规行为
        "tiktok illegal activities",
        "tiktok scam content",
        "tiktok phishing",
        "tiktok fraud",
        "tiktok copyright violation",
        "tiktok privacy violation",
        "tiktok underage content",
        "tiktok child exploitation",
        "tiktok dangerous pranks",
        "tiktok harmful challenges"
    ],


    "moderation_gaps": [
        # 内容审核失败
        "tiktok failed moderation",
        "tiktok moderation failed",
        "tiktok content review failed",
        "tiktok moderation system failed",
        "tiktok content check failed",
        "tiktok moderation process failed",
        "tiktok content screening failed",
        "tiktok moderation check failed",
        "tiktok content filter failed",
        "tiktok moderation filter failed",
        
        # 举报被忽视
        "tiktok ignored my report",
        "tiktok report ignored",
        "tiktok ignored report",
        "tiktok report not reviewed",
        "tiktok ignored complaint",
        "tiktok report dismissed",
        "tiktok ignored flag",
        "tiktok report overlooked",
        "tiktok ignored warning",
        "tiktok report disregarded",
        
        # 多次举报无效
        "tiktok reported many times",
        "tiktok multiple reports ignored",
        "tiktok repeated reports",
        "tiktok many reports failed",
        "tiktok numerous reports ignored",
        "tiktok multiple flags ignored",
        "tiktok repeated flags",
        "tiktok many complaints ignored",
        "tiktok numerous complaints",
        "tiktok multiple warnings ignored",
        
        # 不当内容未被删除
        "tiktok doesn't take this down",
        "tiktok content not removed",
        "tiktok video not deleted",
        "tiktok post not taken down",
        "tiktok content still up",
        "tiktok video still visible",
        "tiktok post still exists",
        "tiktok content remains",
        "tiktok video remains",
        "tiktok post remains",
        
        # 令人不安的内容
        "tiktok disturbing video",
        "tiktok disturbing content",
        "tiktok disturbing post",
        "tiktok disturbing material",
        "tiktok disturbing footage",
        "tiktok disturbing clip",
        "tiktok disturbing content",
        "tiktok disturbing media",
        "tiktok disturbing upload",
        "tiktok disturbing share",
        
        # 内容审核质疑
        "why is this still on tiktok",
        "why is this allowed on tiktok",
        "how is this on tiktok",
        "why is this up on tiktok",
        "how is this allowed on tiktok",
        "why is this visible on tiktok",
        "how is this still on tiktok",
        "why is this existing on tiktok",
        "how is this up on tiktok",
        "why is this present on tiktok",
        
        # 病毒传播质疑
        "how is this viral on tiktok",
        "why is this trending on tiktok",
        "how is this popular on tiktok",
        "why is this going viral on tiktok",
        "how is this spreading on tiktok",
        "why is this shared on tiktok",
        "how is this promoted on tiktok",
        "why is this recommended on tiktok",
        "how is this featured on tiktok",
        "why is this highlighted on tiktok",
        
        # 审核标准质疑
        "tiktok moderation standards",
        "tiktok content standards",
        "tiktok review standards",
        "tiktok moderation criteria",
        "tiktok content criteria",
        "tiktok review criteria",
        "tiktok moderation rules",
        "tiktok content rules",
        "tiktok review rules",
        "tiktok moderation guidelines",
        
        # 审核系统问题
        "tiktok moderation system",
        "tiktok content system",
        "tiktok review system",
        "tiktok moderation process",
        "tiktok content process",
        "tiktok review process",
        "tiktok moderation mechanism",
        "tiktok content mechanism",
        "tiktok review mechanism",
        "tiktok moderation procedure",
        
        # 审核响应问题
        "tiktok moderation response",
        "tiktok content response",
        "tiktok review response",
        "tiktok moderation action",
        "tiktok content action",
        "tiktok review action",
        "tiktok moderation handling",
        "tiktok content handling",
        "tiktok review handling",
        "tiktok moderation management",
        
        # 健康信息传播问题
        "tiktok covid misinformation",
        "tiktok false health info",
        "tiktok vaccine misinformation",
        "tiktok medical misinformation",
        "tiktok health conspiracy",
        "tiktok false medical advice",
        "tiktok dangerous health tips",
        "tiktok misleading health info",
        "tiktok unproven treatments",
        "tiktok health hoaxes",
        
        # 政治言论问题
        "tiktok political misinformation",
        "tiktok election misinformation",
        "tiktok government conspiracy",
        "tiktok political propaganda",
        "tiktok political manipulation",
        "tiktok election interference",
        "tiktok political censorship",
        "tiktok political suppression",
        "tiktok political bias",
        "tiktok political influence",
        
        # 社会议题争议
        "tiktok social justice censorship",
        "tiktok human rights suppression",
        "tiktok protest content",
        "tiktok activism censorship",
        "tiktok social movement suppression",
        "tiktok controversial topics",
        "tiktok sensitive issues",
        "tiktok social commentary",
        "tiktok political activism",
        "tiktok social change",
        
        # 宗教和信仰问题
        "tiktok religious content",
        "tiktok religious censorship",
        "tiktok faith based content",
        "tiktok religious suppression",
        "tiktok spiritual content",
        "tiktok religious discussion",
        "tiktok faith expression",
        "tiktok religious views",
        "tiktok spiritual beliefs",
        "tiktok religious freedom",
        
        # 历史和教育内容
        "tiktok educational censorship",
        "tiktok educational suppression",
        
        # 科学和技术争议
        "tiktok technology censorship",
        "tiktok science denial",
        "tiktok tech suppression",
        
        
        # 其他敏感话题
        "tiktok controversial opinions",
        "tiktok sensitive discussion",
        "tiktok controversial views",
        "tiktok sensitive topics",
        "tiktok controversial content",
        "tiktok sensitive material",
        "tiktok controversial issues",
        "tiktok sensitive subjects",
        "tiktok controversial matters",
        "tiktok sensitive matters"
    ],
    "platform_moderation_issues": [
        # 错误封禁
        "tiktok false ban",
        "tiktok banned for no reason",
        "tiktok wrongfully banned",
        "tiktok unjust ban",
        "tiktok account suspended unfairly",
        "tiktok shadowban",
        "tiktok shadow banned",
        "tiktok account restricted",
        "tiktok content removed unfairly",
        "tiktok video taken down wrong",
        
        # 言论自由和政治内容
        "tiktok freedom of expression",
        "tiktok political content ban",
        "tiktok censorship",
        "tiktok free speech",
        "tiktok political censorship",
        "tiktok content suppression",
        "tiktok opinion censorship",
        "tiktok political opinion ban",
        "tiktok viewpoint discrimination",
        "tiktok content moderation bias",
        
        # 算法错误
        "tiktok algorithm error",
        "tiktok moderation mistakes",
        "tiktok algorithm bias",
        "tiktok broken algorithm",
        "tiktok content filtering error",
        "tiktok recommendation error",
        "tiktok algorithm discrimination",
        "tiktok content distribution error",
        "tiktok algorithm unfair",
        "tiktok content visibility error",
        
        # 申诉问题
        "tiktok appeal denied",
        "tiktok appeal ignored",
        "tiktok support unresponsive",
        "tiktok help center ignored",
        "tiktok customer service failed",
        "tiktok appeal process broken",
        "tiktok support ticket ignored",
        "tiktok help request denied",
        "tiktok appeal system broken",
        "tiktok support team unhelpful",
        
        # 内容审核问题
        "tiktok moderation inconsistency",
        "tiktok content review error",
        "tiktok moderation unfair",
        "tiktok content policy unclear",
        "tiktok community guidelines unclear",
        "tiktok moderation double standards",
        "tiktok content policy inconsistent",
        "tiktok moderation policy confusing",
        "tiktok content rules unclear",
        "tiktok moderation standards unfair",
        
        # 账号管理问题
        "tiktok account recovery failed",
        "tiktok account verification issues",
        "tiktok account security problems",
        "tiktok account access denied",
        "tiktok account locked unfairly",
        "tiktok account disabled wrong",
        "tiktok account verification denied",
        "tiktok account security breach",
        "tiktok account access restricted",
        "tiktok account management issues",
        
        # 系统错误
        "tiktok system error",
        "tiktok technical issues",
        "tiktok platform bug",
        "tiktok system malfunction",
        "tiktok technical glitch",
        "tiktok platform error",
        "tiktok system failure",
        "tiktok technical problem",
        "tiktok platform issue",
        "tiktok system bug",
        
        # 用户反馈问题
        "tiktok feedback ignored",
        "tiktok report system broken",
        "tiktok user complaints ignored",
        "tiktok feedback system failed",
        "tiktok report function broken",
        "tiktok user suggestions ignored",
        "tiktok feedback mechanism broken",
        "tiktok report feature failed",
        "tiktok user input ignored",
        "tiktok feedback process broken",
        
        # 透明度问题
        "tiktok lack of transparency",
        "tiktok unclear policies",
        "tiktok hidden rules",
        "tiktok secret guidelines",
        "tiktok unclear moderation",
        "tiktok hidden algorithms",
        "tiktok secret content rules",
        "tiktok unclear decision making",
        "tiktok hidden moderation process",
        "tiktok secret content policies",
        
        # 平台责任问题
        "tiktok platform responsibility",
        "tiktok content oversight",
        "tiktok platform accountability",
        "tiktok content management",
        "tiktok platform governance",
        "tiktok content supervision",
        "tiktok platform oversight",
        "tiktok content regulation",
        "tiktok platform management",
        "tiktok content control"
    ],
    "monetization_and_fraud": [
        # 虚假广告和误导性推广
        "tiktok fake ads",
        "tiktok misleading promotions",
        "tiktok false advertising",
        "tiktok deceptive marketing",
        "tiktok scam ads",
        "tiktok fake promotions",
        "tiktok misleading ads",
        "tiktok false claims",
        "tiktok deceptive advertising",
        "tiktok scam marketing",
        
        # 虚假产品和诈骗卖家
        "tiktok fake products",
        "tiktok scam sellers",
        "tiktok counterfeit goods",
        "tiktok fake merchandise",
        "tiktok scam products",
        "tiktok fake items",
        "tiktok scam shops",
        "tiktok fake stores",
        "tiktok scam merchants",
        "tiktok fake sellers",
        
        # 点击诱饵和欺骗性链接
        "tiktok clickbait",
        "tiktok deceptive links",
        "tiktok scam links",
        "tiktok fake links",
        "tiktok misleading links",
        "tiktok phishing links",
        "tiktok malicious links",
        "tiktok dangerous links",
        "tiktok scam websites",
        "tiktok fake websites",
        
        # 账号买卖和引流
        "tiktok buy account",
        "tiktok sell account",
        "tiktok account trading",
        "tiktok account selling",
        "tiktok account buying",
        "tiktok account marketplace",
        "tiktok account transfer",
        "tiktok account exchange",
        "tiktok account purchase",
        "tiktok account sale",
        
        # 引流到其他平台
        "tiktok redirect to instagram",
        "tiktok follow on youtube",
        "tiktok subscribe on twitch",
        "tiktok follow on twitter",
        "tiktok join on discord",
        "tiktok visit my website",
        "tiktok check my onlyfans",
        "tiktok follow on snapchat",
        "tiktok subscribe on patreon",
        "tiktok join my telegram",
        
        # 虚假粉丝和互动
        "tiktok fake followers",
        "tiktok fake likes",
        "tiktok fake views",
        "tiktok fake comments",
        "tiktok bot followers",
        "tiktok purchased followers",
        "tiktok bought likes",
        "tiktok fake engagement",
        "tiktok purchased views",
        "tiktok fake interactions",
        
        # 投资和金融诈骗
        "tiktok investment scam",
        "tiktok crypto fraud",
        "tiktok financial scam",
        "tiktok money scam",
        "tiktok trading scam",
        "tiktok investment fraud",
        "tiktok crypto scam",
        "tiktok financial fraud",
        "tiktok money fraud",
        "tiktok trading fraud",
        
        # 抽奖和赠品诈骗
        "tiktok giveaway scam",
        "tiktok lottery scam",
        "tiktok prize scam",
        "tiktok contest scam",
        "tiktok sweepstakes scam",
        "tiktok free gift scam",
        "tiktok reward scam",
        "tiktok promotion scam",
        "tiktok competition scam",
        "tiktok prize fraud",
        
        # 订阅和会员诈骗
        "tiktok subscription scam",
        "tiktok membership fraud",
        "tiktok premium scam",
        "tiktok vip fraud",
        "tiktok paid content scam",
        "tiktok exclusive content fraud",
        "tiktok subscription fraud",
        "tiktok membership scam",
        "tiktok premium fraud",
        "tiktok vip scam",
        
        # 课程和教育诈骗
        "tiktok course scam",
        "tiktok education fraud",
        "tiktok tutorial scam",
        "tiktok learning fraud",
        "tiktok training scam",
        "tiktok class fraud",
        "tiktok workshop scam",
        "tiktok seminar fraud",
        "tiktok coaching scam",
        "tiktok mentoring fraud"
    ],
    "privacy_and_safety": [
        # 儿童安全和暴露
        "tiktok child exposure",
        "tiktok kids safety",
        "tiktok child privacy",
        "tiktok underage content",
        "tiktok child protection",
        "tiktok kids data",
        "tiktok child safety",
        "tiktok minor content",
        "tiktok child exploitation",
        "tiktok kids privacy",
        
        # 数据滥用
        "tiktok data misuse",
        "tiktok data privacy",
        "tiktok data collection",
        "tiktok data sharing",
        "tiktok data security",
        "tiktok data breach",
        "tiktok data leak",
        "tiktok data harvesting",
        "tiktok data tracking",
        "tiktok data surveillance",
        
        # 人脸识别和生物识别
        "tiktok facial recognition",
        "tiktok face tracking",
        "tiktok biometric data",
        "tiktok face detection",
        "tiktok facial data",
        "tiktok face scanning",
        "tiktok biometric tracking",
        "tiktok face analysis",
        "tiktok facial features",
        "tiktok face mapping",
        
        # 网络钓鱼和恶意链接
        "tiktok phishing",
        "tiktok malicious links",
        "tiktok scam links",
        "tiktok dangerous links",
        "tiktok harmful links",
        "tiktok suspicious links",
        "tiktok virus links",
        "tiktok malware links",
        "tiktok unsafe links",
        "tiktok infected links",
        
        # 账号安全
        "tiktok account security",
        "tiktok account hacked",
        "tiktok password stolen",
        "tiktok login issues",
        "tiktok account breach",
        "tiktok security breach",
        "tiktok account safety",
        "tiktok login security",
        "tiktok account protection",
        "tiktok security issues",
        
        # 位置隐私
        "tiktok location tracking",
        "tiktok gps data",
        "tiktok location privacy",
        "tiktok location sharing",
        "tiktok location exposure",
        "tiktok location security",
        "tiktok location data",
        "tiktok location tracking",
        "tiktok location privacy",
        "tiktok location safety",
        
        # 内容隐私
        "tiktok content privacy",
        "tiktok private content",
        "tiktok content security",
        "tiktok content protection",
        "tiktok content safety",
        "tiktok content exposure",
        "tiktok content sharing",
        "tiktok content access",
        "tiktok content control",
        "tiktok content privacy",
        
        # 用户信息保护
        "tiktok user privacy",
        "tiktok personal data",
        "tiktok user information",
        "tiktok user security",
        "tiktok user protection",
        "tiktok user safety",
        "tiktok user data",
        "tiktok user tracking",
        "tiktok user monitoring",
        "tiktok user surveillance",
        
        # 第三方数据共享
        "tiktok third party data",
        "tiktok data sharing",
        "tiktok data partners",
        "tiktok data access",
        "tiktok data transfer",
        "tiktok data exchange",
        "tiktok data collaboration",
        "tiktok data integration",
        "tiktok data connection",
        "tiktok data sharing",
        
        # 隐私设置和控制
        "tiktok privacy settings",
        "tiktok privacy control",
        "tiktok privacy options",
        "tiktok privacy features",
        "tiktok privacy tools",
        "tiktok privacy management",
        "tiktok privacy configuration",
        "tiktok privacy preferences",
        "tiktok privacy controls",
        "tiktok privacy settings"
    ],
    "technical_algorithmic_flaws": [
        # 算法偏见和不公平
        "tiktok biased algorithm",
        "tiktok feed unfair",
        "tiktok algorithm discrimination",
        "tiktok content bias",
        "tiktok recommendation bias",
        "tiktok algorithm prejudice",
        "tiktok feed manipulation",
        "tiktok content suppression",
        "tiktok algorithm unfairness",
        "tiktok feed discrimination",
        
        # 危险挑战和低质量病毒传播
        "tiktok dangerous challenges",
        "tiktok low quality virality",
        "tiktok harmful trends",
        "tiktok dangerous trends",
        "tiktok viral challenges",
        "tiktok risky challenges",
        "tiktok unsafe trends",
        "tiktok harmful challenges",
        "tiktok dangerous viral",
        "tiktok harmful viral",
        
        # 影子禁令和内容限制
        "tiktok shadowban",
        "tiktok reach limited",
        "tiktok content suppressed",
        "tiktok visibility reduced",
        "tiktok engagement dropped",
        "tiktok views decreased",
        "tiktok reach restricted",
        "tiktok content hidden",
        "tiktok algorithm penalty",
        "tiktok reach blocked",
        
        # 内容分发问题
        "tiktok content distribution",
        "tiktok feed algorithm",
        "tiktok content visibility",
        "tiktok reach algorithm",
        "tiktok content promotion",
        "tiktok feed distribution",
        "tiktok content exposure",
        "tiktok reach system",
        "tiktok content algorithm",
        "tiktok feed visibility",
        
        # 推荐系统问题
        "tiktok recommendation system",
        "tiktok suggested content",
        "tiktok for you page",
        "tiktok fyp algorithm",
        "tiktok discovery page",
        "tiktok explore page",
        "tiktok suggested videos",
        "tiktok recommended content",
        "tiktok fyp feed",
        "tiktok discovery feed",
        
        # 内容质量控制
        "tiktok content quality",
        "tiktok video quality",
        "tiktok content standards",
        "tiktok quality control",
        "tiktok content filtering",
        "tiktok video standards",
        "tiktok quality algorithm",
        "tiktok content rating",
        "tiktok video filtering",
        "tiktok quality system",
        
        # 用户互动问题
        "tiktok engagement algorithm",
        "tiktok interaction system",
        "tiktok comment system",
        "tiktok like system",
        "tiktok share system",
        "tiktok follow system",
        "tiktok interaction algorithm",
        "tiktok engagement system",
        "tiktok comment algorithm",
        "tiktok like algorithm",
        
        # 内容审核技术问题
        "tiktok content moderation",
        "tiktok automated review",
        "tiktok ai moderation",
        "tiktok content detection",
        "tiktok automated filtering",
        "tiktok content scanning",
        "tiktok ai review",
        "tiktok automated detection",
        "tiktok content analysis",
        "tiktok ai filtering",
        
        # 平台性能问题
        "tiktok platform performance",
        "tiktok app performance",
        "tiktok loading issues",
        "tiktok buffering problems",
        "tiktok video loading",
        "tiktok app crashes",
        "tiktok performance issues",
        "tiktok loading errors",
        "tiktok buffering errors",
        "tiktok video errors",
        
        # 技术故障和错误
        "tiktok technical issues",
        "tiktok system errors",
        "tiktok app bugs",
        "tiktok platform bugs",
        "tiktok technical glitches",
        "tiktok system glitches",
        "tiktok app errors",
        "tiktok platform errors",
        "tiktok technical problems",
        "tiktok system problems"
    ]
}

section = "top"
start_date = "2025-01-01"
language = "en"
min_retweets = "20"
min_likes = "20"
limit = 20
max_results = 100000

def insert_new_tweets(tweets, collection, category, keyword):
    new_tweets = []
    for tweet in tweets:
        tweet_id = tweet.get("tweet_id")
        if tweet_id and not collection.find_one({"tweet_id": tweet_id}):
            tweet["category"] = category
            tweet["keyword"] = keyword
            new_tweets.append(tweet)
    if new_tweets:
        collection.insert_many(new_tweets)
    return len(new_tweets)

def fetch_data():
    client = connect_mongodb()
    collection = client['tiktok']['twitter']
    
    try:
        for category, keywords in query_categories.items():
            # Limit to first 5 keywords for testing
            for keyword in keywords:
                logger.info(f"Searching: [{category}] '{keyword}'")
                continuation_token = None

                while True:
                    params = {
                        "query": keyword,
                        "section": section,
                        "start_date": start_date,
                        "language": language,
                        "min_retweets": min_retweets,
                        "min_likes": min_likes,
                        "limit": str(limit)
                    }
                    
                    if continuation_token:
                        params["continuationToken"] = continuation_token
                        
                    try:
                        response = requests.get(search_url, headers=headers, params=params)
                        
                        # Check for API errors
                        if response.status_code != 200:
                            logger.error(f"API error: {response.status_code} - {response.text}")
                            break
                            
                        data = response.json()
                        tweets = data.get("results", [])
                        continuation_token = data.get("continuation_token")

                        logger.info(f"Page: {len(tweets)} tweets... token: {continuation_token}")

                        inserted = insert_new_tweets(tweets, collection, category, keyword)
                        logger.info(f"Inserted {inserted} new tweets. Total in DB: {collection.count_documents({})}")

                        if inserted == 0:
                            logger.info("No new tweets found, moving to next keyword.")
                            break

                        if not continuation_token or collection.count_documents({}) >= max_results:
                            break

                        time.sleep(1)

                    except Exception as e:
                        logger.error(f"Error processing request: {str(e)}")
                        break

        logger.info("All queries done.")
    finally:
        client.close()

if __name__ == "__main__":
    fetch_data()
