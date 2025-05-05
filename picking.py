import os
import openai
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set OpenAI API key from the .env file
openai.api_key = os.getenv("OPENAI_API_KEY")

# MongoDB URI from the .env file
MONGO_URI = os.getenv("MONGO_URI")

def connect_mongodb():
    return MongoClient(MONGO_URI)

def classify_issue(text):
    prompt = (
        "As a TikTok Governance PM, analyze this user comment and classify it:\n\n"
        "1 = Ecosystem Issue: User reports problems like impersonation, scams, or harmful content that TikTok hasn't addressed. Examples: fake accounts, stolen content, impersonation, scams, harmful challenges, etc.\n\n"
        "2 = Mishandled Issue: TikTok's action made things worse. Examples: wrong account bans, unfair content removal, or when reporting made the problem worse.\n\n"
        "3 = Non-Issue: User is just sharing content, promoting something, or making general comments without reporting any problems.\n\n"
        f"Comment:\n{text}\n\n"
        "You must respond with either 1, 2, or 3. No other responses are allowed,Do NOT include any explanation, punctuation, or other text."
    )

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        answer = response['choices'][0]['message']['content'].strip()
        if answer == "1":
            return 1
        elif answer == "2":
            return 2
        elif answer == "3":
            return 3
        else:
            raise ValueError(f"Invalid response: {answer}")
    except Exception as e:
        print(f"\n❌ Error processing comment: {e}")
        print(f"Comment text: {text[:100]}...")
        raise  # 重新抛出异常，让外层处理

def classify_and_store():
    # Connect to MongoDB and access collections
    client = connect_mongodb()
    source_collection = client["tiktok"]["twitter"]  # 改为与fetchdata.py相同的集合
    unhandled_collection = client["tiktok"]["unhandled_issues"]  # 未处理问题
    mishandled_collection = client["tiktok"]["mishandled_issues"]  # 处理不当问题
    non_issue_collection = client["tiktok"]["non_issues"]  # 新增：非问题内容

    # 获取已处理的推文ID
    processed_ids = set()
    processed_ids.update(unhandled_collection.distinct("tweet_id"))
    processed_ids.update(mishandled_collection.distinct("tweet_id"))
    processed_ids.update(non_issue_collection.distinct("tweet_id"))
    
    print(f"Found {len(processed_ids)} already processed tweets")

    tweets = source_collection.find()
    unhandled_count = 0
    mishandled_count = 0
    non_issue_count = 0
    error_count = 0
    duplicate_count = 0

    for tweet in tweets:
        tweet_id = tweet.get("tweet_id")
        if not tweet_id:
            continue
            
        if tweet_id in processed_ids:
            duplicate_count += 1
            continue
            
        text = tweet.get("text", "")
        if not text:
            continue
        
        try:
            issue_type = classify_issue(text)
            
            print("\n" + "="*80)
            print(f"Tweet ID: {tweet_id}")
            print(f"Category: {issue_type}")
            print(f"Full Text: {text}")
            print("="*80 + "\n")
            
            if issue_type == 1:  # 未处理问题
                unhandled_collection.insert_one(tweet)
                unhandled_count += 1
                processed_ids.add(tweet_id)
                print(f"✅ Stored as Unhandled Issue")
            elif issue_type == 2:  # 处理不当问题
                mishandled_collection.insert_one(tweet)
                mishandled_count += 1
                processed_ids.add(tweet_id)
                print(f"⚠️ Stored as Mishandled Issue")
            else:  # 非问题内容
                non_issue_collection.insert_one(tweet)
                non_issue_count += 1
                processed_ids.add(tweet_id)
                print(f"📢 Stored as Non-Issue")
        except Exception as e:
            error_count += 1
            print(f"\n❌ Failed to process tweet {tweet_id}")
            print(f"Error: {str(e)}")
            continue

    # Summary of the process
    print("\n" + "="*80)
    print("Classification Summary:")
    print(f"🎯 Total unhandled issues: {unhandled_count}")
    print(f"⚠️ Total mishandled issues: {mishandled_count}")
    print(f"📢 Total non-issues: {non_issue_count}")
    print(f"❌ Total errors: {error_count}")
    print(f"🔄 Skipped duplicates: {duplicate_count}")
    print("="*80)

if __name__ == "__main__":
    classify_and_store()
