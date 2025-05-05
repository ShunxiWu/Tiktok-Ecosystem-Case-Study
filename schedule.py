from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import fetchdata
import picking

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Remove file handler for cloud deployment
    ]
)
logger = logging.getLogger(__name__)

def hourly_task():
    try:
        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=12)
        # Use more precise time format
        # fetchdata.start_date = one_hour_ago.strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"Starting data fetch from {fetchdata.start_date}")
        fetchdata.fetch_data()
        logger.info("Data fetch completed")
        
        logger.info("Starting data classification")
        picking.classify_and_store()
        logger.info("Data classification completed")
        
    except Exception as e:
        logger.error(f"Error in hourly task: {str(e)}")

def main():
    scheduler = BlockingScheduler()
    
    # Run task every hour
    scheduler.add_job(hourly_task, 'interval', hours=1)
    
    # Run immediately on startup
    logger.info("Running initial task...")
    hourly_task()
    
    logger.info("Scheduler started")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")

if __name__ == "__main__":
    main()
