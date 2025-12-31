import time
import os
import sqlite3
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import Nnews_Crawler_v3 as crawler_v3

# Database Name (kept for reference or other needs, though v3 handles its own DB connection)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "projectDB.db")

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Ensure tables exist (Schema matching setup_project_db.py)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news (
            link TEXT PRIMARY KEY,
            date TEXT,
            title TEXT,
            content TEXT,
            industry TEXT,
            sent_score REAL,
            oid TEXT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def crawl_job():
    print(f"\n[{datetime.now()}] Scheduler Job Started.")
    try:
        # Delegate the actual crawling to the self-contained v3 script
        crawler_v3.crawl_incremental()
    except Exception as e:
        print(f"Job Error: {e}")
    finally:
        print(f"[{datetime.now()}] Job Finished.\n")

if __name__ == "__main__":
    init_db()
    
    scheduler = BlockingScheduler()
    # Schedule to run every 10 minutes
    scheduler.add_job(crawl_job, 'interval', minutes=10, id='naver_news_crawler')
    
    print("=== Naver News Scheduler (via Nnews_Crawler_v3) ===")
    print("Scheduler started. Press Ctrl+C to exit.")
    print("First job will run in 10 minutes (or configured interval).")
    
    # Run once immediately on startup
    crawl_job()
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")
