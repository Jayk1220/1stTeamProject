import time
import os
import sys
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from sqlalchemy import create_engine, text

# ==============================================================================
# [Configuration]
# ==============================================================================

# 1. ORACLE Database Connection URL
# Format: oracle+cx_oracle://username:password@host:port/?service_name=xe
ORACLE_URL = 'oracle+cx_oracle://news_db:1234@localhost:1521/?service_name=xe'

# 2. Target Press List ("Press Name": "OID")
TARGET_PRESS_DICT = {
    "매일경제": "009",      
    "한국경제": "015",      
    "머니투데이": "008",    
    "서울경제": "011",      
    "파이낸셜뉴스": "014",  
    "헤럴드경제": "016",    
    "아시아경제": "277",    
    "이데일리": "018",
    "조세일보": "123", 
    "조선비즈": "366", 
    "비즈워치": "648"
}

# 3. Gap Filling Mode (Optional)
# If set to a date string (e.g., "2025-12-26"), the crawler will:
#   - Continue crawling even if duplicates are found.
#   - Stop ONLY when the current crawling date is OLDER than this date.
# If set to None, it works in default "Incremental Mode" (Stops on first duplicate).
GP_UNTIL_DATE = "2025-12-26"  # Set to None to disable

# ==============================================================================
# [Crawler Class]
# ==============================================================================

class NewsCrawlerV4:
    def __init__(self, db_url, press_dict, until_date=None):
        self.db_url = db_url
        self.press_dict = press_dict
        self.until_date = until_date
        self.engine = None
        self.driver = None
        self.limit_dt = None
        
        if self.until_date:
            try:
                self.limit_dt = datetime.strptime(self.until_date, "%Y-%m-%d")
            except ValueError:
                print(f"[ERROR] Invalid date format for limit_date: {self.until_date}. Use YYYY-MM-DD.")
                sys.exit(1)

    def init_db(self):
        """Initialize SQLAlchemy Engine."""
        try:
            self.engine = create_engine(self.db_url)
            print("[System] DB Connected successfully.")
        except Exception as e:
            print(f"[System] DB Connection Failed: {e}")
            sys.exit(1)

    def init_driver(self):
        """Initialize Chrome WebDriver (Headless)."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Add window size to prevent element overlaps in headless mode
        chrome_options.add_argument("--window-size=1920,1080") 
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        print("[System] Browser Driver Initialized.")

    def close(self):
        """Cleanup resources."""
        if self.driver:
            self.driver.quit()
        if self.engine:
            self.engine.dispose()
        print("[System] Resources released.")

    # --------------------------------------------------------------------------
    # Utility Functions
    # --------------------------------------------------------------------------
    
    def clean_date(self, date_str):
        """Parse Naver News date string into YYYY-MM-DD HH:MM:SS format."""
        try:
            date_str = str(date_str).replace("기사입력", "").replace("입력", "").strip()
            is_pm = "오후" in date_str
            date_str = date_str.replace("오전", "").replace("오후", "").strip()
            
            # Format: 2025.12.15. 10:30
            dt = datetime.strptime(date_str, "%Y.%m.%d. %H:%M")
            
            if is_pm and dt.hour != 12: 
                dt = dt.replace(hour=dt.hour + 12)
            elif not is_pm and dt.hour == 12: 
                dt = dt.replace(hour=0)
                
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return date_str

    def is_link_in_db(self, link):
        """Check presence of link in DB."""
        if not self.engine: return False
        try:
            with self.engine.connect() as conn:
                query = text('SELECT 1 FROM NEWS WHERE LINK = :link')
                result = conn.execute(query, {'link': link}).fetchone()
                return result is not None
        except Exception as e:
            print(f"[DB Error] Check Link: {e}")
            return False

    def insert_article(self, data, oid):
        """Insert cleared data into DB."""
        if not self.engine: return
        try:
            dt_val = None
            try:
                dt_val = datetime.strptime(data['날짜'], "%Y-%m-%d %H:%M:%S")
            except:
                pass # Leave as None if parsing failed logic check

            with self.engine.begin() as conn:
                query = text('''
                    INSERT INTO NEWS (LINK, NDATE, TITLE, CONTENT, "OID")
                    VALUES (:link, :ndate, :title, :content, :oid)
                ''')
                conn.execute(query, {
                    'link': data['링크'], 
                    'ndate': dt_val,
                    'title': data['제목'], 
                    'content': data['본문'], 
                    'oid': oid
                })
        except Exception as e:
            # Duplicate entry or other DB errors
            # print(f"[DB Error] Insert: {e}") 
            pass

    # --------------------------------------------------------------------------
    # Scraping Logic
    # --------------------------------------------------------------------------

    def extract_article_info(self, url):
        """Navigate to URL and extract title, content, date."""
        try:
            self.driver.get(url)
            # Efficient wait (Wait less for explicit elements)
            time.sleep(0.3) 
            
            # Domain Filter
            curr_url = self.driver.current_url
            if any(x in curr_url for x in ["entertain.naver.com", "sports.news.naver.com", "sports.naver.com"]):
                return None

            # Check validity (Title presence)
            try:
                WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#title_area > span")))
            except TimeoutException:
                # Retry check for redirect
                curr_url = self.driver.current_url
                if any(x in curr_url for x in ["entertain.naver.com", "sports.naver.com"]):
                    return None
                return None

            # 1. Title
            try: title = self.driver.find_element(By.CSS_SELECTOR, "#title_area > span").text
            except: title = "제목 없음"

            # 2. Content
            try:
                dic_area = self.driver.find_element(By.CSS_SELECTOR, "#dic_area")
                self.driver.execute_script("""
                    var element = arguments[0];
                    var dirts = element.querySelectorAll(".img_desc, .media_end_summary"); 
                    for (var i = 0; i < dirts.length; i++) { dirts[i].remove(); }
                """, dic_area)
                content = dic_area.text.replace("\n", " ").strip()
            except: content = "본문 없음"

            # 3. Date
            raw_date = "날짜 없음"
            date_selectors = [
                ".media_end_head_info_datestamp .media_end_head_info_datestamp_time",
                ".media_end_head_info_datestamp span",
                ".t11"
            ]
            for sel in date_selectors:
                try:
                    raw_date = self.driver.find_element(By.CSS_SELECTOR, sel).text
                    if raw_date: break
                except: continue
            
            # If still found nothing, check attribute
            if raw_date == "날짜 없음":
                try:
                    elem = self.driver.find_element(By.CSS_SELECTOR, ".media_end_head_info_datestamp")
                    raw_date = elem.get_attribute("data-date-time")
                except: pass

            clean_dt = self.clean_date(raw_date)
            print(f"  ▷ [Scraping] {title[:20]}... ({clean_dt})")
            
            return {"날짜": clean_dt, "제목": title, "본문": content, "링크": url}

        except Exception as e:
            # print(f"[Scraping Error] {e}")
            return None

    def process_day_press(self, date_str, press_name, oid):
        """Process a single press on a specific date."""
        target_url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid={oid}&date={date_str}"
        self.driver.get(target_url)

        # Check if page exists
        try:
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#main_content > div.list_body"))
            )
        except TimeoutException:
            print(f"   [!] No content or timeout for {press_name} on {date_str}")
            return True, 0 # Considered 'done' but no articles

        page_count = 1
        inserted_count = 0
        stop_press = False
        
        # Session cache for this specific run context (Press + Date)
        session_crawled_links = set()

        while True:
            # A. Extract Links from current list page
            try:
                article_urls = []
                # Selectors for headlines and normal lists
                link_elements = self.driver.find_elements(By.CSS_SELECTOR, "#main_content > div.list_body ul li dl dt a")
                
                # Filter out photo links (usually dt.photo a) if struct differs, but Naver structure is standard:
                # type06_headline > li > dl > dt > a  (Sometimes dt is photo)
                # Let's stick to the robust selector list used previously
                selectors = [
                     "#main_content > div.list_body.newsflash_body > ul.type06_headline > li dl > dt:not(.photo) > a",
                     "#main_content > div.list_body.newsflash_body > ul.type06 > li dl > dt:not(.photo) > a"
                ]
                for sel in selectors:
                    elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    for el in elems:
                        href = el.get_attribute("href")
                        if href: article_urls.append(href)
            except Exception as e:
                print(f"   [!] Error collecting links: {e}")
                break

            if not article_urls:
                break # End of pages

            main_window = self.driver.current_window_handle
            
            # B. Process Links
            for url in article_urls:
                if "entertain.naver.com" in url or "sports.naver.com" in url:
                    continue
                
                # 1. Check Session Cache (Pagination Shift Protection)
                if url in session_crawled_links:
                    continue
                
                # 2. Check DB
                if self.is_link_in_db(url):
                    # Gap Filling Logic
                    if self.limit_dt: # Forced mode
                        # Just skip, don't stop
                        continue
                    else:
                        # Incremental Mode -> Stop here
                        # But double check: is it really an old article?
                        # Yes, because session cache miss + DB hit = Old data.
                        print(f"      [STOP] Reached existing data: {url}")
                        stop_press = True
                        break
                
                # 3. Open & Scrape
                try:
                    self.driver.execute_script("window.open('');")
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    
                    data = self.extract_article_info(url)
                    
                    self.driver.close()
                    self.driver.switch_to.window(main_window)
                    
                    if data:
                        self.insert_article(data, oid)
                        inserted_count += 1
                        session_crawled_links.add(url)
                
                except Exception as e:
                    print(f"      [!] Tab Error: {e}")
                    self.driver.switch_to.window(main_window)

            if stop_press:
                break
            
            # C. Next Page Logic
            try:
                paging_div = self.driver.find_element(By.CSS_SELECTOR, "#main_content > div.paging")
                current_num = int(paging_div.find_element(By.TAG_NAME, "strong").text)
                next_num = current_num + 1
                
                # Try finding number button
                try:
                    next_btn = paging_div.find_element(By.XPATH, f".//a[normalize-space()='{next_num}']")
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    page_count += 1
                    time.sleep(1.0)
                except NoSuchElementException:
                    # Try finding 'Next' arrow
                    try:
                        next_arrow = paging_div.find_element(By.CSS_SELECTOR, "a.next")
                        self.driver.execute_script("arguments[0].click();", next_arrow)
                        page_count += 1
                        time.sleep(1.0)
                    except NoSuchElementException:
                        break # End of pages (No more buttons)

            except (NoSuchElementException, TimeoutException):
                break # No paging div -> Single page
            except Exception as e:
                print(f"   [!] Pagination Error: {e}")
                break
        
        return stop_press, inserted_count

    def run(self):
        """Main Loop."""
        self.init_db()
        self.init_driver()
        
        current_date = datetime.now()
        active_targets = list(self.press_dict.items())
        
        print("=======================================================")
        print(f" News Crawler v4 (Oracle) Started at {current_date}")
        print(f" Mode: {'Gap Filling (UNTIL ' + self.until_date + ')' if self.until_date else 'Incremental (Stop on Duplicate)'}")
        print("=======================================================")

        try:
            while active_targets:
                date_str = current_date.strftime("%Y%m%d")
                
                # Date Limit Check
                if self.limit_dt:
                    # If we went past the limit date (older than limit), stop.
                    if current_date.date() < self.limit_dt.date():
                        print(f"\n[INFO] Reached Limit Date ({self.until_date}). Stopping.")
                        break
                
                print(f"\n>>> DATE: {date_str} | Active Press: {len(active_targets)}")
                
                finished_targets = []
                
                for press_name, oid in active_targets:
                    print(f"   -> [{press_name}] Scanning...")
                    is_stopped, count = self.process_day_press(date_str, press_name, oid)
                    
                    print(f"      Result: {count} inserted.")
                    if is_stopped:
                        print(f"      [COMPLETE] {press_name} is up-to-date.")
                        finished_targets.append((press_name, oid))
                
                # Remove finished press from active list
                for ft in finished_targets:
                    active_targets.remove(ft)
                
                # Go to yesterday
                current_date -= timedelta(days=1)
                
        except KeyboardInterrupt:
            print("\n[!] Stopped by User.")
        except Exception as e:
            print(f"\n[!] Critical Error: {e}")
        finally:
            self.close()

# ==============================================================================
# [Entry Point]
# ==============================================================================

if __name__ == "__main__":
    crawler = NewsCrawlerV4(ORACLE_URL, TARGET_PRESS_DICT, GP_UNTIL_DATE)
    crawler.run()
