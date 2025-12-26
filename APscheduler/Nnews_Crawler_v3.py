import time
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuration
# 1-1. 수집할 언론사 목록 ("언론사명": "oid코드")
TARGET_PRESS_DICT = {
    "매일경제": "009",      #★★★
    "한국경제": "015",      #★★★
    "머니투데이": "008",    #★
    "서울경제": "011",      #★
    "파이낸셜뉴스": "014",  #★
    "헤럴드경제": "016",    #★
    "아시아경제": "277",    #★
    # "이데일리": "018",
    # "조세일보": "123", 
    # "조선비즈": "366", 
    # "비즈워치": "648"
}
# Named helper functions copied from v2_3.py to make this script self-contained

def clean_date(date_str):
    try:
        # 불필요한 텍스트 제거
        date_str = str(date_str).replace("기사입력", "").replace("입력", "").strip()
        # 오전/오후 처리를 위한 플래그 설정 (오후인 경우를 구분)
        is_pm = "오후" in date_str
        date_str = date_str.replace("오전", "").replace("오후", "").strip()
        # 날짜 포맷 파싱 (예: 2025.12.15. 10:30)
        dt = datetime.strptime(date_str, "%Y.%m.%d. %H:%M")
        # 12시간제 -> 24시간제 변환
        if is_pm and dt.hour != 12: 
            dt = dt.replace(hour=dt.hour + 12)
        elif not is_pm and dt.hour == 12: 
            dt = dt.replace(hour=0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    except:
        # 변환 실패 시 원본 그대로 반환 (에러 방지)
        return date_str

def extract_article_info(driver, url):
    try:
        driver.get(url)
        time.sleep(0.3) # 페이지 로딩 대기
        # [필터링] 현재 페이지 URL 확인하여 연예/스포츠 도메인이면 수집 제외
        current_url = driver.current_url
        if "entertain.naver.com" in current_url:
            return None
        if "sports.naver.com" in current_url:
            return None
        # [대기] 제목 요소가 뜰 때까지 최대 3초 대기 (일반 뉴스 페이지인지 확인)
        try:
            WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#title_area > span")))
        except:
            # 타임아웃 발생 시, 혹시 리다이렉트된 연예/스포츠인지 다시 확인
            if "entertain.naver.com" in driver.current_url or "sports.news.naver.com" in driver.current_url:
                return None
            return None # 그 외 로딩 실패 건너뜀

        # 2-3-1. 제목 추출
        try: title = driver.find_element(By.CSS_SELECTOR, "#title_area > span").text
        except: title = "제목 없음"

        # 2-3-2. 본문 추출 (불필요한 이미지 설명, 안내 문구 등 제거)
        try:
            dic_area = driver.find_element(By.CSS_SELECTOR, "#dic_area")
            # JavaScript로 불필요한 요소 강제 삭제
            driver.execute_script("""
                var element = arguments[0];
                var dirts = element.querySelectorAll(".img_desc, .media_end_summary"); 
                for (var i = 0; i < dirts.length; i++) { dirts[i].remove(); }
            """, dic_area)
            content = dic_area.text.replace("\n", " ").strip()
        except: content = "본문 없음"

        # 2-3-3. 날짜 추출
        try:
            # 전략 1: 표준 '기사입력' 클래스명으로 찾기 (가장 정확함)
            # 경로(>)를 쓰지 않고 공백(하위 요소 검색)을 사용하여 유연하게 찾습니다.
            date_element = driver.find_element(By.CSS_SELECTOR, ".media_end_head_info_datestamp .media_end_head_info_datestamp_time")
            raw_date = date_element.text
        except:
            try:
                # 전략 2: 만약 위의 클래스명이 없다면, 좀 더 넓은 범위의 span 찾기
                date_element = driver.find_element(By.CSS_SELECTOR, ".media_end_head_info_datestamp span")
                raw_date = date_element.text
            except:
                try:
                    # 전략 3: 아주 옛날 기사나 특이한 레이아웃 대응 (.t11)
                    raw_date = driver.find_element(By.CSS_SELECTOR, ".t11").text
                except:
                    # 전략 4: 속성값(data-date-time)에서 직접 가져오기 (화면에 안 보여도 소스엔 있을 수 있음)
                    try:
                        elem = driver.find_element(By.CSS_SELECTOR, ".media_end_head_info_datestamp")
                        raw_date = elem.get_attribute("data-date-time")
                    except:
                        raw_date = "날짜 없음"

        # 날짜 포맷 정리 (2-1 정의 함수)
        clean_date_str = clean_date(raw_date)
        # 진행 상황 출력
        print(f"  ▷ [수집] {title[:30]}... ({clean_date_str})")
        return {"날짜": clean_date_str, "제목": title, "본문": content, "링크": url}
    
    except Exception as e:
        # 에러 발생 시 로그만 남기고 멈추지 않고 계속 진행
        # print(f"[■ 에러] {e}")
        return None

# Ensure DB is found relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "projectDB.db")

def is_link_in_db(link):
    """Check if the link already exists in the database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM news WHERE link = ?", (link,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def insert_article(data, oid, press_name):
    """Insert a new article into the database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO news (link, date, title, content, oid)
            VALUES (?, ?, ?, ?, ?)
        ''', (data['링크'], data['날짜'], data['제목'], data['본문'], oid))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        conn.close()

def crawl_incremental():
    """
    Crawl from TODAY backwards.
    For each press (OID), continue crawling previous days until a duplicate article is found in DB.
    """
    current_date = datetime.now()
    
    print(f"[{datetime.now()}] Starting Incremental Crawl from {current_date.strftime('%Y-%m-%d')} backwards...")

    # Chrome Setup (Headless)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # Track active OIDs (Initially all)
    # We use a list of [name, oid] mechanism to remove finished ones
    active_targets = list(TARGET_PRESS_DICT.items())
    
    try:
        while active_targets:
            date_str = current_date.strftime("%Y%m%d")
            print(f"\n>>> Processing DATE: {date_str} | Remaining Press: {len(active_targets)}")
            
            finished_targets = []

            for press_name, oid in active_targets:
                print(f"   -> Scanning {press_name} ({oid})...")
                
                target_url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid={oid}&date={date_str}"
                driver.get(target_url)
                time.sleep(1)
                
                page_count = 1
                new_count_for_oid = 0
                stop_oid = False
                
                while True:
                    # 1. Collect Links
                    article_urls = []
                    selectors = [
                         "#main_content > div.list_body.newsflash_body > ul.type06_headline > li dl > dt:not(.photo) > a",
                         "#main_content > div.list_body.newsflash_body > ul.type06 > li dl > dt:not(.photo) > a"
                    ]
                    for sel in selectors:
                        links = driver.find_elements(By.CSS_SELECTOR, sel)
                        for link in links:
                            url = link.get_attribute("href")
                            if url: article_urls.append(url)
                    
                    if not article_urls:
                        break # No more articles on this page/date
                        
                    list_window = driver.current_window_handle
                    
                    for url in article_urls:
                        if "entertain.naver.com" in url or "sports.naver.com" in url:
                            continue
                            
                        # CHECK DUPLICATE
                        if is_link_in_db(url):
                             print(f"      [STOP] Found existing article: {url}")
                             stop_oid = True
                             break # Break inner URL loop
                        
                        # Extract
                        driver.execute_script("window.open('');")
                        driver.switch_to.window(driver.window_handles[-1])
                        
                        data = extract_article_info(driver, url)
                        
                        driver.close()
                        driver.switch_to.window(list_window)
                        
                        if data:
                            insert_article(data, oid, press_name)
                            new_count_for_oid += 1
                            print(f"      [INSERT] {data['제목'][:15]}...")
                    
                    if stop_oid:
                        break # Break Page Loop
                    
                    # Next Page
                    try:
                        paging_area = driver.find_element(By.CSS_SELECTOR, "#main_content > div.paging")
                        current_num = int(paging_area.find_element(By.TAG_NAME, "strong").text)
                        next_num = current_num + 1
                        
                        try:
                            next_btn = paging_area.find_element(By.XPATH, f".//a[text()='{next_num}']")
                            next_btn.click()
                            page_count += 1
                            time.sleep(0.7)
                        except:
                            try:
                                next_group = paging_area.find_element(By.CSS_SELECTOR, "a.next")
                                next_group.click()
                                page_count += 1
                                time.sleep(0.7)
                            except:
                                break # No next page
                    except:
                        break # No paging area
                
                print(f"      Done {press_name}. Inserted: {new_count_for_oid}")
                
                if stop_oid:
                    finished_targets.append((press_name, oid))
            
            # Remove finished targets
            for ft in finished_targets:
                active_targets.remove(ft)
                print(f"   !!! {ft[0]} Finished (Duplicate Found).")
            
            # Move to previous day
            current_date -= timedelta(days=1)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()
        print(f"\n[{datetime.now()}] Incremental Crawl Finished.")

if __name__ == "__main__":
    print("=== Incremental News Crawler (Nnews_Crawler_v3) ===")
    print("Crawling from TODAY backwards until duplicates are found.")
    print("-------------------------------------------------------------")
    
    crawl_incremental()
