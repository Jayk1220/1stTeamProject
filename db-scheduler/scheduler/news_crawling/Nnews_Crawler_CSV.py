import time
import os
import sys
import csv
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ==============================================================================
# [뉴스 크롤러 클래스 정의]
# ==============================================================================

class NewsCrawlerCSV:
    """
    네이버 뉴스 크롤러 (CSV 저장 전용)
    
    주요 기능:
    1. 날짜별/언론사별 뉴스 기사 수집
    2. 중복 기사 발견 시 즉시 중단 (Incremental Crawling)
    3. 수집된 데이터를 CSV 파일에 실시간 저장
    """
    
    def __init__(self, csv_path, press_dict, until_date=None):
        """
        초기화 함수
        :param csv_path: 저장할 CSV 파일 경로
        :param press_dict: 수집 대상 언론사 딕셔너리 {이름: OID}
        :param until_date: 과거 수집 제한 날짜 (None이면 무한 수집이 아닌 중복 발견 시 중단 모드)
        """
        self.csv_path = csv_path
        self.press_dict = press_dict
        self.until_date = until_date
        self.driver = None
        self.limit_dt = None
        self.existing_links = set()
        
        # CSV 컬럼 정의
        self.fieldnames = ["날짜", "제목", "본문", "링크", "OID", "INDUSTRY", "SENT_SCORE"]
        
        # 날짜 제한 설정
        if self.until_date:
            try:
                self.limit_dt = datetime.strptime(self.until_date, "%Y-%m-%d")
            except ValueError:
                print(f"[오류] 날짜 형식이 올바르지 않습니다: {self.until_date}. (YYYY-MM-DD 권장)")
                sys.exit(1)

        # 기존 데이터 로드 (중복 체크용)
        self.load_existing_links()

    # ==========================================================================
    # [설정 및 리소스 관리]
    # ==========================================================================

    def init_driver(self):
        """Chrome WebDriver 초기화 (Headless 모드 권장)"""
        print("[시스템] 브라우저 드라이버 초기화 중...")
        chrome_options = Options()
        chrome_options.add_argument("--headless") # 화면 없이 실행
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080") 
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        print("[시스템] 브라우저 로드 완료.")

    def close(self):
        """리소스 정리 및 브라우저 종료"""
        if self.driver:
            self.driver.quit()
        print("[시스템] 크롤러 종료 및 리소스 해제.")

    # ==========================================================================
    # [CSV 파일 관리]
    # ==========================================================================

    def load_existing_links(self):
        """기존 CSV 파일을 읽어 이미 수집된 기사 링크를 메모리에 로드 (중복 방지)"""
        print(f"[시스템] 타겟 CSV: {self.csv_path}")
        
        if not os.path.exists(self.csv_path):
            print("[시스템] CSV 파일이 없어 새로 생성합니다.")
            try:
                with open(self.csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writeheader()
            except Exception as e:
                print(f"[오류] CSV 생성 실패: {e}")
        else:
            print("[시스템] 기존 CSV 파일에 이어서 수집합니다.")
            try:
                # 링크 컬럼만 최적화하여 로드
                df = pd.read_csv(self.csv_path, usecols=["링크"])
                self.existing_links = set(df["링크"].dropna().astype(str))
                print(f"[시스템] 기존 수집된 기사 {len(self.existing_links)}건 로드 완료.")
            except Exception as e:
                print(f"[경고] 기존 데이터 로드 실패 (파일이 비었을 수 있음): {e}")
                self.existing_links = set()

    def is_link_in_db(self, link):
        """메모리 상에서 링크 중복 여부 확인"""
        return link in self.existing_links

    def insert_article(self, data, oid):
        """수집된 기사를 CSV에 한 줄 추가"""
        if not data: return
        
        row = {
            "날짜": data['날짜'],
            "제목": data['제목'],
            "본문": data['본문'],
            "링크": data['링크'],
            "OID": oid,
            "INDUSTRY": "",     # 빈 값 (추후 AI로 채움)
            "SENT_SCORE": ""    # 빈 값 (추후 AI로 채움)
        }
        
        try:
            with open(self.csv_path, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerow(row)
            
            # 캐시 업데이트
            self.existing_links.add(data['링크'])
            
        except Exception as e:
            print(f"[CSV 저장 오류] {e}")

    # ==========================================================================
    # [유틸리티 함수]
    # ==========================================================================
    
    def clean_date(self, date_str):
        """날짜 문자열을 표준 포맷(YYYY-MM-DD HH:MM:SS)으로 변환"""
        try:
            date_str = str(date_str).replace("기사입력", "").replace("입력", "").strip()
            is_pm = "오후" in date_str
            date_str = date_str.replace("오전", "").replace("오후", "").strip()
            
            # 네이버 뉴스 날짜 포맷: 2025.12.15. 10:30
            dt = datetime.strptime(date_str, "%Y.%m.%d. %H:%M")
            
            if is_pm and dt.hour != 12: 
                dt = dt.replace(hour=dt.hour + 12)
            elif not is_pm and dt.hour == 12: 
                dt = dt.replace(hour=0)
                
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            # 파싱 실패 시 원본 반환
            return date_str

    # ==========================================================================
    # [크롤링 핵심 로직]
    # ==========================================================================

    def extract_article_info(self, url):
        """기사 상세 페이지로 이동하여 제목, 본문, 날짜 추출"""
        try:
            self.driver.get(url)
            time.sleep(0.3) 
            
            curr_url = self.driver.current_url
            # 연예/스포츠 뉴스 제외
            if any(x in curr_url for x in ["entertain.naver.com", "sports.news.naver.com", "sports.naver.com"]):
                return None

            # 제목 로딩 대기
            try:
                WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#title_area > span")))
            except TimeoutException:
                # 타임아웃 시 한 번 더 URL 체크 (리다이렉트 가능성)
                curr_url = self.driver.current_url
                if any(x in curr_url for x in ["entertain.naver.com", "sports.naver.com"]):
                    return None
                return None

            # 제목 추출
            try: title = self.driver.find_element(By.CSS_SELECTOR, "#title_area > span").text
            except: title = "제목 없음"

            # 본문 추출 (불필요 태그 제거)
            try:
                dic_area = self.driver.find_element(By.CSS_SELECTOR, "#dic_area")
                self.driver.execute_script("""
                    var element = arguments[0];
                    var dirts = element.querySelectorAll(".img_desc, .media_end_summary"); 
                    for (var i = 0; i < dirts.length; i++) { dirts[i].remove(); }
                """, dic_area)
                content = dic_area.text.replace("\n", " ").strip()
            except: content = "본문 없음"

            # 날짜 추출 (여러 선택자 시도)
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
            
            # data-attribute 백업 시도
            if raw_date == "날짜 없음":
                try:
                    elem = self.driver.find_element(By.CSS_SELECTOR, ".media_end_head_info_datestamp")
                    raw_date = elem.get_attribute("data-date-time")
                except: pass

            clean_dt = self.clean_date(raw_date)
            print(f"  ▷ [Scraping] {title[:20]}... ({clean_dt})")
            
            return {"날짜": clean_dt, "제목": title, "본문": content, "링크": url}

        except Exception as e:
            return None

    def process_day_press(self, date_str, press_name, oid):
        """특정 날짜, 특정 언론사의 기사 리스트를 순회하며 수집"""
        target_url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid={oid}&date={date_str}"
        self.driver.get(target_url)

        # 리스트 로딩 대기
        try:
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#main_content > div.list_body"))
            )
        except TimeoutException:
            print(f"   [!] 컨텐츠 로딩 실패 또는 기사 없음: {press_name} ({date_str})")
            return True, 0

        page_count = 1
        inserted_count = 0
        stop_press = False
        session_crawled_links = set() # 현재 세션 임시 저장

        while True:
            # 기사 링크 수집
            try:
                article_urls = []
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
                print(f"   [!] 링크 수집 중 에러: {e}")
                break

            if not article_urls:
                break

            main_window = self.driver.current_window_handle
            
            for url in article_urls:
                # 1. 제외 대상 확인
                if "entertain.naver.com" in url or "sports.naver.com" in url:
                    continue
                
                # 2. 중복 확인
                if url in session_crawled_links:
                    continue
                
                if self.is_link_in_db(url):
                    if self.limit_dt:
                        # 과거 수집 모드면 계속 진행 (혹시 건너뛴 게 있을까봐?) - 보통 중단이 맞으나 로직 유지
                        continue
                    else:
                        # [증분 모드] 이미 있는 기사를 만나면, 더 이상 과거 기사는 볼 필요 없음
                        print(f"      [중단] 기존 데이터 발견: {url}")
                        stop_press = True
                        break
                
                # 3. 새 탭에서 기사 수집
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
                    print(f"      [!] 탭 처리 오류: {e}")
                    self.driver.switch_to.window(main_window)

            if stop_press:
                break
            
            # 4. 다음 페이지 이동
            try:
                paging_div = self.driver.find_element(By.CSS_SELECTOR, "#main_content > div.paging")
                current_num = int(paging_div.find_element(By.TAG_NAME, "strong").text)
                next_num = current_num + 1
                
                try:
                    # 번호 버튼 클릭
                    next_btn = paging_div.find_element(By.XPATH, f".//a[normalize-space()='{next_num}']")
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    page_count += 1
                    time.sleep(1.0)
                except NoSuchElementException:
                    try:
                        # '다음' 화살표 클릭
                        next_arrow = paging_div.find_element(By.CSS_SELECTOR, "a.next")
                        self.driver.execute_script("arguments[0].click();", next_arrow)
                        page_count += 1
                        time.sleep(1.0)
                    except NoSuchElementException:
                        break # 마지막 페이지
            except (NoSuchElementException, TimeoutException):
                break
            except Exception as e:
                print(f"   [!] 페이징 처리 오류: {e}")
                break
        
        return stop_press, inserted_count

    def run(self):
        """메인 실행 루프"""
        self.init_driver()
        
        current_date = datetime.now()
        active_targets = list(self.press_dict.items())
        
        print("=======================================================")
        print(f" News Crawler CSV 실행 (시작: {current_date})")
        print(f" 모드: {'Gap Filling (과거 ' + str(self.until_date) + '까지)' if self.until_date else 'Incremental (중복 발견 시 중단)'}")
        print("=======================================================")

        try:
            while active_targets:
                date_str = current_date.strftime("%Y%m%d")
                
                # 날짜 제한 확인
                if self.limit_dt:
                    if current_date.date() < self.limit_dt.date():
                        print(f"\n[알림] 설정된 날짜 한계({self.until_date})에 도달했습니다. 종료합니다.")
                        break
                
                print(f"\n>>> 날짜: {date_str} | 남은 언론사: {len(active_targets)}")
                
                finished_targets = []
                
                for press_name, oid in active_targets:
                    print(f"   -> [{press_name}] 스캔 중...")
                    is_stopped, count = self.process_day_press(date_str, press_name, oid)
                    
                    print(f"      결과: {count}건 저장됨.")
                    if is_stopped:
                        print(f"      [완료] {press_name}: 최신 데이터까지 수집 완료.")
                        finished_targets.append((press_name, oid))
                
                # 완료된 언론사 제거
                for ft in finished_targets:
                    active_targets.remove(ft)
                
                # 하루 전으로 이동
                current_date -= timedelta(days=1)
                
        except KeyboardInterrupt:
            print("\n[!] 사용자 중단.")
        except Exception as e:
            print(f"\n[!] 치명적 오류 발생: {e}")
        finally:
            self.close()
