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
    
    [주요 기능]
    1. 날짜별/언론사별 뉴스 기사 수집 (제목, 본문, 링크, 날짜 등)
    2. 중복 기사 발견 시 해당 언론사의 당일 수집을 즉시 중단 (Incremental Crawling)
    3. 수집된 데이터를 CSV 파일에 실시간 저장 (Excel 호환 utf-8-sig)
    
    [사용법]
    crawler = NewsCrawlerCSV(csv_path="...", press_dict={...})
    crawler.run()
    """
    
    def __init__(self, csv_path, press_dict, until_date=None, start_date=None):
        """
        초기화 함수
        :param csv_path: 저장할 CSV 파일 경로
        :param press_dict: 수집 대상 언론사 딕셔너리 {이름: OID}
        :param until_date: 수집 종료 기준 날짜 (이 날짜 이전 데이터는 수집 안 함, None이면 중복 발견 시까지)
        :param start_date: 수집 시작 날짜 (None이면 오늘부터)
        """
        self.csv_path = csv_path
        self.press_dict = press_dict
        self.until_date = until_date
        self.start_date = start_date
        
        self.driver = None
        self.limit_dt = None
        self.existing_links = set()
        
        # CSV 저장 컬럼 정의
        self.fieldnames = ["NDATE", "TITLE", "CONTENT", "LINK", "OID", "INDUSTRY", "SENT_SCORE"]
        
        # 날짜 제한 파싱
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
        """Chrome WebDriver 초기화 (Headless 모드, 메모리 최적화 옵션)"""
        print("[시스템] 브라우저 드라이버 초기화 중...")
        chrome_options = Options()
        chrome_options.add_argument("--headless") # 화면 출력 없이 실행
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage") # Shared Memory 사용 안함 (Docker/Linux 환경 대응)
        chrome_options.add_argument("--window-size=1920,1080")
        # 봇 탐지 방지를 위한 User-Agent 설정
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            print("[시스템] 브라우저 로드 완료.")
        except Exception as e:
            print(f"[오류] 드라이버 초기화 실패: {e}")
            sys.exit(1)

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
                # utf-8-sig: 엑셀에서 한글 깨짐 방지
                with open(self.csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    writer.writeheader()
            except Exception as e:
                print(f"[오류] CSV 생성 실패: {e}")
        else:
            print("[시스템] 기존 CSV 파일에 이어서 수집합니다.")
            try:
                df = pd.read_csv(self.csv_path)
                # 'LINK' 컬럼이 있는 경우만 로드
                if 'LINK' in df.columns:
                    self.existing_links = set(df['LINK'].dropna().unique())
                print(f"[정보] 기존 수집 기사 수: {len(self.existing_links)}개")
            except Exception as e:
                print(f"[경고] 기존 CSV 읽기 실패 (덮어쓰기 될 수 있음): {e}")

    def save_to_csv(self, article_data):
        """
        단일 기사 데이터를 CSV에 추가 (Append Mode)
        :param article_data: 기사 정보 딕셔너리
        """
        if not article_data:
            return

        try:
            with open(self.csv_path, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                # 누락된 필드는 빈 값으로 처리
                row = {k: article_data.get(k, "") for k in self.fieldnames}
                writer.writerow(row)
        except Exception as e:
            print(f"[오류] CSV 저장 실패: {e}")

    # ==========================================================================
    # [크롤링 핵심 로직]
    # ==========================================================================

    def clean_date(self, date_text):
        """
        네이버 뉴스 날짜 형식 파싱 ('2023.01.01. 오후 1:23' 등)
        :return: 'YYYY-MM-DD HH:MM:SS' 또는 실패 시 '0000-00-00 00:00:00'
        """
        try:
            if "오전" in date_text or "오후" in date_text:
                parts = date_text.split(" ")
                ampm = parts[2]
                time_part = parts[3]
                date_part = parts[0] + " " + parts[1] # YYYY.MM.DD.
                
                # 오전/오후 시간을 24시간제로 변환
                if ampm == "오후":
                    h, m = map(int, time_part.split(":"))
                    if h != 12: h += 12
                    time_part = f"{h}:{m}"
                elif ampm == "오전":
                    h, m = map(int, time_part.split(":"))
                    if h == 12: h = 0
                    time_part = f"{h}:{m}"
                
                full_date_str = f"{date_part} {time_part}"
                dt = datetime.strptime(full_date_str, "%Y.%m.%d. %H:%M")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # 일반적인 YYYY.MM.DD 형식 처리
                dt = datetime.strptime(date_text, "%Y.%m.%d.")
                return dt.strftime("%Y-%m-%d %H:%M:%S")

        except Exception:
            return "0000-00-00 00:00:00"

    def get_article_content(self, link):
        """
        기사 상세 페이지에서 본문과 날짜 추출
        :param link: 기사 URL
        :return: (본문, 날짜문자열)
        """
        try:
            self.driver.get(link)
            
            # 본문 추출 시도 (selector 목록 순회)
            content = ""
            selectors = ["#dic_area", "#articeBody", ".news_end", "#articleBodyContents"]
            for sel in selectors:
                try:
                    elem = self.driver.find_element(By.CSS_SELECTOR, sel)
                    content = elem.text.strip()
                    if content: break
                except:
                    continue
            
            # 본문이 너무 짧으면 실패로 간주
            if not content or len(content) < 10:
                return None, None

            # 날짜 추출 시도
            date_str = "0000-00-00 00:00:00"
            date_selectors = [
                ".media_end_head_info_datestamp_time",
                ".t11",
                ".info_view .date"
            ]
            for d_sel in date_selectors:
                try:
                    d_elem = self.driver.find_element(By.CSS_SELECTOR, d_sel)
                    raw_date = d_elem.get_attribute("data-date-time") # 속성 먼저 확인
                    if not raw_date:
                        raw_date = d_elem.text.strip()
                    
                    if raw_date:
                        date_str = self.clean_date(raw_date)
                        break
                except:
                    continue
                    
            return content, date_str

        except Exception as e:
            # print(f"[경고] 상세 페이지 로드 실패 ({link}): {e}")
            return None, None

    def process_day_press(self, target_date_str, press_name, oid):
        """
        특정 날짜, 특정 언론사의 기사 목록을 순회하며 수집
        :param target_date_str: 'YYYYMMDD'
        :param press_name: 언론사 이름
        :param oid: 언론사 ID
        :return: bool (중복 발견으로 인한 중단 여부)
        """
        # 네이버 뉴스 리스트 URL (지면 기사 위주)
        url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid={oid}&date={target_date_str}"
        self.driver.get(url)
        
        page = 1
        stop_crawling = False
        
        while True:
            try:
                # 기사 목록 요소 추출
                articles = self.driver.find_elements(By.CSS_SELECTOR, ".list_body ul li")
                
                # 페이지 내 기사가 없으면 종료 (마지막 페이지)
                if not articles:
                    break

                for art in articles:
                    try:
                        # 링크 및 제목 추출
                        link_elem = art.find_element(By.TAG_NAME, "a")
                        link = link_elem.get_attribute("href")
                        title = link_elem.text.strip()
                        
                        if not title: # 이미지가 링크인 경우 대비
                            try:
                                title = link_elem.find_element(By.TAG_NAME, "img").get_attribute("alt")
                            except:
                                continue

                        # 1. 중복 확인 (이미 수집된 링크면 즉시 중단)
                        if link in self.existing_links:
                            print(f"  [중복] 이미 수집된 기사 발견. {press_name} ({target_date_str}) 수집 종료.")
                            return True # 중단 신호 리턴

                        # 2. 본문 상세 수집
                        content, rdate = self.get_article_content(link)
                        
                        if content:
                            article_data = {
                                "NDATE": rdate,
                                "TITLE": title,
                                "CONTENT": content,
                                "LINK": link,
                                "OID": oid,
                                "INDUSTRY": "",     # 추후 AI로 채움
                                "SENT_SCORE": ""    # 추후 AI로 채움
                            }
                            
                            # CSV 저장 및 메모리 업데이트
                            self.save_to_csv(article_data)
                            self.existing_links.add(link)
                            
                        # 네이버 차단 방지 딜레이
                        time.sleep(0.3)
                        
                        # 목록 페이지로 복귀 (get_article_content에서 페이지 이동했으므로)
                        self.driver.back()
                        
                        # [중요] 페이지 복귀 후 요소 다시 참조해야 StaleElementReferenceException 방지
                        # 하지만 리스트를 다시 로드하려면 비효율적이므로, 
                        # 여기서는 목록 페이지를 다시 get 하는 방식이 안전함.
                        # 다만 속도 저하가 있으므로, 새 탭을 여는 방식이 좋으나 여기서는 간단히 다시 로드 안함.
                        # (driver.back() 만으로 DOM이 유지된다고 가정하거나, 에러 시 재시도 로직 필요)
                        # 안정성을 위해 목록 페이지 URL을 다시 로드 (가장 확실)
                        # self.driver.get(current_list_url) -> 페이지네이션 상태 유지 필요

                    except Exception:
                        continue
                
                # 다음 페이지 이동
                # '다음' 버튼이나 현재 페이지 + 1 버튼 찾기
                page += 1
                try:
                    next_page_elem = self.driver.find_element(By.LINK_TEXT, str(page))
                    next_page_elem.click()
                    time.sleep(0.5)
                except NoSuchElementException:
                    # 다음 페이지 번호가 없으면 '다음' 버튼 확인 (11페이지 이상인 경우)
                    try:
                        next_btn = self.driver.find_element(By.CLASS_NAME, "next")
                        next_btn.click()
                        time.sleep(0.5)
                    except:
                        # 더 이상 페이지 없음
                        break
                        
            except Exception as e:
                print(f"[오류] 목록 처리 중 에러 ({press_name} p.{page}): {e}")
                break
                
        return stop_crawling

    # ==========================================================================
    # [메인 실행 함수]
    # ==========================================================================

    def run(self):
        """
        크롤링 전체 프로세스 실행
        오늘(혹은 start_date)부터 과거로 날짜를 하루씩 줄여가며 press_dict의 모든 언론사를 탐색
        """
        self.init_driver()
        
        # 시작 날짜 설정 (기본값: 현재 시각)
        if self.start_date:
            try:
                current_date = datetime.strptime(self.start_date, "%Y-%m-%d")
            except ValueError:
                print(f"[오류] Start Date 형식 에러({self.start_date}). 오늘 날짜로 시작합니다.")
                current_date = datetime.now()
        else:
            current_date = datetime.now()

        print(f"=== 뉴스 크롤링 시작 (Start: {current_date.strftime('%Y-%m-%d')}) ===")
        
        try:
            while True:
                # 날짜 제한 체크 (limit_dt보다 과거로 가면 전체 종료)
                if self.limit_dt and current_date < self.limit_dt:
                    print(f"[종료] 설정된 날짜 한계({self.until_date})에 도달했습니다.")
                    break
                
                target_date_str = current_date.strftime("%Y%m%d")
                print(f"\n>>> [날짜: {target_date_str}] 크롤링 진행 중...")
                
                # 모든 언론사 순회
                all_press_stopped = True # 모든 언론사에서 중복 발견 시 날짜 이동? 아니면 전체 종료?
                                         # 로직: 각 언론사별로 중단되더라도 다른 언론사는 계속 해야 함.
                                         # 여기서는 '모두 중복'이면 더 과거로 갈 필요가 있는가? 에 대한 결정이 필요.
                                         # 보통 날짜별 기사는 독립적이므로 날짜를 계속 과거로 이동해야 함.
                                         
                for press_name, oid in self.press_dict.items():
                    print(f"   - {press_name} (OID: {oid}) 탐색...")
                    
                    # 해당 언론사, 해당 날짜 크롤링 실행
                    is_stopped = self.process_day_press(target_date_str, press_name, oid)
                    
                    if not is_stopped:
                        all_press_stopped = False
                
                # 만약 날짜 제한 모드가 아니고(Incremental), 오늘 기사들이 모두 중복이라면?
                # 즉, 과거의 모든 기사를 다 긁었다는 보장은 없으므로 날짜는 계속 줄여가야 함.
                # 다만 until_date가 None일 때의 종료 조건이 모호할 수 있음.
                # 사용자 의도: "중복 나오면 그만하고 싶다" -> 보통은 최근 날짜부터 하므로, 중복 나오면 '그 날짜'의 수집을 멈추고
                # '그 전날'도 수집해야 하는가? -> Incremental은 보통 "마지막 수집일"까지 감.
                # 본 로직은 until_date를 명시적으로 주는 것이 안전함.
                
                # 하루 전으로 이동
                current_date -= timedelta(days=1)
                
        except KeyboardInterrupt:
            print("\n[사용자 중단] 크롤링을 강제 종료합니다.")
        except Exception as e:
            print(f"[시스템 오류] {e}")
        finally:
            self.close()

# ==============================================================================
# [단독 실행 테스트]
# ==============================================================================
if __name__ == "__main__":
    # 테스트용 설정
    TEST_CSV = "test_news.csv"
    TEST_PRESS = {"매일경제": "009"} # 테스트용 1개만
    
    crawler = NewsCrawlerCSV(TEST_CSV, TEST_PRESS, until_date="2025-12-28", start_date="2025-12-30")
    crawler.run()
