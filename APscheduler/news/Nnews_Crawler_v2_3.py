import time
import os
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =================================================================================
# 📌 1. 범위 설정 - 크롤링 대상 언론사와 기간 결정

# 1-1. 수집할 언론사 목록 ("언론사명": "oid코드") → 3-3-1 에서 순환 사용
# 주석(#)을 적용/해제하는 방식으로 선택
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
# ---------------------------------------------------------------------------------
# 1-2. 수집 기간 설정 (YYYYMMDD)
START_DATE = "20251219"   # 시작일
END_DATE   = "20251219"   # 종료일
# =================================================================================

# =================================================================================
# 2. 기능(함수) 정의
# 2-1.  날짜 문자열 정리 함수 정의
# 네이버 뉴스 특유의 "오후/오전" 포맷을 Python 표준 datetime(YYYY-MM-DD HH:MM:SS)으로 변환.
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
# ---------------------------------------------------------------------------------
# 2-2. 크롬 브라우저 설정 및 실행
# Selenium 웹 드라이버를 옵션과 함께 실행.
def set_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # 네이버의 봇 탐지 방지를 위한 User-Agent 설정
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    # 드라이버 자동 설치 및 실행
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=chrome_options
        )
    return driver
# ---------------------------------------------------------------------------------
# 2-3. 기사 상세 내용 추출
# 개별 기사 페이지에 들어가서 제목, 본문, 날짜를 가져오기.
# (연예/스포츠 기사 필터링하여 건너뜀.)
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
# ---------------------------------------------------------------------------------
# 2-4. 하루치 기사 리스트 순회 (페이지 넘김 기능 포함)
# 특정 언론사(oid)의 특정 날짜(date_str) 리스트 페이지를 1페이지부터 끝까지 색인.
def crawl_one_day(driver, oid, date_str):
    # 해당 날짜의 리스트 페이지 URL 생성 (URL 조작 방식 사용)
    target_url = f"https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid={oid}&date={date_str}"
    print(f"\n◆ [날짜 진입] {date_str} 리스트 탐색 시작 -> {target_url}") # 탐색 범위 확인용
    driver.get(target_url)
    time.sleep(1) # 리스트 페이지 로딩 대기
    daily_data = [] # 하루치 데이터 수집 리스트
    page_count = 1
    
    while True:
        print(f"  ▶{page_count} 페이지 스캔 중…") # 
        # 2-4-1. 현재 페이지의 기사 링크 수집
        article_urls = []
        selectors = [ # 썸네일형/리스트형 구조의 두 가지 선택자 모두 확인
            "#main_content > div.list_body.newsflash_body > ul.type06_headline > li dl > dt:not(.photo) > a",
            "#main_content > div.list_body.newsflash_body > ul.type06 > li dl > dt:not(.photo) > a"
        ]
        for sel in selectors:
            links = driver.find_elements(By.CSS_SELECTOR, sel)
            for link in links:
                url = link.get_attribute("href")
                if url: article_urls.append(url)
        # 2-4-2. 상세 페이지 진입 및 데이터 추출
        list_window = driver.current_window_handle # 현재 리스트 창 변수 할당
        for url in article_urls:
            # url 문자열 자체에 연예/스포츠가 포함되어 있으면 접속 거름 (크롤링 지연 요소 방지)
            if "entertain.naver.com" in url or "sports.naver.com" in url:
                continue
            # 새 탭 열기 (리스트 페이지는 유지)
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])
            # 상세 내용 가져오기 함수(2-3 정의) 호출
            data = extract_article_info(driver, url)
            if data: daily_data.append(data)
            # 탭 닫고 리스트로 복귀
            driver.close()
            driver.switch_to.window(list_window)
        # 2-4-3. 다음 페이지 이동
        try:
            paging_area = driver.find_element(By.CSS_SELECTOR, "#main_content > div.paging")
            current_num = int(paging_area.find_element(By.TAG_NAME, "strong").text)
            next_num = current_num + 1
            # 2-4-3-1. 현재 페이지의 +1 숫자 버튼이 있는지 확인
            try:
                next_btn = paging_area.find_element(By.XPATH, f".//a[text()='{next_num}']")
                next_btn.click()
                page_count += 1
                time.sleep(1.5) # 페이지 로딩 대기
                continue
            except: pass
            
            # 2-4-3-2. 다음 숫자 버튼이 없으면 '다음' 화살표 버튼 확인
            try:
                next_group = paging_area.find_element(By.CSS_SELECTOR, "a.next")
                next_group.click()
                page_count += 1
                time.sleep(1.5)
                continue
            except:
                # 다음 버튼도 없으면 마지막 페이지임
                print(f"   ◇ {date_str} 수집 완료 (총 {len(daily_data)}건 추출)")
                break
        except:
            # 페이징 영역 자체를 못 찾으면 기사가 거의 없는 경우니까 break
            break

    return daily_data
# =================================================================================

# =================================================================================
# 3. 메인 실행 함수
# 언론사 루프 -> 날짜 루프 -> 페이지 루프 -> 저장
def main():
    # 3-1. 크롬 드라이버 실행
    driver = set_chrome_driver()
    # 3-2. 날짜 범위 계산 (Start ~ End 사이의 일수)
    start_dt = datetime.strptime(START_DATE, "%Y%m%d")
    end_dt = datetime.strptime(END_DATE, "%Y%m%d")
    delta_days = (end_dt - start_dt).days + 1
    # 3-3. 순환-수집-저장
    try:
        # 3-3-1. 언론사별 순환
        for press_name, oid in TARGET_PRESS_DICT.items():
            print(f"\n\n========================================================")
            print(f"◈ 언론사 크롤링 시작: {press_name} (OID: {oid})")
            print(f"========================================================")
            # [폴더 생성] 언론사 이름으로 폴더를 생성
            # 예: ./매일경제_009/
            save_folder = f"{oid}_{press_name}"
            if not os.path.exists(save_folder): # 없으면 만들기
                os.makedirs(save_folder)
        # 3-3-2. 날짜별 순환
            for i in range(delta_days):
                current_date = start_dt + timedelta(days=i)
                date_str = current_date.strftime("%Y%m%d") # YYYYMMDD
        # 3-3-3. 해당 일자 크롤링 수행 (2-4 정의 함수)
                one_day_data = crawl_one_day(driver, oid, date_str)
        # 3-3-4. 수집된 해당 일자 데이터 파일 저장
                if one_day_data:
                    df = pd.DataFrame(one_day_data)
                    df = df[['날짜', '제목', '본문', '링크']] # 컬럼 순서 지정
                    # 기존 저장 파일 형식: 폴더명/oid=009&date=20251215.csv
                    # filename = f"oid={oid}&date={date_str}.csv"
                    # 새 저장 파일 형식: 폴더명/009-20251215.csv
                    filename = f"{oid}-{date_str}.csv"
                    save_path = os.path.join(save_folder, filename)
                    df.to_csv(save_path, index=False, encoding='utf-8-sig') # 즉시 확인 목적 일단 csv로. 추후 별도 변환.
                    print(f"   ■ 저장 완료: {save_path} ({len(df)}개)")
                else:
                    print(f"   △{date_str}일자에는 수집된 기사가 없습니다.")                   
            print(f"◆ {press_name} 크롤링이 모두 완료되었습니다.")
            
    except KeyboardInterrupt:
        print("\n!! [중단] 작업을 강제로 종료합니다.")
        
    except Exception as e:
        print(f"\n!! [오류] 실행 중 문제가 발생했습니다: {e}")
        
    finally:
        driver.quit()
        print("\n◈ 모든 크롤링 작업이 종료되었습니다.")
# =================================================================================

if __name__ == "__main__":
    main()
