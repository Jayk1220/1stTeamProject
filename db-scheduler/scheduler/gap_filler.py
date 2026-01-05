import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime

# 스크립트 실행 위치에 따라 모듈 경로 추가 (scheduler 폴더 기준)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 커스텀 모듈 임포트
from news_crawling.Nnews_Crawler_CSV import NewsCrawlerCSV
from industry_labeling.industry_classifier import IndustryClassifier
from sentiment_analysis.sentiment_analyzer import NewsSentimentAnalyzer

# ==============================================================================
# [설정 및 전역 변수]
# ==============================================================================
# Load .env
from dotenv import load_dotenv
import cx_Oracle

# Current: scheduler/gap_filler.py
# Root needed: Project1/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "web", ".env")
load_dotenv(ENV_PATH)

NEWS_CSV_PATH = "scheduler/db/news_db.csv"

# Oracle DB Configuration
DB_USER = os.getenv("DB_USER", "news_db")
DB_PASS = os.getenv("DB_PASS", "1234")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1521")
DB_SERVICE = os.getenv("DB_SERVICE", "xe")

# DB Config for cx_Oracle (Used by IndustryClassifier & SentimentAnalyzer)
DB_CONFIG = {
    "user": DB_USER,
    "password": DB_PASS,
    "dsn": cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
}

# [강제 탐색/적재 범위 설정]
# 이 변수들이 설정되면(None이 아니면) 해당 기간 내의 데이터만 강제로 크롤링하고 분석합니다.
# 둘 다 None이면 기본 모드(Incremental: 오늘부터 과거로 중복 전까지)로 동작합니다.
FORCE_START_DATE = None  # 예: "2025-12-30"
FORCE_END_DATE = None    # 예: "2025-01-01"

# Target Press List ("Press Name": "OID")
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

# 모델 인스턴스를 전역으로 관리 (Singleton / Lazy Loading)
classifier = None
sentiment_analyzer = None

# ==============================================================================
# [헬퍼 함수 정의]
# ==============================================================================

def init_models():
    """
    [모델 초기화]
    KoBERT(산업분류)와 KoElectra(감성분석) 모델을 필요할 때 메모리에 로드합니다.
    이미 로드되어 있다면 재사용합니다 (Singleton 패턴).
    """
    global classifier, sentiment_analyzer
    
    if classifier is None:
        print("[GapFiller] 산업분류 모델(KoBERT) 로딩 중...")
        try:
            # DB Config 전달 -> DB 연결 기능 활성화 (label_db_news 사용 가능)
            classifier = IndustryClassifier(db_config=DB_CONFIG)
        except Exception as e:
            print(f"[오류] 산업분류 모델 로드 실패: {e}")
        
    if sentiment_analyzer is None:
        print("[GapFiller] 감성분석 모델(KoElectra) 로딩 중...")
        try:
            # DB Config 전달 -> DB 연결 기능 활성화 (analyze_db_news 사용 가능)
            sentiment_analyzer = NewsSentimentAnalyzer(db_config=DB_CONFIG)
        except Exception as e:
            print(f"[오류] 감성분석 모델 로드 실패: {e}")

def get_csv_path():
    """CSV 파일 절대 경로 반환"""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Project1/
    # NEWS_CSV_PATH가 scheduler/db/news_db.csv 이므로
    return os.path.join(base, NEWS_CSV_PATH.replace('/', os.sep))

# ==============================================================================
# [Step 2 & 3: AI 분석 기능]
# ==============================================================================

def fill_missing_industry(start_date=None, end_date=None):
    """
    CSV의 'INDUSTRY' 컬럼이 비어있는 행을 찾아 산업분류 모델로 채웁니다.
    :param start_date: 처리 시작 날짜 (문자열 YYYY-MM-DD or None)
    :param end_date: 처리 종료 날짜 (문자열 YYYY-MM-DD or None)
    """
    init_models()
    if classifier is None:
        print("[오류] 산업분류 모델이 없어 작업을 건너뜁니다.")
        return

    csv_file = get_csv_path()
    if not os.path.exists(csv_file):
        print(f"[오류] CSV 파일 없음: {csv_file}")
        return

    df = pd.read_csv(csv_file)
    
    # 1. 미분류 데이터 필터링 (NaN or Empty)
    if 'INDUSTRY' not in df.columns:
        df['INDUSTRY'] = ""
    
    mask = (df['INDUSTRY'].isna()) | (df['INDUSTRY'] == "")
    
    # 2. 날짜 필터링 (옵션)
    if start_date and end_date:
        try:
            # CSV의 NDATE는 'YYYY-MM-DD HH:MM:SS' 형식이라고 가정
            dt_col = pd.to_datetime(df['NDATE'], errors='coerce')
            s_dt = pd.to_datetime(start_date)
            e_dt = pd.to_datetime(end_date)
            # 날짜 범위 내의 데이터만 마스킹
            date_mask = (dt_col >= s_dt) & (dt_col <= e_dt)
            mask = mask & date_mask
        except Exception as e:
            print(f"[경고] 날짜 필터링 중 오류: {e}")

    target_indices = df[mask].index
    
    if len(target_indices) == 0:
        print("[GapFiller] 산업분류가 필요한 데이터가 없습니다.")
        return

    print(f"[GapFiller] 산업분류 대상: {len(target_indices)}건")
    
    # 3. 예측 수행 (배치 처리 필요하지만 여기선 단순 반복 예시)
    # 효율을 위해 타이틀 리스트 추출 후 배치 예측 권장
    titles = df.loc[target_indices, 'TITLE'].fillna("").tolist()
    
    # 배치 예측 (classifier.predict_batch 내부 구현 활용)
    results = classifier.predict_batch(titles, batch_size=32)
    
    # 4. 결과 반영
    for idx, (label, prob) in zip(target_indices, results):
        df.at[idx, 'INDUSTRY'] = label
        
    # 5. 저장
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print("[GapFiller] 산업분류 완료 및 저장.")

def fill_missing_sentiment(start_date=None, end_date=None):
    """
    CSV의 'SENT_SCORE' 컬럼이 비어있고, 특정 산업군(자동차, 건설, 헬스케어)인 행에 대해 감성분석 수행
    :param start_date: 처리 시작 날짜 (문자열 YYYY-MM-DD or None)
    :param end_date: 처리 종료 날짜 (문자열 YYYY-MM-DD or None)
    """
    init_models()
    if sentiment_analyzer is None:
        print("[오류] 감성분석 모델이 없어 작업을 건너뜁니다.")
        return

    csv_file = get_csv_path()
    if not os.path.exists(csv_file):
        return

    df = pd.read_csv(csv_file)
    
    if 'SENT_SCORE' not in df.columns:
        df['SENT_SCORE'] = ""
        
    # 타겟 산업군 정의
    target_industries = ["자동차", "건설", "헬스케어"]
    
    # 1. 대상 필터링: 산업군 일치 AND 점수 없음
    # INDUSTRY가 타겟에 포함되어야 함
    ind_mask = df['INDUSTRY'].isin(target_industries)
    sent_mask = (df['SENT_SCORE'].isna()) | (df['SENT_SCORE'] == "")
    mask = ind_mask & sent_mask
    
    # 2. 날짜 필터링
    if start_date and end_date:
        try:
            dt_col = pd.to_datetime(df['NDATE'], errors='coerce')
            s_dt = pd.to_datetime(start_date)
            e_dt = pd.to_datetime(end_date)
            date_mask = (dt_col >= s_dt) & (dt_col <= e_dt)
            mask = mask & date_mask
        except Exception as e:
            print(f"[경고] 날짜 필터링 중 오류: {e}")

    target_indices = df[mask].index
    
    if len(target_indices) == 0:
        print("[GapFiller] 감성분석 대상이 없습니다.")
        return

    print(f"[GapFiller] 감성분석 대상: {len(target_indices)}건")
    
    # 3. 데이터 준비 (제목 + 본문 일부)
    texts = []
    for idx in target_indices:
        title = str(df.at[idx, 'TITLE'])
        content = str(df.at[idx, 'CONTENT'])
        text = f"{title} {content[:200]}"
        texts.append(text)
        
    # 4. 예측
    scores = sentiment_analyzer.predict_batch(texts, batch_size=32)
    
    # 5. 반영
    for idx, score in zip(target_indices, scores):
        df.at[idx, 'SENT_SCORE'] = score
        
    # 6. 저장
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print("[GapFiller] 감성분석 완료 및 저장.")

# ==============================================================================
# [메인 실행 함수]
# ==============================================================================

def run_gap_filler(start_date=None, end_date=None):
    """
    뉴스 파이프라인 전체 실행 (Step 1~3)
    1. 뉴스 크롤링 (Crawler)
    2. 산업 분류 (Gap Filling)
    3. 감성 분석 (Gap Filling)
    
    :param start_date: 강제 시작 날짜 (설정 시 이 날짜부터)
    :param end_date: 강제 종료 날짜
    """
    print("\n[GapFiller] 뉴스 데이터 파이프라인 시작...")
    
    # 1. 날짜 설정 (Global 설정을 우선)
    use_start = FORCE_START_DATE if FORCE_START_DATE else start_date
    use_end = FORCE_END_DATE if FORCE_END_DATE else end_date
    
    csv_path = get_csv_path()
    
    # --------------------------------------------------------------------------
    # Step 1: 뉴스 크롤링
    # --------------------------------------------------------------------------
    print("\n>>> [Step 1] 뉴스 크롤링 시작")
    # until_date는 '과거의 어느 시점까지'를 의미.
    # 만약 FORCE_START/END가 있다면, NewsCrawlerCSV의 until_date와 start_date를 맞춰준다.
    # NewsCrawlerCSV(until_date=..., start_date=...)
    # - start_date: 수집 시작일 (최신)
    # - until_date: 수집 마지노선 (과거)
    
    # 날짜 로직:
    # use_start (최신 날짜) -> use_end (과거 날짜)? 아니면 구간인가?
    # 보통 start_date="2025-12-30", end_date="2025-01-01" 이면 구간임.
    # 크롤러는 역순(최신->과거)이므로:
    # crawler.start_date = "2025-12-30" (시작점)
    # crawler.until_date = "2025-01-01" (멈추는 점 - 더 과거)
    # 따라서 use_start가 더 미래여야 함. (문자열 비교 주의)
    
    crawler_start = use_start
    crawler_until = use_end
    
    # 만약 날짜가 서로 반대라면(Start < End) 스왑해주거나 처리 필요
    # 여기서는 사용자가 올바르게 입력한다고 가정 (Start=최신, Until=과거)
    
    crawler = NewsCrawlerCSV(
        csv_path=csv_path, 
        press_dict=TARGET_PRESS_DICT, 
        start_date=crawler_start,
        until_date=crawler_until
    )
    crawler.run()
    
    # --------------------------------------------------------------------------
    # Step 2: 산업 분류 (빈 값 채우기)
    # --------------------------------------------------------------------------
    print("\n>>> [Step 2] 미분류 데이터 산업 라벨링 시작")
    fill_missing_industry(start_date=use_start, end_date=use_end)
    
    # --------------------------------------------------------------------------
    # Step 3: 감성 분석 (빈 값 채우기)
    # --------------------------------------------------------------------------
    print("\n>>> [Step 3] 타겟 산업군 감성 분석 시작")
    fill_missing_sentiment(start_date=use_start, end_date=use_end)
    
    print("\n[GapFiller] 모든 파이프라인 작업 완료.")

if __name__ == "__main__":
    # 테스트 실행
    run_gap_filler()
