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
from news_crawling.naver_news.Nnews_Crawler_v4 import TARGET_PRESS_DICT

# ==============================================================================
# [설정 및 전역 변수]
# ==============================================================================
NEWS_CSV_PATH = "scheduler/db/news_db.csv"

# 모델 인스턴스를 전역으로 관리 (Lazy Loading)
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
            # DB URL None 전달 -> DB 직접 연결 없이 추론 기능만 사용
            classifier = IndustryClassifier(db_url=None)
        except Exception as e:
            print(f"[오류] 산업분류 모델 로드 실패: {e}")
        
    if sentiment_analyzer is None:
        print("[GapFiller] 감성분석 모델(KoElectra) 로딩 중...")
        try:
            sentiment_analyzer = NewsSentimentAnalyzer(db_config=None)
        except Exception as e:
            print(f"[오류] 감성분석 모델 로드 실패: {e}")

def get_csv_path():
    """
    [경로 탐색]
    실행 위치에 따라 CSV 파일의 절대/상대 경로를 찾아 반환합니다.
    """
    candidates = [
        NEWS_CSV_PATH,
        "db/news_db.csv",
        "../scheduler/db/news_db.csv"
    ]
    
    for path in candidates:
        if os.path.exists(path):
            return path
            
    # 파일이 아직 없다면 기본 설정 경로 반환 (새로 생성용)
    return NEWS_CSV_PATH

# ==============================================================================
# [핵심 로직 함수]
# ==============================================================================

def fill_missing_industry(df):
    """
    [Step 2] 산업분류 (Industry Classification)
    'INDUSTRY' 컬럼이 비어있는 기사를 찾아 KoBERT 모델로 분류합니다.
    """
    global classifier
    dirty = False
    
    # NaN 값을 빈 문자열로 통일
    df["INDUSTRY"] = df["INDUSTRY"].fillna("")
    
    # 분류가 필요한 데이터 필터링 (빈 값 or 'nan' 문자열)
    mask = (df["INDUSTRY"] == "") | (df["INDUSTRY"] == "nan")
    target_idx = df[mask].index
    
    if len(target_idx) == 0:
        print("   -> 모든 기사에 산업분류가 완료되었습니다.")
        return df, False

    print(f"   -> 미분류 기사 {len(target_idx)}건 발견. 분류를 시작합니다...")
    
    # 모델 로드 확인
    init_models()
    if classifier is None:
        print("   -> [중단] 모델이 로드되지 않아 건너뜁니다.")
        return df, False

    # 배치 처리 준비
    batch_size = 128
    texts = []
    valid_indices = []

    for idx in target_idx:
        title = str(df.loc[idx, "제목"])
        content = str(df.loc[idx, "본문"])
        # 본문이 충분히 길면 본문 사용, 아니면 제목 사용
        text_input = content if len(content) > 10 else title
        texts.append(text_input)
        valid_indices.append(idx)

    # 배치 단위 추론 수행
    processed_cnt = 0
    for i in range(0, len(texts), batch_size):
        b_texts = texts[i:i+batch_size]
        b_idxs = valid_indices[i:i+batch_size]
        
        try:
            # predict_batch 결과: [(label, prob), ...]
            results = classifier.predict_batch(b_texts)
            labels = [r[0] for r in results]
            
            # DataFrame 업데이트
            df.loc[b_idxs, "INDUSTRY"] = labels
            dirty = True
            
            processed_cnt += len(b_texts)
            print(f"      [진행률] {processed_cnt}/{len(texts)} 분류 완료")
            
        except Exception as e:
            print(f"      [오류] 배치 처리 중 에러 발생: {e}")

    return df, dirty

def fill_missing_sentiment(df):
    """
    [Step 3] 감성분석 (Sentiment Analysis)
    특정 산업군(자동차, 건설, 헬스케어)에 속하면서 'SENT_SCORE'가 비어있는 기사를 분석합니다.
    """
    global sentiment_analyzer
    dirty = False
    
    # 타겟 산업군 정의
    target_industries = ["자동차", "건설", "헬스케어"]
    
    # 점수 컬럼 NaN 처리
    df["SENT_SCORE"] = df["SENT_SCORE"].fillna("")
    
    # 분석 대상 필터링: (타겟 산업군 포함) AND (점수 없음)
    mask = (
        df["INDUSTRY"].isin(target_industries) & 
        ((df["SENT_SCORE"] == "") | (df["SENT_SCORE"] == "nan"))
    )
    target_idx = df[mask].index
    
    if len(target_idx) == 0:
        print("   -> 타겟 산업군 내 분석할 기사가 없습니다.")
        return df, False

    print(f"   -> 감성분석 대상 {len(target_idx)}건 발견. 분석을 시작합니다...")
    
    # 모델 로드 확인
    init_models()
    if sentiment_analyzer is None:
        print("   -> [중단] 모델이 로드되지 않아 건너뜁니다.")
        return df, False

    # 배치 처리 준비
    batch_size = 64
    texts = []
    valid_indices = []
    
    for idx in target_idx:
        title = str(df.loc[idx, "제목"])
        content = str(df.loc[idx, "본문"])
        text_input = content if len(content) > 10 else title
        texts.append(text_input)
        valid_indices.append(idx)
        
    # 배치 단위 추론 수행
    processed_cnt = 0
    for i in range(0, len(texts), batch_size):
        b_texts = texts[i:i+batch_size]
        b_idxs = valid_indices[i:i+batch_size]
        
        try:
            # predict_batch 결과: [-0.5, 0.8, ...] (float 리스트)
            scores = sentiment_analyzer.predict_batch(b_texts)
            
            df.loc[b_idxs, "SENT_SCORE"] = scores
            dirty = True
            
            processed_cnt += len(b_texts)
            print(f"      [진행률] {processed_cnt}/{len(texts)} 분석 완료")
            
        except Exception as e:
            print(f"      [오류] 배치 처리 중 에러 발생: {e}")
            
    return df, dirty

# ==============================================================================
# [메인 실행 함수]
# ==============================================================================

def run_gap_filler():
    """
    [Gap Filler 실행]
    1. 뉴스 크롤링: 최신 기사를 수집하여 CSV에 추가
    2. 데이터 로드: CSV 파일 읽기
    3. 결측치 채우기: 산업분류 및 감성분석 수행
    4. 저장: 변경사항이 있을 경우 CSV 덮어쓰기
    """
    print(f"\n[{datetime.now()}] Gap Filler 프로세스 시작")
    
    target_csv = get_csv_path()
    print(f"[시스템] 타겟 데이터베이스: {target_csv}")

    # 1. 뉴스 크롤링 (Incremental Mode)
    # -----------------------------------------------------
    print("\n>> [Step 1] 뉴스 크롤링 (신규 기사 수집)")
    try:
        # until_date=None -> 중복된 기사가 나올 때까지 수집 (증분 수집)
        crawler = NewsCrawlerCSV(target_csv, TARGET_PRESS_DICT, until_date=None)
        crawler.run()
    except Exception as e:
        print(f"[오류] 크롤러 실행 실패: {e}")

    # 2. CSV 파일 로드 및 AI 분석
    # -----------------------------------------------------
    print("\n>> [Step 2] AI 분석 (산업분류 및 감성분석)")
    
    if not os.path.exists(target_csv):
        print(f"[오류] CSV 파일이 존재하지 않습니다: {target_csv}")
        return

    try:
        df = pd.read_csv(target_csv)
        # 필수 컬럼 보장
        if "INDUSTRY" not in df.columns: df["INDUSTRY"] = ""
        if "SENT_SCORE" not in df.columns: df["SENT_SCORE"] = ""
    except Exception as e:
        print(f"[오류] CSV 읽기 실패: {e}")
        return

    total_dirty = False
    
    # 2-1. 산업분류 수행
    df, dirty_ind = fill_missing_industry(df)
    if dirty_ind: total_dirty = True
    
    # 2-2. 감성분석 수행
    df, dirty_sent = fill_missing_sentiment(df)
    if dirty_sent: total_dirty = True
    
    # 3. 결과 저장
    # -----------------------------------------------------
    if total_dirty:
        print(f"\n>> [저장] 변경된 데이터를 저장합니다: {target_csv}")
        try:
            df.to_csv(target_csv, index=False, encoding='utf-8-sig')
            print("[성공] 저장 완료.")
        except Exception as e:
            print(f"[오류] 파일 저장 실패: {e}")
    else:
        print("\n>> [알림] 변경된 데이터가 없어 저장을 건너뜁니다.")
        
    print(f"[{datetime.now()}] Gap Filler 프로세스 종료\n")

if __name__ == "__main__":
    run_gap_filler()
