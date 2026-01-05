import time
import pandas as pd
import numpy as np
import datetime
import sys
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

# ==============================================================================
# [설정 및 환경변수 로드]
# ==============================================================================
# Current: scheduler/db_scheduler.py
# Root needed: Project1/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "web", ".env")
load_dotenv(ENV_PATH)

# 모듈 경로 추가 (scheduler 폴더)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ==============================================================================
# [핵심 모듈 임포트]
# ==============================================================================
# 1. gap_filler: 뉴스 크롤링(Step 1) -> 산업분류(Step 2) -> 감성분석(Step 3) 파이프라인
from gap_filler import run_gap_filler

# 2. update_stock_data: 주식 시장 데이터(KRX) 크롤링 및 DB 업데이트 (Step 4)
from market_crawling.update_stock_data import main as update_stock_main

# 3. calculate_risk: 리스크 지표 계산 및 예측, DB 저장 (Step 5 & 6)
from risk_measurement import calculate_risk

# 뉴스 데이터 CSV 경로
NEWS_CSV_PATH = "scheduler/db/news_db.csv"

# ==============================================================================
# [스케줄러 작업 정의]
# ==============================================================================

def job_news_pipeline():
    """
    [Interval Job: 10분 주기] 뉴스 데이터 파이프라인 실행
    
    Steps:
    1. 네이버 뉴스 크롤링 (Incremental)
    2. 산업군 분류 (KoBERT)
    3. 감성 분석 (KoElectra)
    """
    now = datetime.datetime.now()
    print(f"\n[{now}] >> [Job: News Pipeline] 뉴스 수집 및 분석 시작...")
    
    try:
        # run_gap_filler 내부에서 Crawler -> Classifier -> Analyzer 순차 실행
        run_gap_filler()
        print(f"[{datetime.datetime.now()}] << [Job: News Pipeline] 작업 완료.")
    except Exception as e:
        print(f"[오류] 뉴스 파이프라인 실행 실패: {e}")

def job_market_risk():
    """
    [Cron Job: 매일 00:00] 마켓 데이터 업데이트 및 리스크 분석
    
    Steps:
    4. 주식 시장 데이터 수집 (KRX -> STOCK Table)
    5. 리스크 인덱스 산출 (RISK Table)
    6. 주가 등락 예측 (DailyStockPredictor)
    """
    now = datetime.datetime.now()
    print(f"\n[{now}] >> [Job: Market & Risk] 일일 마감 작업 시작...")
    
    # 트랜잭션 단위가 크므로 Step별 예외처리 분리
    
    # [Step 4] 주식 데이터 업데이트
    print("\n>>> [Step 4] Stock Data Update (STOCK Table)")
    try:
        update_stock_main()
    except Exception as e:
        print(f"[오류] 주식 데이터 업데이트 실패: {e}")
        # 주식 데이터가 없으면 리스크 계산이 불가능하므로 중단 고려 가능
        # 여기서는 진행 시도
        
    # [Step 5 & 6] 리스크 계산 및 예측
    print("\n>>> [Step 5 & 6] Risk Calculation & Prediction (RISK Table)")
    try:
        # 어제 날짜 기준으로 계산 (오늘 00시 실행이므로 어제 마감 데이터 대상)
        # 예: 31일 00시에 실행 -> 30일자 데이터 분석
        yesterday_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
        calculate_risk.calculate_daily_risk(yesterday_date)
    except Exception as e:
        print(f"[오류] 리스크 분석 실패: {e}")
        
    print(f"[{datetime.datetime.now()}] << [Job: Market & Risk] 작업 완료.")

# ==============================================================================
# [메인 실행부]
# ==============================================================================
if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    print("=== [Scheduler V2] 시스템 가동 시작 ===")
    print(f"[설정] Target CSV: {NEWS_CSV_PATH}")
    print(f"[설정] DB Config: {ENV_PATH}")
    
    # 1. 뉴스 파이프라인: 10분마다 실행
    # (실제 운영 시에는 minutes=10, 테스트 시에는 더 짧게 설정 가능)
    scheduler.add_job(job_news_pipeline, 'interval', minutes=10, id='news_pipeline_10m')
    
    # 2. 마켓/리스크 분석: 매일 00:00 실행
    scheduler.add_job(job_market_risk, 'cron', hour=0, minute=0, id='market_risk_midnight')
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n[시스템] 스케줄러 종료.")
    except Exception as e:
        print(f"[오류] 스케줄러 에러: {e}")
