import time
import pandas as pd
import numpy as np
import datetime
import sys
import os
import cx_Oracle
from pydantic import BaseModel, ValidationError, field_validator
from apscheduler.schedulers.blocking import BlockingScheduler

# 모듈 임포트
# gap_filler: 뉴스 크롤링부터 산업분류, 감성분석까지 빈 데이터를 채우는 핵심 로직
from gap_filler import run_gap_filler
# NewsCrawlerCSV: CSV 기반 뉴스 크롤러 
from news_crawling.Nnews_Crawler_CSV import NewsCrawlerCSV
# update_stock_main: 주식 시장 데이터(KRX) 크롤링 및 DB 업데이트
from market_crawling.update_stock_data import main as update_stock_main

# ==============================================================================
# [설정 및 상수 정의]
# ==============================================================================
# Oracle DB Configuration
DB_USER = "news_db"
DB_PASS = "1234"
DB_HOST = "localhost"
DB_PORT = 1521
DB_SERVICE = "xe"

# 뉴스 데이터가 저장될 CSV 파일 경로
NEWS_CSV_PATH = "scheduler/db/news_db.csv"

# ==============================================================================
# [Data Models]
# ==============================================================================
class RiskRecord(BaseModel):
    RDATE: datetime.datetime
    INDUSTRY: str
    MEAN_SENT: float
    RISK_INDEX: float
    PREDICT: float | None = None

    @field_validator('RDATE', mode='before')
    def parse_rdate(cls, v):
        if isinstance(v, str):
            return datetime.datetime.strptime(v, "%Y-%m-%d")
        return v

# ==============================================================================
# [DB Helper Functions]
# ==============================================================================
def get_db_connection():
    try:
        dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
        connection = cx_Oracle.connect(user=DB_USER, password=DB_PASS, dsn=dsn)
        return connection
    except Exception as e:
        print(f"[오류] DB 연결 실패: {e}")
        raise e

# ==============================================================================
# [리스크 계산 로직]
# ==============================================================================
def calculate_risk_hybrid(target_date):
    """
    [핵심 기능] 뉴스 데이터(CSV)와 주식 데이터(Oracle)를 결합하여 리스크 지수를 계산하고 DB에 저장합니다.
    """
    print(f"[Risk] Calculating for {target_date.strftime('%Y-%m-%d')}...")
    
    # 1. 뉴스 데이터 로드 (CSV)
    # --------------------------------------------------------------------------
    if not os.path.exists(NEWS_CSV_PATH):
        # 경로 예외 처리
        if os.path.exists("db/news_db.csv"):
            path = "db/news_db.csv"
        else:
            print("[오류] CSV 파일을 찾을 수 없습니다.")
            return
    else:
        path = NEWS_CSV_PATH
        
    try:
        # CSV 컬럼명: 날짜, 제목, industry, sent_score
        use_cols = ["날짜", "제목", "industry", "sent_score"]
        df = pd.read_csv(path, usecols=use_cols)
        
        # 컬럼명 통일 (대문자로)
        df.rename(columns={"industry": "INDUSTRY", "sent_score": "SENT_SCORE"}, inplace=True)
    except Exception as e:
        print(f"[오류] CSV 읽기 오류: {e}")
        return
    
    # 날짜 필터링
    try:
        df['dt'] = pd.to_datetime(df['날짜'])
    except:
        print("[오류] 날짜 형식 변환 실패.")
        return
        
    target_day_str = target_date.strftime("%Y-%m-%d")
    daily_news = df[df['dt'].dt.strftime('%Y-%m-%d') == target_day_str].copy()
    
    if daily_news.empty:
        print(f"[오류] {target_day_str} 일자의 뉴스 데이터가 없습니다.")
        return

    # 산업별 뉴스 통계 집계
    daily_news['SENT_SCORE'] = pd.to_numeric(daily_news['SENT_SCORE'], errors='coerce')
    
    news_stats = daily_news.groupby('INDUSTRY').agg(
        NEWS_COUNT=('제목', 'count'),
        AVE_SENT=('SENT_SCORE', 'mean')
    ).reset_index()
    
    total_news = news_stats['NEWS_COUNT'].sum()
    
    # 2. 주식 데이터 조회 (Oracle via cx_Oracle)
    # --------------------------------------------------------------------------
    conn = None
    stock_df = pd.DataFrame()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        stock_query = """
            SELECT MARKET_INDEX, VOLUME
            FROM STOCK
            WHERE TRUNC(SDATE) = TO_DATE(:tdate, 'YYYY-MM-DD')
        """
        cursor.execute(stock_query, {'tdate': target_day_str})
        rows = cursor.fetchall()
        
        if rows:
            # cx_Oracle returns tuples, need column names
            col_names = [d[0] for d in cursor.description]
            stock_df = pd.DataFrame(rows, columns=col_names)
        
    except Exception as e:
        print(f"[오류] 주식 데이터 조회 실패: {e}")
        return
    finally:
        if conn: conn.close()
        
    if stock_df.empty:
        print(f"[오류] {target_day_str} 일자의 주식 데이터가 없습니다.")
        return

    # 산업명 매핑
    stock_df['INDUSTRY'] = stock_df['MARKET_INDEX'].str.replace('KRX ', '', regex=False).str.strip()
    total_vol = stock_df['VOLUME'].sum()
    
    # 3. 데이터 병합 및 계산
    # --------------------------------------------------------------------------
    news_stats.columns = [c.upper() for c in news_stats.columns]
    stock_df.columns = [c.upper() for c in stock_df.columns]
    
    merged = pd.merge(news_stats, stock_df, on='INDUSTRY', how='inner')
    
    if merged.empty:
        print("[오류] 매칭되는 산업군이 없습니다.")
        return
        
    merged['AVE_SENT'] = merged['AVE_SENT'].fillna(0)
    
    # [리스크 계산 공식]
    # 사용자 요청: 아직 모델 이식 전이므로 MEAN_SENT만 저장하고 RISK_INDEX는 0으로 처리
    # merged['RISK_INDEX'] = (
    #     merged['AVE_SENT'] * 
    #     np.log1p(merged['NEWS_COUNT'] / total_news) * 
    #     np.log1p(merged['VOLUME'] / total_vol)
    # )
    merged['RISK_INDEX'] = 0.0
    
    # 4. 결과 DB 저장
    # --------------------------------------------------------------------------
    records_to_insert = []
    for _, row in merged.iterrows():
        try:
            record = RiskRecord(
                RDATE=target_date,
                INDUSTRY=row['INDUSTRY'],
                MEAN_SENT=float(row['AVE_SENT']),
                RISK_INDEX=float(row['RISK_INDEX']),
                PREDICT=None
            )
            records_to_insert.append((
                record.RDATE,
                record.INDUSTRY,
                record.MEAN_SENT,
                record.RISK_INDEX,
                record.PREDICT
            ))
        except ValidationError as ve:
            print(f"[경고] 리스크 데이터 검증 실패: {ve}")
            continue

    if records_to_insert:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 먼저 해당 날짜 데이터 삭제 (중복 방지)
            delete_sql = "DELETE FROM RISK WHERE TRUNC(RDATE) = TO_DATE(:1, 'YYYY-MM-DD')"
            cursor.execute(delete_sql, [target_day_str])
            
            # 삽입
            insert_sql = """
                INSERT INTO RISK (RDATE, INDUSTRY, MEAN_SENT, RISK_INDEX, PREDICT)
                VALUES (:1, :2, :3, :4, :5)
            """
            cursor.executemany(insert_sql, records_to_insert)
            conn.commit()
            
            print(f"[Risk] 성공적으로 {len(records_to_insert)}건의 리스크 데이터를 Oracle에 저장했습니다.")
            
        except Exception as e:
            print(f"[오류] DB 저장 오류: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

# ==============================================================================
# [스케줄러 작업 정의]
# ==============================================================================

def job_news_pipeline():
    """
    [10분 단위 작업] 뉴스 크롤링 -> 산업분류 -> 감성분석
    """
    print(f"\n[{datetime.datetime.now()}] >> [Interval Job] 뉴스 파이프라인 시작 (Gap Filler)...")
    try:
        run_gap_filler()
    except Exception as e:
        print(f"[오류] 뉴스 파이프라인 실행 중 예외 발생: {e}")
    print(f"[{datetime.datetime.now()}] << [Interval Job] 완료.\n")

def job_market_risk():
    """
    [일일 작업: 00:00] 주식 데이터 업데이트 및 리스크 산출
    """
    print(f"\n[{datetime.datetime.now()}] >> [Daily Job] 마켓 및 리스크 분석 시작...")
    
    # 1. 주식 데이터 업데이트
    print("[Step 1] Stock Data Updating...")
    try:
        from market_crawling.update_stock_data import main as stock_main
        stock_main()
    except Exception as e:
        print(f"[오류] Stock Update 실패: {e}")
        
    # 2. 리스크 계산 (전일 기준)
    print("[Step 2] Risk Calculation...")
    try:
        yesterday_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
        # Risk 분석 부분 활성화 (RDATE, INDUSTRY, MEAN_SENT 저장)
        calculate_risk_hybrid(yesterday_date)
    except Exception as e:
        print(f"[오류] Risk Calc 실패: {e}")
        
    print(f"[{datetime.datetime.now()}] << [Daily Job] 완료.\n")

# ==============================================================================
# [메인 실행부]
# ==============================================================================
if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    # 1. 뉴스 파이프라인: 10분마다 실행
    scheduler.add_job(job_news_pipeline, 'interval', minutes=10, id='news_pipeline_10m')
    
    # 2. 마켓/리스크 분석: 매일 00:00 실행
    scheduler.add_job(job_market_risk, 'cron', hour=0, minute=0, id='market_risk_midnight')
    
    print("=== Scheduler V2 (Split Schedule) Started ===")
    print(f"Target CSV: {NEWS_CSV_PATH}")
    print(f"Algorithm Hybrid Mode: CSV(News) + Oracle(Stock/Risk)")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("스케줄러가 종료되었습니다.")
