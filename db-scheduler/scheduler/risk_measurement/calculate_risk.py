import os
import pandas as pd
import numpy as np
import datetime
import cx_Oracle
from datetime import timedelta
import sys
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError

# ==============================================================================
# [설정 및 환경변수 로드]
# ==============================================================================
# Path to .env file (Project1/web/.env)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Project1/
sys.path.append(os.path.join(BASE_DIR, "scheduler")) # Add scheduler to path (for predictor import)

ENV_PATH = os.path.join(BASE_DIR, "web", ".env")
load_dotenv(ENV_PATH)

# Predictor 모듈 임포트
from prediction.predictor import DailyStockPredictor

# Oracle DB Configuration
DB_USER = os.getenv("DB_USER", "news_db")
DB_PASS = os.getenv("DB_PASS", "1234")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1521")
DB_SERVICE = os.getenv("DB_SERVICE", "xe")

# ==============================================================================
# [데이터 모델 정의]
# ==============================================================================

class RiskRecord(BaseModel):
    """RISK 테이블 삽입 데이터 검증 모델"""
    ID: int
    RDATE: datetime.date
    INDUSTRY: str
    MEAN_SENT: float
    RISK_INDEX: float
    PREDICT: float

# ==============================================================================
# [핵심 로직]
# ==============================================================================

def init_db():
    """Oracle DB 연결 초기화"""
    try:
        dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
        conn = cx_Oracle.connect(user=DB_USER, password=DB_PASS, dsn=dsn)
        return conn
    except Exception as e:
        print(f"[오류] DB 연결 실패: {e}")
        raise e

def ensure_risk_table(conn):
    """RISK 테이블 존재 여부 확인 및 생성"""
    create_sql = """
        CREATE TABLE RISK (
            ID NUMBER,
            RDATE DATE,
            INDUSTRY VARCHAR2(100),
            MEAN_SENT NUMBER,
            RISK_INDEX NUMBER,
            PREDICT NUMBER,
            CONSTRAINT pk_risk PRIMARY KEY (ID)
        )
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM user_tables WHERE table_name = 'RISK'")
        check = cursor.fetchone()[0]
        if check == 0:
            print("[Risk] RISK 테이블 생성 중...")
            cursor.execute(create_sql)
        cursor.close()
    except Exception as e:
        print(f"[오류] 테이블 확인/생성 실패: {e}")

def calculate_daily_risk(target_date):
    """
    [일일 리스크 계산 및 주가 등락 예측]
    1. 해당 날짜(target_date)의 뉴스 CSV 데이터 로드
    2. 산업군별 감성지수(Mean Sentiment) 및 뉴스량 계산
    3. Oracle DB(STOCK)에서 해당 날짜의 시장 데이터(거래량, 종가 등) 로드
    4. 리스크 지수(Risk Index) 산출: 
       Risk = Sentiment * ln(1 + IndustryNews/TotalNews) * ln(1 + IndustryVol/TotalVol)
    5. AI 모델을 통한 주가 등락 예측 (Predictor)
    6. 결과 DB(RISK) 저장
    """
    conn = init_db()
    ensure_risk_table(conn)
    cursor = conn.cursor()
    
    # 예측 모델 초기화
    predictor = DailyStockPredictor()
    
    date_str = target_date.strftime("%Y-%m-%d")
    print(f"\n[Risk] {date_str} 리스크 분석 시작...")
    
    try:
        # ----------------------------------------------------------------------
        # 1. 뉴스 데이터 로드 (CSV)
        # ----------------------------------------------------------------------
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
        csv_path = os.path.join(base_dir, "db", "news_db.csv")
        
        if not os.path.exists(csv_path):
            print(f"[오류] 뉴스 데이터 파일 없음: {csv_path}")
            return

        df = pd.read_csv(csv_path)
        
        # 날짜 필터링 (Format: YYYY-MM-DD)
        # CSV의 'NDATE' 포맷에 따라 처리 (YYYY-MM-DD HH:MM:SS 가정 시 substring 사용)
        df['NDATE_STR'] = df['NDATE'].astype(str).str.slice(0, 10)
        daily_df = df[df['NDATE_STR'] == date_str]
        
        if daily_df.empty:
            print(f"[알림] {date_str} 해당 날짜의 뉴스가 없습니다.")
            return

        # ----------------------------------------------------------------------
        # 2. 뉴스 통계 계산
        # ----------------------------------------------------------------------
        total_news_count = len(daily_df)
        
        industry_stats = daily_df.groupby('INDUSTRY').agg({
            'SENT_SCORE': 'mean',
            'TITLE': 'count' # 뉴스 개수
        }).rename(columns={'SENT_SCORE': 'mean_sent', 'TITLE': 'news_count'})
        
        # ----------------------------------------------------------------------
        # 3. 주식 데이터 로드 (DB)
        # ----------------------------------------------------------------------
        stock_query = """
            SELECT MARKET_INDEX, VOLUME, CLOSE, CHANGE
            FROM STOCK
            WHERE SDATE = TO_DATE(:sdate, 'YYYY-MM-DD')
        """
        cursor.execute(stock_query, {'sdate': date_str})
        stock_rows = cursor.fetchall()
        
        if not stock_rows:
            print(f"[알림] {date_str} 주식 데이터가 없어 리스크 계산을 중단합니다.")
            return

        # 데이터프레임 변환
        stock_df = pd.DataFrame(stock_rows, columns=['MARKET_INDEX', 'VOLUME', 'CLOSE', 'CHANGE'])
        
        # 인덱스 이름 매핑 (뉴스 산업분류 -> 주식 인덱스명)
        # 예: "건설" -> "KRX 건설"
        market_map = {
            "건설": "KRX 건설",
            "자동차": "KRX 자동차",
            "헬스케어": "KRX 헬스케어"
        }
        
        total_volume = stock_df['VOLUME'].sum()
        
        # ----------------------------------------------------------------------
        # 4. 리스크 계산 및 예측
        # ----------------------------------------------------------------------
        # RISK 테이블 PK (ID) 생성용
        cursor.execute("SELECT MAX(ID) FROM RISK")
        max_id_row = cursor.fetchone()
        next_id = (max_id_row[0] if max_id_row and max_id_row[0] else 0) + 1
        
        rows_to_insert = []
        
        for industry, row in industry_stats.iterrows():
            if industry not in market_map:
                continue
                
            market_idx = market_map[industry]
            stock_data = stock_df[stock_df['MARKET_INDEX'] == market_idx]
            
            if stock_data.empty:
                continue
                
            # 변수 추출
            mean_sent = row['mean_sent']
            news_count = row['news_count']
            
            close = stock_data.iloc[0]['CLOSE']
            volume = stock_data.iloc[0]['VOLUME']
            change = stock_data.iloc[0]['CHANGE']
            
            # 비율 계산 (Avoid Divide by Zero)
            news_ratio = news_count / total_news_count if total_news_count > 0 else 0
            vol_ratio = volume / total_volume if total_volume > 0 else 0
            
            # Risk Index Formula
            # Risk = Mean_Sent * ln(1 + News_Ratio) * ln(1 + Vol_Ratio) * 1000 (Scaling)
            risk_index = mean_sent * np.log1p(news_ratio) * np.log1p(vol_ratio) * 1000
            
            # ------------------------------------------------------------------
            # 5. AI 예측 수행
            # ------------------------------------------------------------------
            pred_input = {
                'ave_sent': mean_sent,
                'news_count': news_count,
                'close': close,
                'volume': volume,
                'change': change,
                'total_news': total_news_count,
                'total_vol': total_volume,
                'risk_index': risk_index,
                'article_ratio': news_ratio,
                'volume_ratio': vol_ratio,
                'INDUSTRY': industry # Optional depending on predictor logic
            }
            
            prediction = predictor.predict(pred_input)
            if prediction is None:
                prediction = -1 # 예측 실패 시 -1
                
            # 데이터 검증 (Pydantic)
            try:
                record = RiskRecord(
                    ID=next_id,
                    RDATE=target_date.date(),
                    INDUSTRY=industry,
                    MEAN_SENT=float(mean_sent),
                    RISK_INDEX=float(risk_index),
                    PREDICT=float(prediction)
                )
                rows_to_insert.append(record)
                next_id += 1
            except ValidationError as e:
                print(f"[경고] 데이터 검증 실패 ({industry}): {e}")

        # ----------------------------------------------------------------------
        # 6. DB 저장 (기존 데이터 삭제 후 삽입 - 멱등성 보장)
        # ----------------------------------------------------------------------
        if rows_to_insert:
            # 해당 날짜 데이터 삭제
            del_sql = "DELETE FROM RISK WHERE RDATE = TO_DATE(:rdate, 'YYYY-MM-DD')"
            cursor.execute(del_sql, {'rdate': date_str})
            
            # 새 데이터 삽입
            ins_sql = """
                INSERT INTO RISK (ID, RDATE, INDUSTRY, MEAN_SENT, RISK_INDEX, PREDICT)
                VALUES (:1, :2, :3, :4, :5, :6)
            """
            data_list = [
                (r.ID, r.RDATE, r.INDUSTRY, r.MEAN_SENT, r.RISK_INDEX, r.PREDICT) 
                for r in rows_to_insert
            ]
            cursor.executemany(ins_sql, data_list)
            conn.commit()
            print(f"[성공] {len(rows_to_insert)}건의 리스크/예측 데이터 저장 완료.")
        else:
            print("[알림] 저장할 리스크 데이터가 없습니다.")

    except Exception as e:
        print(f"[오류] 리스크 계산 실패: {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        
if __name__ == "__main__":
    # Test Run
    target = datetime.datetime(2025, 12, 30)
    calculate_daily_risk(target)
