import os
import sys
import cx_Oracle
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, field_validator

# ==============================================================================
# [설정 및 환경변수 로드]
# ==============================================================================
# Path to .env file (Project1/web/.env)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENV_PATH = os.path.join(BASE_DIR, "web", ".env")
load_dotenv(ENV_PATH)

# Oracle DB Configuration
DB_USER = os.getenv("DB_USER", "news_db")
DB_PASS = os.getenv("DB_PASS", "1234")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1521")
DB_SERVICE = os.getenv("DB_SERVICE", "xe")

# KRX API URL
API_URL = "https://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd"

# 수집 대상 지수 목록
TARGET_INDICES = ["KRX 건설", "KRX 자동차", "KRX 헬스케어"]

# ==============================================================================
# [데이터 모델 정의]
# ==============================================================================

class StockRecord(BaseModel):
    """주식 데이터 레코드 검증 모델"""
    ID: int
    SDATE: datetime
    MARKET_INDEX: str
    CLOSE: float
    VOLUME: int
    CHANGE: float

    @field_validator('SDATE', mode='before')
    def parse_sdate(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%Y-%m-%d")
        return v

# ==============================================================================
# [DB 관리 함수]
# ==============================================================================

def init_db():
    """Oracle DB 연결 초기화"""
    try:
        dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
        connection = cx_Oracle.connect(user=DB_USER, password=DB_PASS, dsn=dsn)
        return connection
    except Exception as e:
        print(f"[오류] DB 연결 실패: {e}")
        sys.exit(1)

def get_latest_db_status(conn):
    """
    DB에 저장된 가장 최근 날짜와 해당 날짜의 종가 정보를 조회합니다.
    :return: (최신 날짜 객체, {지수명: 종가})
    """
    try:
        cursor = conn.cursor()
        
        # 1. 가장 최근 날짜 조회
        cursor.execute("SELECT MAX(SDATE) FROM STOCK")
        row = cursor.fetchone()
        max_date = row[0] if row else None
        
        if not max_date:
            return None, {}
        
        print(f"[정보] DB 최신 데이터 날짜: {max_date}")
        
        # 2. 해당 날짜의 종가 조회 (전일비 계산용)
        sql = "SELECT MARKET_INDEX, CLOSE FROM STOCK WHERE SDATE = :sdate"
        cursor.execute(sql, {'sdate': max_date})
        rows = cursor.fetchall()
        
        last_close_map = {row[0]: row[1] for row in rows}
        return max_date, last_close_map
        
    except Exception as e:
        print(f"[오류] DB 상태 조회 실패: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals(): cursor.close()

def get_max_id(conn):
    """현재 STOCK 테이블의 최대 ID 값을 조회 (PK 생성용)"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(ID) FROM STOCK")
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        print(f"[오류] MAX ID 조회 실패: {e}")
        return 0
    finally:
        if 'cursor' in locals(): cursor.close()

# ==============================================================================
# [API 통신 및 데이터 처리]
# ==============================================================================

def fetch_krx_data(date_str):
    """
    KRX API를 호출하여 특정 날짜의 지수 데이터를 가져옵니다.
    :param date_str: 'YYYYMMDD' 형식
    :return: API 응답 JSON
    """
    params = {
        "basDd": date_str,
    }
    headers = {
        "AUTH_KEY": os.getenv("KRX_API_KEY", ""),
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(API_URL, params=params, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[오류] API 호출 실패 ({date_str}): {response.status_code}")
            return None
    except Exception as e:
        print(f"[오류] API 요청 중 예외 발생: {e}")
        return None

def process_and_insert(conn, date_obj, json_data, last_close_map, start_id):
    """
    API 데이터를 파싱하고 계산하여 DB에 삽입합니다.
    """
    if not json_data or "OutBlock_1" not in json_data:
        print(f"[경고] 유효한 데이터가 없습니다 ({date_obj})")
        return []

    new_records = []
    current_id = start_id
    
    raw_list = json_data["OutBlock_1"]
    
    for item in raw_list:
        idx_name = item.get("IDX_NM")
        
        # 타겟 지수만 필터링
        if idx_name in TARGET_INDICES:
            current_id += 1
            close_price = float(item.get("CLSPRC", 0))
            volume = int(item.get("ACC_TRDVOL", 0))
            
            # 전일비 계산 (전일 종가 대비)
            prev_close = last_close_map.get(idx_name)
            change = 0.0
            if prev_close:
                change = close_price - prev_close
                # 등락률로 하고 싶다면: (change / prev_close) * 100
            
            try:
                # Pydantic 검증
                record = StockRecord(
                    ID=current_id,
                    SDATE=date_obj,
                    MARKET_INDEX=idx_name,
                    CLOSE=close_price,
                    VOLUME=volume,
                    CHANGE=change
                )
                new_records.append(record)
            except ValidationError as e:
                print(f"[경고] 데이터 검증 실패 ({idx_name}): {e}")
                continue

    if not new_records:
        return []

    # DB 삽입
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO STOCK (ID, SDATE, MARKET_INDEX, CLOSE, VOLUME, CHANGE)
        VALUES (:1, :2, :3, :4, :5, :6)
    """
    try:
        data_to_insert = [
            (r.ID, r.SDATE, r.MARKET_INDEX, r.CLOSE, r.VOLUME, r.CHANGE) 
            for r in new_records
        ]
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"[성공] {date_obj.strftime('%Y-%m-%d')} - {len(new_records)}개 데이터 저장 완료.")
        return new_records
    except Exception as e:
        print(f"[오류] DB 삽입 실패: {e}")
        conn.rollback()
        return []
    finally:
        cursor.close()

# ==============================================================================
# [메인 실행 함수]
# ==============================================================================

def main():
    print("=== 주식 데이터 업데이트 시작 ===")
    conn = init_db()
    
    # 1. DB 현황 파악
    latest_date, last_close_map = get_latest_db_status(conn)
    current_max_id = get_max_id(conn)
    
    # 시작 날짜 설정 (데이터가 없으면 임의의 과거 날짜)
    if latest_date:
        start_date = latest_date + timedelta(days=1)
    else:
        start_date = datetime(2023, 1, 1) # 초기값
        
    end_date = datetime.now()
    
    print(f"[작업] 수집 구간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    current_date = start_date
    while current_date <= end_date:
        # 주말 제외 (토=5, 일=6)
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y%m%d")
            print(f">>> {date_str} 데이터 요청 중...")
            
            # API 호출
            data = fetch_krx_data(date_str)
            
            # 처리 및 저장
            inserted = process_and_insert(conn, current_date, data, last_close_map, current_max_id)
            
            if inserted:
                # 다음 날짜를 위해 전일 종가 정보 업데이트
                for rec in inserted:
                    last_close_map[rec.MARKET_INDEX] = rec.CLOSE
                current_max_id += len(inserted)
            
        current_date += timedelta(days=1)
        
    conn.close()
    print("=== 주식 데이터 업데이트 완료 ===")

if __name__ == "__main__":
    main()
