from database.connection import conn
from typing import List, Dict, Tuple
from datetime import datetime

def get_industries() -> List[str]:
    """모든 산업군 목록 조회"""
    cursor = conn.cursor()
    try:
        sql = "SELECT DISTINCT INDUSTRY FROM RISK ORDER BY INDUSTRY"
        cursor.execute(sql)
        result = cursor.fetchall()
        return [row[0] for row in result]
    finally:
        cursor.close()

def get_date_range() -> Tuple[str, str]:
    """데이터의 날짜 범위 조회"""
    cursor = conn.cursor()
    try:
        sql = """
            SELECT 
                TO_CHAR(MIN(SDATE), 'YYYY-MM-DD') AS min_date,
                TO_CHAR(MAX(SDATE), 'YYYY-MM-DD') AS max_date
            FROM STOCK
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        return result[0], result[1]
    finally:
        cursor.close()

def get_stock_data(start_date: str, end_date: str, market_index: str = 'KOSPI') -> List[Dict]:
    """STOCK 테이블에서 기간별 데이터 조회"""
    cursor = conn.cursor()
    try:
        sql = """
            SELECT 
                TO_CHAR(SDATE, 'YYYY-MM-DD') AS sdate,
                MARKET_INDEX,
                CLOSE,
                CHANGE,
                VOLUME
            FROM STOCK
            WHERE SDATE BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') 
                            AND TO_DATE(:end_date, 'YYYY-MM-DD')
                AND MARKET_INDEX = :market_index
            ORDER BY SDATE
        """
        cursor.execute(sql, {
            'start_date': start_date,
            'end_date': end_date,
            'market_index': market_index
        })
        result = cursor.fetchall()
        keys = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(keys, row)) for row in result]
    finally:
        cursor.close()

def get_risk_data(start_date: str, end_date: str, industry: str) -> List[Dict]:
    """RISK 테이블에서 산업별, 기간별 데이터 조회"""
    cursor = conn.cursor()
    try:
        sql = """
            SELECT 
                TO_CHAR(RDATE, 'YYYY-MM-DD') AS rdate,
                INDUSTRY,
                MEAN_SENT,
                RISK,
                PREDICT,
                TOTAL_NEWS,
                ARTICLE_RATIO,
                TOTAL_VOLUME,
                TRADE_VOLUME_RATIO
            FROM RISK
            WHERE RDATE BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') 
                            AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND INDUSTRY = :industry
            ORDER BY RDATE
        """
        cursor.execute(sql, {
            'start_date': start_date,
            'end_date': end_date,
            'industry': industry
        })
        result = cursor.fetchall()
        keys = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(keys, row)) for row in result]
    finally:
        cursor.close()

def get_combined_data(start_date: str, end_date: str, industry: str, market_index: str = 'KOSPI') -> Dict:
    """STOCK과 RISK 데이터를 결합하여 조회"""
    cursor = conn.cursor()
    try:
        sql = """
            SELECT 
                TO_CHAR(s.SDATE, 'YYYY-MM-DD') AS TRADE_DATE,
                s.CLOSE,
                s.CHANGE,
                s.VOLUME,
                r.MEAN_SENT,
                r.RISK,
                r.PREDICT,
                r.TOTAL_NEWS,
                r.ARTICLE_RATIO,
                r.TOTAL_VOLUME AS RISK_VOLUME,
                r.TRADE_VOLUME_RATIO
            FROM STOCK s
            LEFT JOIN RISK r ON s.SDATE = r.RDATE AND r.INDUSTRY = :industry
            WHERE s.SDATE BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') 
                            AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND s.MARKET_INDEX = :market_index
            ORDER BY s.SDATE
        """
        cursor.execute(sql, {
            'start_date': start_date,
            'end_date': end_date,
            'industry': industry,
            'market_index': market_index
        })
        result = cursor.fetchall()
        keys = [desc[0].lower() for desc in cursor.description]
        data_list = [dict(zip(keys, row)) for row in result]
        
        # 데이터 분리
        dates = [row['trade_date'] for row in data_list]
        closes = [float(row['close']) if row['close'] else 0 for row in data_list]
        article_ratios = [float(row['article_ratio']) if row['article_ratio'] else 0 for row in data_list]
        trade_volume_ratios = [float(row['trade_volume_ratio']) if row['trade_volume_ratio'] else 0 for row in data_list]
        mean_sents = [float(row['mean_sent']) if row['mean_sent'] else 0 for row in data_list]
        risk = [float(row['risk']) if row['risk'] else 0 for row in data_list]
        predicts = [float(row['predict']) if row['predict'] else 0 for row in data_list]
        
        return {
            'dates': dates,
            'closes': closes,
            'article_ratios': article_ratios,
            'trade_volume_ratios': trade_volume_ratios,
            'mean_sents': mean_sents,
            'risk': risk,
            'predicts': predicts,
            'raw_data': data_list
        }
    finally:
        cursor.close()

if __name__ == "__main__":
    # 테스트
    print("Industries:", get_industries())
    print("Date Range:", get_date_range())
