from database.connection import conn

# 1. STOCK 테이블의 MARKET_INDEX 값 확인
print("=" * 50)
print("STOCK 테이블 MARKET_INDEX 값 확인")
print("=" * 50)
cursor = conn.cursor()
sql = """
    SELECT DISTINCT 
        MARKET_INDEX,
        LENGTH(MARKET_INDEX) AS 길이,
        DUMP(MARKET_INDEX) AS 바이트정보
    FROM STOCK
    ORDER BY MARKET_INDEX
"""
cursor.execute(sql)
for row in cursor.fetchall():
    print(f"값: '{row[0]}' | 길이: {row[1]} | 바이트: {row[2]}")
cursor.close()

# 2. RISK 테이블의 INDUSTRY 값 확인
print("\n" + "=" * 50)
print("RISK 테이블 INDUSTRY 값 확인")
print("=" * 50)
cursor = conn.cursor()
sql = """
    SELECT DISTINCT 
        INDUSTRY,
        LENGTH(INDUSTRY) AS 길이,
        DUMP(INDUSTRY) AS 바이트정보
    FROM RISK
    ORDER BY INDUSTRY
"""
cursor.execute(sql)
for row in cursor.fetchall():
    print(f"값: '{row[0]}' | 길이: {row[1]} | 바이트: {row[2]}")
cursor.close()

# 3. 날짜 범위 확인
print("\n" + "=" * 50)
print("날짜 범위 확인")
print("=" * 50)
cursor = conn.cursor()
sql = """
    SELECT 
        'STOCK' AS 테이블,
        TO_CHAR(MIN(SDATE), 'YYYY-MM-DD') AS 시작일,
        TO_CHAR(MAX(SDATE), 'YYYY-MM-DD') AS 종료일,
        COUNT(*) AS 행수
    FROM STOCK
    UNION ALL
    SELECT 
        'RISK' AS 테이블,
        TO_CHAR(MIN(RDATE), 'YYYY-MM-DD') AS 시작일,
        TO_CHAR(MAX(RDATE), 'YYYY-MM-DD') AS 종료일,
        COUNT(*) AS 행수
    FROM RISK
"""
cursor.execute(sql)
for row in cursor.fetchall():
    print(f"{row[0]} | {row[1]} ~ {row[2]} | 총 {row[3]}행")
cursor.close()

# 4. 샘플 데이터로 JOIN 테스트
print("\n" + "=" * 50)
print("JOIN 테스트 (2025-09-30 ~ 2025-10-02, 건설)")
print("=" * 50)
cursor = conn.cursor()
sql = """
    SELECT 
        TO_CHAR(s.SDATE, 'YYYY-MM-DD') AS 날짜,
        s.MARKET_INDEX AS STOCK_산업,
        r.INDUSTRY AS RISK_산업,
        s.CLOSE,
        r.RISK
    FROM STOCK s
    LEFT JOIN RISK r
        ON TO_CHAR(s.SDATE, 'YYYY-MM-DD') = TO_CHAR(r.RDATE, 'YYYY-MM-DD')
        AND TRIM(r.INDUSTRY) = TRIM(s.MARKET_INDEX)
    WHERE s.SDATE BETWEEN TO_DATE('2025-09-30', 'YYYY-MM-DD') 
                      AND TO_DATE('2025-10-02', 'YYYY-MM-DD')
    AND s.MARKET_INDEX LIKE '%건설%'
    ORDER BY s.SDATE
"""
cursor.execute(sql)
result = cursor.fetchall()
if result:
    for row in result:
        print(f"{row[0]} | STOCK: '{row[1]}' | RISK: '{row[2]}' | CLOSE: {row[3]} | RISK: {row[4]}")
else:
    print("데이터 없음!")
cursor.close()

print("\n완료!")
