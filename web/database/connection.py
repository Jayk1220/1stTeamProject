import cx_Oracle
import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

DBSERVER_IP = os.getenv('DBSERVER_IP')
ORACLE_PORT = os.getenv('ORACLE_PORT', '1521')
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_SERVICE = os.getenv('ORACLE_SERVICE', 'xe')

# Oracle 연결
try:
    conn = cx_Oracle.connect(
        ORACLE_USER,
        ORACLE_PASSWORD,
        f"{DBSERVER_IP}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    )
    print("[DB] Connection established successfully")
except cx_Oracle.DatabaseError as e:
    print(f"[DB ERROR] Connection failed: {e}")
    raise
