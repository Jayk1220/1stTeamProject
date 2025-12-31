import os
import sys
import cx_Oracle
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, field_validator

# ==============================================================================
# [Configuration]
# ==============================================================================
# Path to .env file
ENV_PATH = r'C:\Users\Admin\Desktop\model_test\scheduler\market_crawling\.env'

# Oracle DB Configuration (Extracted from previous SQLAlchemy URL)
DB_USER = "news_db"
DB_PASS = "1234"
DB_HOST = "localhost"
DB_PORT = 1521
DB_SERVICE = "xe"

API_URL = "https://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd"

# Target Indices
TARGET_INDICES = ["KRX 건설", "KRX 자동차", "KRX 헬스케어"]

# ==============================================================================
# [Data Models]
# ==============================================================================
class StockRecord(BaseModel):
    ID: int
    SDATE: datetime
    MARKET_INDEX: str
    CLOSE: float
    VOLUME: int
    CHANGE: float

    @field_validator('SDATE', mode='before')
    def parse_sdate(cls, v):
        if isinstance(v, str):
            # Parse string if necessary (though current logic keeps it as datetime)
            return datetime.strptime(v, "%Y-%m-%d")
        return v

# ==============================================================================
# [DB Functions]
# ==============================================================================
def init_db():
    try:
        dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
        connection = cx_Oracle.connect(user=DB_USER, password=DB_PASS, dsn=dsn)
        return connection
    except Exception as e:
        print(f"[Error] DB Connection failed: {e}")
        sys.exit(1)

def get_latest_db_status(conn):
    """
    Get the latest date in DB and the close prices on that date.
    Returns: (latest_date_obj, {index_name: close_price})
    """
    try:
        cursor = conn.cursor()
        
        # 1. Get Max Date
        cursor.execute("SELECT MAX(SDATE) FROM STOCK")
        row = cursor.fetchone()
        max_date = row[0] if row else None
        
        if not max_date:
            return None, {}
        
        print(f"[Info] Latest DB Date: {max_date}")
        
        # 2. Get Close Prices on Max Date
        sql = "SELECT MARKET_INDEX, CLOSE FROM STOCK WHERE SDATE = :sdate"
        cursor.execute(sql, {'sdate': max_date})
        rows = cursor.fetchall()
        
        last_close_map = {row[0]: row[1] for row in rows}
        return max_date, last_close_map
        
    except Exception as e:
        print(f"[Error] Failed to fetch DB status: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals(): cursor.close()

def get_max_id(conn):
    """Get current maximum ID from DB."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(ID) FROM STOCK")
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        print(f"[Warning] Could not get Max ID: {e}")
        return 0
    finally:
        if 'cursor' in locals(): cursor.close()

# ==============================================================================
# [API Functions]
# ==============================================================================
def fetch_api_data(api_key, start_date, end_date):
    """Fetch data from KRX API."""
    all_rows = []
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        
        params = {
            "AUTH_KEY": api_key,
            "basDd": date_str,
            "REQ_TYPE": "json"
        }
        
        try:
            # print(f"   -> Fetching {date_str}...")
            response = requests.get(API_URL, params=params)
            data = response.json()
            
            if "OutBlock_1" in data and data["OutBlock_1"]:
                for row in data["OutBlock_1"]:
                    if row["IDX_NM"] in TARGET_INDICES:
                        # Append raw data
                        all_rows.append({
                            "SDATE": current_date, # Keep as datetime
                            "MARKET_INDEX": row["IDX_NM"],
                            "CLOSE": float(row["CLSPRC_IDX"].replace(',', '')),
                            "VOLUME": int(row["ACC_TRDVOL"].replace(',', ''))
                        })
        except Exception as e:
            print(f"   [!] API Error on {date_str}: {e}")
            
        current_date += timedelta(days=1)
        
    return all_rows

# ==============================================================================
# [Main]
# ==============================================================================
def main():
    # 1. Load Env
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)
    else:
        print(f"[Error] .env file not found at {ENV_PATH}")
        load_dotenv() # Fallback to cwd
        
    api_key = os.getenv('KEY')
    if not api_key:
        print("[Error] API KEY not found in .env")
        return

    # 2. DB Init
    conn = init_db()
    
    try:
        # 3. Determine Date Range
        latest_date, last_close_map = get_latest_db_status(conn)
        
        if latest_date:
            start_date = latest_date + timedelta(days=1)
        else:
            # Fallback if DB empty
            start_date = datetime(2025, 9, 30)
            
        end_date = datetime.now()
        
        # Normalize time to 00:00:00
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if start_date > end_date:
            print("[Info] DB is already up-to-date.")
            return

        print(f"[System] Updating Stock Data: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

        # 4. Fetch Data
        new_data = fetch_api_data(api_key, start_date, end_date)
        
        if not new_data:
            print("[Info] No new data found from API.")
            return

        # 5. Process Data
        df = pd.DataFrame(new_data)
        df.sort_values(by=['MARKET_INDEX', 'SDATE'], inplace=True)
        
        records_to_insert = []
        sorted_rows = df.to_dict(orient='records')
        
        for row in sorted_rows:
            idx_name = row['MARKET_INDEX']
            curr_close = row['CLOSE']
            
            prev_close = last_close_map.get(idx_name)
            
            if prev_close is not None:
                change = curr_close - prev_close
            else:
                change = 0.0
                
            row['CHANGE'] = round(change, 2)
            
            # Update map for next iteration
            last_close_map[idx_name] = curr_close
            records_to_insert.append(row)

        # 6. Assign IDs and Validate with Pydantic
        current_max_id = get_max_id(conn)
        print(f"[System] Starting ID: {current_max_id + 1}")
        
        validated_data = []
        for i, row in enumerate(records_to_insert):
            row['ID'] = current_max_id + 1 + i
            try:
                # Validate
                record = StockRecord(**row)
                
                # Convert back to tuple for cx_Oracle executemany
                # params: (ID, SDATE, MARKET_INDEX, CLOSE, VOLUME, CHANGE)
                validated_data.append((
                    record.ID,
                    record.SDATE,
                    record.MARKET_INDEX,
                    record.CLOSE,
                    record.VOLUME,
                    record.CHANGE
                ))
            except ValidationError as ve:
                print(f"[Error] Validation failed for row {i}: {ve}")
                continue

        if not validated_data:
            print("[Info] No valid records to insert.")
            return

        # 7. Insert to DB
        cursor = conn.cursor()
        sql_insert = """
            INSERT INTO STOCK (ID, SDATE, MARKET_INDEX, CLOSE, VOLUME, CHANGE)
            VALUES (:1, :2, :3, :4, :5, :6)
        """
        # Note: cx_Oracle defaults to :1, :2 positional binds for executemany with list of tuples
        
        cursor.executemany(sql_insert, validated_data)
        conn.commit()
        
        print(f"[Success] Inserted {len(validated_data)} new stock records.")
        
    except Exception as e:
        print(f"[Error] Main loop failed: {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    main()
