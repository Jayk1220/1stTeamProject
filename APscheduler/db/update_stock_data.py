import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import sys

# ==============================================================================
# [Configuration]
# ==============================================================================
# Path to .env file
ENV_PATH = r'c:/Users/Admin/Desktop/model_test/APScheduler_Test/market_crawling/.env'
ORACLE_URL = 'oracle+cx_oracle://news_db:1234@localhost:1521/?service_name=xe'
API_URL = "https://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd"

# Target Indices
TARGET_INDICES = ["KRX 건설", "KRX 자동차", "KRX 헬스케어"]

def init_db():
    return create_engine(ORACLE_URL)

def get_latest_db_status(engine):
    """
    Get the latest date in DB and the close prices on that date.
    Returns: (latest_date_obj, {index_name: close_price})
    """
    try:
        with engine.connect() as conn:
            # 1. Get Max Date
            result = conn.execute(text("SELECT MAX(SDATE) FROM STOCK")).fetchone()
            max_date = result[0]
            
            if not max_date:
                # Table empty fallback
                return None, {}
            
            print(f"[Info] Latest DB Date: {max_date}")
            
            # 2. Get Close Prices on Max Date
            query = text("SELECT MARKET_INDEX, CLOSE FROM STOCK WHERE SDATE = :sdate")
            rows = conn.execute(query, {'sdate': max_date}).fetchall()
            
            last_close_map = {row[0]: row[1] for row in rows}
            return max_date, last_close_map
            
    except Exception as e:
        print(f"[Error] Failed to fetch DB status: {e}")
        sys.exit(1)

def get_max_id(engine):
    """Get current maximum ID from DB."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(ID) FROM STOCK")).fetchone()
            return result[0] if result and result[0] is not None else 0
    except Exception as e:
        print(f"[Warning] Could not get Max ID: {e}")
        return 0

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

def main():
    # 1. Load Env
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)
    else:
        print(f"[Error] .env file not found at {ENV_PATH}")
        # fallback to current dir
        load_dotenv()
        
    api_key = os.getenv('KEY')
    if not api_key:
        print("[Error] API KEY not found in .env")
        return

    # 2. DB Init
    engine = init_db()
    
    # 3. Determine Date Range
    latest_date, last_close_map = get_latest_db_status(engine)
    
    if latest_date:
        start_date = latest_date + timedelta(days=1)
    else:
        # Fallback if DB empty
        start_date = datetime(2025, 9, 30)
        
    end_date = datetime.now()
    
    # Normalize time to 00:00:00 for comparison
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

    # 5. Process Data (Calculate Change)
    # Convert to DF for easier sorting/processing
    df = pd.DataFrame(new_data)
    df.sort_values(by=['MARKET_INDEX', 'SDATE'], inplace=True)
    
    # Logic: iterate and calculate change using last_close_map
    # We update last_close_map as we go (so 2nd day uses 1st day's close)
    
    records_to_insert = []
    
    # Assuming DF is sorted by Date Ascending per Index
    # But it's easier to just iterate sorted list
    
    sorted_rows = df.to_dict(orient='records')
    
    for row in sorted_rows:
        idx_name = row['MARKET_INDEX']
        curr_close = row['CLOSE']
        
        prev_close = last_close_map.get(idx_name)
        
        if prev_close is not None:
            change = curr_close - prev_close
        else:
            # First time seeing this index? Or DB was empty
            # If DB empty, change is 0 (Start)
            change = 0.0
            
        row['CHANGE'] = round(change, 2)
        
        # Update map for next iteration (next day)
        last_close_map[idx_name] = curr_close
        
        records_to_insert.append(row)

    # 6. Insert to DB
    try:
        current_max_id = get_max_id(engine)
        print(f"[System] Starting ID: {current_max_id + 1}")
        
        # Add IDs
        for i, row in enumerate(records_to_insert):
            row['ID'] = current_max_id + 1 + i
            
        with engine.begin() as conn:
            query = text('''
                INSERT INTO STOCK (ID, SDATE, MARKET_INDEX, CLOSE, VOLUME, CHANGE)
                VALUES (:ID, :SDATE, :MARKET_INDEX, :CLOSE, :VOLUME, :CHANGE)
            ''')
            conn.execute(query, records_to_insert)
            
        print(f"[Success] Inserted {len(records_to_insert)} new stock records.")
        
    except Exception as e:
        print(f"[Error] DB Insert Failed: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
