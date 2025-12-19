import pandas as pd
from datetime import datetime
import os
import sys

# ====================================================
# [설정] 변환할 파일명 입력 (경로 없이 이름만 적으세요!)
# ====================================================
TARGET_FILE_NAME = "oid=009&date=20251218.csv"


# ====================================================
# [핵심] 절대 경로 자동 생성 로직
# ====================================================
# 현재 파이썬 파일이 있는 폴더 경로를 자동으로 알아냅니다.
script_dir = os.path.dirname(os.path.abspath(__file__))

# 파이썬 파일과 같은 폴더에 있는 CSV 파일을 가리키는 '절대 경로'를 만듭니다.
# 이렇게 하면 터미널 위치(c:/ai/)가 어디든 상관없이 무조건 파일을 찾습니다.
input_file_path = os.path.join(script_dir, TARGET_FILE_NAME)


# ====================================================
# [함수] 날짜 변환 로직 (네이버 뉴스 포맷 -> 표준 포맷)
# ====================================================
def clean_date(date_str):
    try:
        date_str = str(date_str).replace("기사입력", "").replace("입력", "").strip()
        is_pm = "오후" in date_str
        date_str = date_str.replace("오전", "").replace("오후", "").strip()
        
        # 포맷: YYYY.MM.DD. H:MM
        dt = datetime.strptime(date_str, "%Y.%m.%d. %H:%M")
        
        if is_pm and dt.hour != 12:
            dt = dt.replace(hour=dt.hour + 12)
        elif not is_pm and dt.hour == 12:
            dt = dt.replace(hour=0)
            
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # 변환 실패 시 원본 유지
        return date_str

# ====================================================
# [실행] 메인 처리 로직
# ====================================================
def process_csv():
    print(f"\n🚀 데이터 변환 작업을 시작합니다.")
    print(f"📂 대상 파일 경로: {input_file_path}")

    # 1. 파일 존재 여부 확인 (가장 중요한 단계)
    if not os.path.exists(input_file_path):
        print(f"\n❌ [오류] 파일을 찾을 수 없습니다!")
        print(f"   -> 현재 파이썬 파일 위치: {script_dir}")
        print(f"   -> 찾으려는 파일: {TARGET_FILE_NAME}")
        print("   👉 팁: 파이썬 파일과 CSV 파일이 같은 폴더에 있는지 꼭 확인해주세요.")
        return

    # 2. 파일 읽기 (인코딩 자동 감지)
    try:
        df = pd.read_csv(input_file_path, encoding='utf-8')
    except UnicodeDecodeError:
        print("   -> UTF-8 인코딩 실패, CP949로 재시도합니다.")
        df = pd.read_csv(input_file_path, encoding='cp949')

    # 3. 날짜 변환
    if '날짜' in df.columns:
        df['날짜'] = df['날짜'].apply(clean_date)
    else:
        print("⚠️ 경고: '날짜' 컬럼이 없어 변환을 건너뜁니다.")

    # 4. 컬럼 순서 재배치 (날짜 -> 제목 -> 본문 -> 링크)
    target_order = ['날짜', '제목', '본문', '링크']
    
    # 있는 컬럼만 추려서 순서 맞추기
    final_cols = [c for c in target_order if c in df.columns] + \
                 [c for c in df.columns if c not in target_order]
    df = df[final_cols]

    # 5. 저장 (파일명_p.csv)
    # 확장자(.csv)를 떼어내고 _p를 붙인 뒤 다시 .csv를 붙임
    file_root, file_ext = os.path.splitext(TARGET_FILE_NAME)
    output_name = f"{file_root}_p{file_ext}"
    output_path = os.path.join(script_dir, output_name)

    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"\n✅ 변환 완료!")
    print(f"💾 저장된 파일: {output_name}")
    print(f"📊 저장 경로: {output_path}")

if __name__ == "__main__":
    process_csv()