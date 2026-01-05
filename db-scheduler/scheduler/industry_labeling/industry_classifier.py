import os
import torch
import numpy as np
import pandas as pd
import cx_Oracle
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sklearn.preprocessing import LabelEncoder

# ==============================================================================
# [산업 분류기 클래스 정의]
# ==============================================================================

class IndustryClassifier:
    """
    뉴스 산업 분류기 (KoBERT 기반)
    
    [주요 기능]
    1. 저장된 KoBERT 모델(safetensors) 및 LabelEncoder 로드
    2. 입력 텍스트(기사 본문/제목)를 분석하여 가장 확률이 높은 산업군으로 분류
    3. DB(Oracle)에 저장된 산업 미분류 뉴스를 조회하여 라벨링 후 업데이트
    """
    
    def __init__(self, model_dir=None, db_config=None):
        """
        초기화 함수
        :param model_dir: 모델 파일(pytorch_model.bin 등)이 위치한 디렉토리
        :param db_config: Oracle DB 연결 설정 {user, password, dsn} (None이면 DB 기능 미사용)
        """
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.db_config = db_config
        
        # 모델 경로 설정
        if model_dir is None:
            base_path = os.path.dirname(os.path.abspath(__file__))
            model_dir = os.path.join(base_path, "my_kobert_model")
            self.classes_path = os.path.join(base_path, "classes.npy")
        else:
            # model_dir이 주어지면 그 상위 폴더 등에 classes.npy가 있다고 가정
            self.classes_path = os.path.join(model_dir, "..", "classes.npy") 
            # 만약 classes.npy가 model_dir 내부에 있다면 경로 수정 필요

        print(f"[산업분류] 모델 로딩 중: {model_dir} (Device: {self.device})...")
        try:
            # Safetensors 포맷 지원
            self.tokenizer = AutoTokenizer.from_pretrained(model_dir, use_safetensors=True)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_dir, use_safetensors=True).to(self.device)
            self.model.eval()
            
            # 라벨 인코더 로드 (Numeric -> String Class Name)
            self.le = LabelEncoder()
            if os.path.exists(self.classes_path):
                self.le.classes_ = np.load(self.classes_path, allow_pickle=True)
            else:
                print(f"[오류] classes.npy 파일을 찾을 수 없습니다: {self.classes_path}")
                # 파일이 없으면 정상 작동 불가하므로 예외 처리 필요하나, 
                # 여기서는 경고만 하고 넘어가거나 더미로 초기화 할 수도 있음.
                pass 
                
            print("[산업분류] 모델 및 리소스 로드 완료.")
        except Exception as e:
            print(f"[오류] 모델 초기화 실패: {e}")
            raise e

    def predict_batch(self, texts, batch_size=32, threshold=0.7):
        """
        [배치 분류] 텍스트 리스트에 대해 산업군을 예측
        :param texts: 분류할 텍스트 리스트
        :param batch_size: 배치 크기
        :param threshold: 확신도 임계값 (미만이면 '분류 불가')
        :return: [(Label, Probability), ...]
        """
        all_results = []
        clean_texts = [str(t) if t else "" for t in texts] # None 처리

        with torch.inference_mode():
            for i in range(0, len(clean_texts), batch_size):
                batch = clean_texts[i:i+batch_size]
                
                # Tokenize
                inputs = self.tokenizer(
                    batch, 
                    return_tensors="pt", 
                    truncation=True, 
                    padding=True, 
                    max_length=256
                ).to(self.device)
                
                outputs = self.model(**inputs)
                probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
                
                # Max Probability
                max_probs, idxs = torch.max(probs, dim=1)
                
                for prob, idx in zip(max_probs.cpu().numpy(), idxs.cpu().numpy()):
                    try:
                        label_name = self.le.inverse_transform([idx])[0]
                    except:
                        label_name = "Unknown"
                        
                    final_label = label_name if prob >= threshold else "분류 불가"
                    all_results.append((final_label, float(prob)))
        
        return all_results

    def label_db_news(self, limit=100):
        """
        [DB 전용 메서드]
        Oracle DB에서 INDUSTRY가 비어있는 뉴스를 조회하여 분류 수행 후 업데이트
        :param limit: 한 번에 처리할 최대 기사 수
        :return: 업데이트된 기사 수
        """
        if not self.db_config:
            print("[산업분류] DB 설정이 없어 DB 작업을 수행할 수 없습니다.")
            return 0
            
        try:
            conn = cx_Oracle.connect(**self.db_config)
            cursor = conn.cursor()

            # 1. 미분류 기사 조회 (INDUSTRY IS NULL OR INDUSTRY = '')
            # OID+LINK가 PK라고 가정하거나, LINK를 유니크 키로 사용
            select_sql = """
                SELECT LINK, TITLE 
                FROM NEWS 
                WHERE INDUSTRY IS NULL OR INDUSTRY = '분류 불가' OR INDUSTRY = ''
                FETCH FIRST :limit ROWS ONLY
            """
            cursor.execute(select_sql, {"limit": limit})
            rows = cursor.fetchall()
            
            if not rows:
                print("[산업분류] 미분류 기사가 없습니다.")
                return 0
            
            links = [r[0] for r in rows]
            titles = [r[1] for r in rows]
            
            print(f"[산업분류] {len(rows)}개 기사 분류 시작...")
            
            # 2. 예측 수행 (제목 기반 분류가 일반적, 필요 시 본문 사용)
            results = self.predict_batch(titles, batch_size=32)
            
            # 3. DB 업데이트
            update_sql = "UPDATE NEWS SET INDUSTRY = :industry WHERE LINK = :link"
            update_data = []
            
            for link, (label, prob) in zip(links, results):
                if label != "분류 불가":
                    update_data.append((label, link))
            
            if update_data:
                cursor.executemany(update_sql, update_data)
                conn.commit()
                print(f"[산업분류] {len(update_data)}개 기사 라벨링 업데이트 완료.")
                return len(update_data)
            else:
                print("[산업분류] 유효한 분류 결과가 없어 업데이트하지 않았습니다.")
                return 0

        except Exception as e:
            print(f"[오류] DB 라벨링 작업 실패: {e}")
            if 'conn' in locals(): conn.rollback()
            return 0
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()
