import os
import torch
import numpy as np
import pandas as pd
import cx_Oracle
from pydantic import BaseModel, ValidationError
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# ==============================================================================
# [데이터 모델 정의]
# ==============================================================================
class NewsItem(BaseModel):
    LINK: str
    TITLE: str
    CONTENT: str

# ==============================================================================
# [감성분석기 클래스 정의]
# ==============================================================================

class NewsSentimentAnalyzer:
    """
    뉴스 기사 감성 분석기 (KoElectra 기반)
    
    [주요 기능]
    1. KoElectra 모델 로드 및 GPU/CPU 자동 설정
    2. 텍스트 배치를 입력받아 긍정/부정 점수(-1.0 ~ 1.0) 산출
    3. DB에 저장된 뉴스 데이터를 조회하여 점수 업데이트 (V1 호환용, cx_Oracle 사용)
    """
    
    def __init__(self, db_config=None, model_name="jaehyeong/koelectra-base-v3-generalized-sentiment-analysis"):
        """
        초기화 함수
        :param db_config: 오라클 DB 연결 설정 딕셔너리 (user, password, dsn 등). None이면 DB 기능 사용 불가.
        :param model_name: HuggingFace 모델명 (기본값: jaehyeong/koelectra...)
        """
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.db_config = db_config
        self.model_name = model_name
        
        print(f"[감성분석] 모델 로딩: {model_name} (장치: {self.device})...")
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
            self.model.eval() # 추론 모드 설정
            print("[감성분석] 모델 로드 완료.")
        except Exception as e:
            print(f"[오류] 모델 초기화 실패: {e}")
            raise e

    def predict_batch(self, texts, batch_size=32):
        """
        [배치 추론] 다수의 텍스트 리스트에 대해 감성 점수를 반환합니다.
        
        :param texts: 분석할 텍스트 리스트
        :param batch_size: 한 번에 처리할 텍스트 수
        :return: 감성 점수 리스트 (범위: -1.0 ~ 1.0)
        """
        all_scores = []
        
        # GPU 메모리 효율을 위해 배치 단위로 처리
        for i in range(0, len(texts), batch_size):
            batch_texts = [str(t) for t in texts[i:i+batch_size]]
            
            # 토크나이징
            inputs = self.tokenizer(
                batch_texts, 
                return_tensors="pt", 
                padding=True, 
                truncation=True, 
                max_length=512
            ).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(**inputs)
                probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
                
                # 모델 출력: [부정확률, 긍정확률]
                # 점수 = 긍정 - 부정 (1.0 가까울수록 긍정, -1.0 가까울수록 부정)
                scores = (probs[:, 1] - probs[:, 0]).cpu().numpy()
                all_scores.extend(scores)
                
        return all_scores

    def analyze_db_news(self, limit=100):
        """
        [DB 전용 메서드] 
        Oracle DB에서 SENT_SCORE가 없는 기사를 조회하여 점수를 업데이트합니다.
        :param limit: 업데이트할 최대 기사 수
        :return: 업데이트된 기사 수
        """
        if not self.db_config:
            print("[감성분석] DB 설정이 없어 DB 작업을 수행할 수 없습니다.")
            return 0
            
        try:
            conn = cx_Oracle.connect(**self.db_config)
            cursor = conn.cursor()

            # 1. 대상 데이터 조회 (SENT_SCORE IS NULL)
            select_sql = """
                SELECT LINK, TITLE, CONTENT 
                FROM NEWS 
                WHERE SENT_SCORE IS NULL 
                FETCH FIRST :limit ROWS ONLY
            """
            cursor.execute(select_sql, {"limit": limit})
            rows = cursor.fetchall()
            
            if not rows:
                print("[감성분석] 분석할 대상 기사가 없습니다.")
                return 0
                
            print(f"[감성분석] {len(rows)}개 기사 분석 시작...")
            
            # 2. 데이터 준비 및 분석 (제목 + 본문 앞부분 조합)
            update_data = []
            texts_to_analyze = []
            links = []
            
            for row in rows:
                link, title, content = row
                # 제목과 본문 앞 300자를 합쳐서 분석 텍스트 생성
                text_input = f"{title} {content[:300]}"
                texts_to_analyze.append(text_input)
                links.append(link)
                
            # 배치 추론
            scores = self.predict_batch(texts_to_analyze, batch_size=16)
            
            # 3. DB 업데이트 준비
            update_sql = "UPDATE NEWS SET SENT_SCORE = :score WHERE LINK = :link"
            
            for link, score in zip(links, scores):
                # float 형변환 중요 (numpy float to python float)
                update_data.append((float(score), link))
                
            # 4. DB 반영
            cursor.executemany(update_sql, update_data)
            conn.commit()
            
            print(f"[감성분석] {len(update_data)}개 기사 감성 점수 업데이트 완료.")
            return len(update_data)

        except Exception as e:
            print(f"[오류] DB 감성 분석 실패: {e}")
            if 'conn' in locals(): conn.rollback()
            return 0
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()
