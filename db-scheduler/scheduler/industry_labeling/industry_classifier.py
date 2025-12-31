import os
import torch
import numpy as np
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sklearn.preprocessing import LabelEncoder


# ==============================================================================
# [산업 분류기 클래스 정의]
# ==============================================================================

class IndustryClassifier:
    """
    뉴스 산업 분류기 (KoBERT 기반)
    
    주요 기능:
    1. 저장된 KoBERT 모델 및 LabelEncoder 로드
    2. 입력 텍스트를 분석하여 가장 확률이 높은 산업군으로 분류
    3. DB에 저장된 미분류 뉴스에 대해 산업 라벨링 수행 (V1 호환)
    """
    
    def __init__(self, model_dir=None):
        """
        초기화 함수
        :param model_dir: 모델 파일이 위치한 디렉토리 (None이면 기본값 사용)
        """
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        # 모델 경로 설정 (현재 파일 위치 기준 상대 경로)
        if model_dir is None:
            base_path = os.path.dirname(os.path.abspath(__file__))
            model_dir = os.path.join(base_path, "my_kobert_model")
            self.classes_path = os.path.join(base_path, "classes.npy")
        else:
            self.classes_path = os.path.join(model_dir, "..", "classes.npy") 

        print(f"[산업분류] 모델 로딩 중: {model_dir}...")
        try:
            # Safetensors 포맷 지원을 위해 use_safetensors=True 설정
            self.tokenizer = AutoTokenizer.from_pretrained(model_dir, use_safetensors=True)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_dir, use_safetensors=True).to(self.device)
            self.model.eval()
            
            # 라벨 인코더 로드 (숫자 -> 산업명 변환용)
            self.le = LabelEncoder()
            if os.path.exists(self.classes_path):
                self.le.classes_ = np.load(self.classes_path, allow_pickle=True)
            else:
                print(f"[오류] classes.npy 파일을 찾을 수 없습니다: {self.classes_path}")
                raise FileNotFoundError("classes.npy missing")
                
            print("[산업분류] 모델 로드 완료.")
        except Exception as e:
            print(f"[오류] 모델 초기화 실패: {e}")
            raise e

    def predict_batch(self, texts, batch_size=32, threshold=0.7):
        """
        [배치 분류] 텍스트 리스트에 대해 산업군을 예측합니다.
        
        :param texts: 분류할 텍스트 리스트
        :param batch_size: 배치 크기
        :param threshold: 확신도 임계값 (이보다 낮으면 '분류 불가' 처리)
        :return: 튜플 리스트 [(라벨, 확률), ...]
        """
        all_results = []
        
        # 빈 텍스트 필터링 및 변환
        clean_texts = [str(t) if t else "" for t in texts]

        with torch.inference_mode():
            for i in range(0, len(clean_texts), batch_size):
                batch = clean_texts[i:i+batch_size]
                
                # 토크나이징
                inputs = self.tokenizer(
                    batch, 
                    return_tensors="pt", 
                    truncation=True, 
                    padding=True, 
                    max_length=256
                ).to(self.device)
                
                outputs = self.model(**inputs)
                probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
                
                # 가장 높은 확률과 인덱스 추출
                max_probs, idxs = torch.max(probs, dim=1)
                
                for prob, idx in zip(max_probs.cpu().numpy(), idxs.cpu().numpy()):
                    label_name = self.le.inverse_transform([idx])[0]
                    # 임계값 확인
                    final_label = label_name if prob >= threshold else "분류 불가"
                    all_results.append((final_label, float(prob)))
        
        return all_results


