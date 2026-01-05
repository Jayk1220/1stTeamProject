import os
import joblib
import pandas as pd
import numpy as np
import sklearn 

# ==============================================================================
# [주가 등락 예측기 클래스 정의]
# ==============================================================================

class DailyStockPredictor:
    """
    일일 주가 등락 예측 (상승/하락) 클래스
    
    [주요 기능]
    1. 사전 학습된 ML 모델(.joblib)과 Scaler 로드
    2. 리스크 지표 및 시장 데이터를 입력받아 다음 날 주가 등락(0 or 1) 예측
    """
    
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 모델 및 스케일러 파일 경로
        # (파일명은 실제 학습된 산출물 이름에 맞춰야 함)
        self.model_path = os.path.join(self.base_dir, "predict_Acc62.50_Prec61.54_Loss0.72_Err0.38.joblib")
        self.scaler_path = os.path.join(self.base_dir, "MinMaxScaler.joblib")
        
        self.model = None
        self.scaler = None
        self._load_resources()

    def _load_resources(self):
        """모델과 스케일러를 안전하게 로드"""
        try:
            if os.path.exists(self.model_path):
                self.model = joblib.load(self.model_path)
                print(f"[Predictor] 모델 로드 완료: {os.path.basename(self.model_path)}")
            else:
                print(f"[Predictor] 모델 파일 없음: {self.model_path}")

            if os.path.exists(self.scaler_path):
                self.scaler = joblib.load(self.scaler_path)
                print(f"[Predictor] 스케일러 로드 완료: {os.path.basename(self.scaler_path)}")
            else:
                print(f"[Predictor] 스케일러 파일 없음: {self.scaler_path}")
                
        except Exception as e:
            print(f"[Predictor] 리소스 로드 중 오류: {repr(e)}")

    def predict(self, data_row):
        """
        단일 데이터 행에 대한 예측 수행
        
        :param data_row: 예측에 필요한 피처를 담은 딕셔너리
        :return: 0(하락/보합) 또는 1(상승), 실패 시 None
        """
        if self.model is None or self.scaler is None:
            return None
            
        try:
            # [Feature Engineering]
            # 모델 학습 시 사용된 피처 순서와 정확히 일치해야 함
            
            # 산업군 인코딩 (Mapping)
            industry = data_row.get('INDUSTRY', '기타')
            ind_map = {"건설": 0, "자동차": 1, "헬스케어": 2, "기타": 3}
            ind_code = ind_map.get(industry, 3) # Default 3

            # 피처 리스트 구성
            features = [
                data_row['ave_sent'],       # 평균 감성 점수
                data_row['news_count'],     # 기사 수
                ind_code,                   # 산업 코드
                data_row['close'],          # 종가
                data_row['volume'],         # 거래량
                data_row['change'],         # 등락폭
                data_row['total_news'],     # 전체 뉴스 수
                data_row['total_vol'],      # 전체 거래량
                data_row['risk_index'],     # 리스크 인덱스
                data_row['article_ratio'],  # 기사 점유율
                data_row['volume_ratio']    # 거래량 점유율
            ]
            
            # 2D Array 변환 (1 sample, N features)
            X = np.array(features).reshape(1, -1)
            
            # Scaling
            X_scaled = self.scaler.transform(X)
            
            # Prediction
            pred = self.model.predict(X_scaled)
            return int(pred[0])
            
        except Exception as e:
            print(f"[Predictor] 예측 수행 실패: {e}")
            return None
