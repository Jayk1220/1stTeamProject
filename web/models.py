# pip install pydantic
# models.py

from pydantic import BaseModel, Field
from typing import Optional
from datetime import date

class StockData(BaseModel):
    """주식 데이터 모델"""
    id: Optional[int] = None
    sdate: date
    market_index: str
    close: float
    change: float
    volume: int

class RiskData(BaseModel):
    """리스크 데이터 모델"""
    id: Optional[int] = None
    rdate: date
    industry: str
    mean_sent: float
    risk: float
    predict: float
    total_news: int
    article_ratio: float
    total_volume: int
    trade_volume_ratio: float

class SearchParams(BaseModel):
    """검색 파라미터 모델"""
    start_date: str = Field(..., description="시작 날짜 (YYYY-MM-DD)")
    end_date: str = Field(..., description="종료 날짜 (YYYY-MM-DD)")
    industry: str = Field(..., description="산업군")