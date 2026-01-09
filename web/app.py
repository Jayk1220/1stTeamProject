"""
주식 데이터 시각화 Flask 애플리케이션

실행 방법:
  flask run --debug
  또는
  python app.py
"""

from flask import Flask, render_template, request, jsonify
from database.repository import (
    get_industries, 
    get_date_range, 
    get_combined_data
)
from models import SearchParams
from datetime import datetime, timedelta
import json

app = Flask(__name__)
app.config["SECRET_KEY"] = "stock_visualization_2025"

@app.route("/")
def index():
    """메인 페이지 - 검색 폼"""
    industries = get_industries()
    min_date, max_date = get_date_range()
    
    # 기본값: 최근 30일
    default_end = max_date
    default_start = (datetime.strptime(max_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
    
    return render_template(
        "stock/index.html",
        industries=industries,
        min_date=min_date,
        max_date=max_date,
        default_start=default_start,
        default_end=default_end
    )

@app.route("/visualization")
def visualization():
    """시각화 페이지"""
    # 파라미터 가져오기
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    industry = request.args.get('industry')

    # industry를 market_index로도 사용 (같은 값)
    market_index = industry
        
    # 유효성 검사
    if not all([start_date, end_date, industry]):
        return "필수 파라미터가 누락되었습니다.", 400
    
    try:
        # Pydantic 모델로 검증
        params = SearchParams(
            start_date=start_date,
            end_date=end_date,
            industry=industry
        )
    except Exception as e:
        return f"파라미터 오류: {e}", 400
    
    # 데이터 조회 전 디버깅 로그
    print(f"[DEBUG] 조회 파라미터:")
    print(f"  - start_date: {start_date}")
    print(f"  - end_date: {end_date}")
    print(f"  - industry: {industry}")
    print(f"  - market_index: {market_index}")

    # 데이터 조회
    try:
        data = get_combined_data(start_date, end_date, industry, market_index)
        print(f"[DEBUG] 조회된 데이터 개수: {len(data['dates'])}개")
    except Exception as e:
        print(f"[ERROR] 데이터 조회 오류: {e}")
        import traceback
        traceback.print_exc()
        return f"데이터 조회 오류: {e}", 500
    
    # 데이터가 없는 경우
    if not data['dates']:
        return render_template(
            "stock/no_data.html",
            start_date=start_date,
            end_date=end_date,
            industry=industry
        )
    
    # 시각화 페이지 렌더링
    return render_template(
        "stock/visualization.html",
        start_date=start_date,
        end_date=end_date,
        industry=industry,
        market_index=market_index,
        dates=json.dumps(data['dates']),
        closes=json.dumps(data['closes']),
        article_ratios=json.dumps(data['article_ratios']),
        trade_volume_ratios=json.dumps(data['trade_volume_ratios']),
        mean_sents=json.dumps(data['mean_sents']),
        risk=json.dumps(data['risk']),
        predicts=json.dumps(data['predicts'])
    )

@app.route("/api/data")
def api_data():
    """API 엔드포인트 - JSON 데이터 반환"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    industry = request.args.get('industry')
    market_index = request.args.get('market_index', 'KOSPI')
    
    if not all([start_date, end_date, industry]):
        return jsonify({"error": "필수 파라미터 누락"}), 400
    
    try:
        data = get_combined_data(start_date, end_date, industry, market_index)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def page_not_found(error):
    """404 에러 핸들러"""
    return render_template("page_not_found.html", error=error), 404

@app.errorhandler(500)
def internal_error(error):
    """500 에러 핸들러"""
    return render_template("error.html", error=error), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
