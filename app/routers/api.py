
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from ..db import get_db
from ..models import (
    NaverFinance, EvTop, MarketTop, BlogCrawl, YoutubeComment, KakaoAIImage, KakaoTalk,
    PublicAptTrade, KmaForecast, JejuFloPop, SeoulForPop, ApiBatchStat
)
from app.service.finance_data_reader_parser import FinanceDataReaderParser
from app.service.naver_finance_crawler import NaverFinanceCrawler
from app.service.ev_car_portal_crawler import EvCarPortalCrawler
from app.service.naver_blog_crawler import NaverBlogCrawler
from app.service.youtube_comment_crawler import YoutubeCommentCrawler
from app.service.kakao_talk_crawler import KakaoTalkCrawler
from app.service.airflow_runner import AirflowRunner
from app.service.seoul_public_data_crawler import SeoulPublicDataCrawler
from app.service.jeju_public_data_crawler import JejuPublicDataCrawler
from app.service.kma_public_data_crawler import KmaPublicDataCrawler

router = APIRouter(prefix="/api", tags=["api"])

@router.post("/run/naver-finance-crawler")
async def run_naver_finance_crawler(request: Request):
    """네이버 금융 크롤링 실행"""
    try:
        crawler = NaverFinanceCrawler()
        data_list = crawler.run()
        
        # ins_dt가 이제 문자열이므로 직렬화 처리 불필요
        return JSONResponse({
            "message": "네이버 금융 크롤링 완료", 
            "count": len(data_list), 
            "data": data_list
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "네이버 금융 크롤링 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/ev-car-portal-crawler")
async def run_ev_car_portal_crawler(request: Request):
    """EV 포털 크롤링 실행"""
    try:
        crawler = EvCarPortalCrawler()
        data_list = crawler.run()
        
        # ins_dt가 이제 문자열이므로 직렬화 처리 불필요
        return JSONResponse({
            "message": "EV 포털 크롤링 완료", 
            "count": len(data_list), 
            "data": data_list
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "EV 포털 크롤링 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/finance-data-reader")
async def run_finance_data_reader(request: Request):
    parser = FinanceDataReaderParser()
    df = parser.get_data()
    df['stock_day'] = df['stock_day'].astype(str)  # Timestamp -> str 변환
    parser.save_to_dbms_market_stock(df)
    # 결과 row 수 반환
    return JSONResponse({"rows": df.to_dict(orient="records")})

@router.post("/run/naver-blog-crawler")
async def run_naver_blog_crawler(request: Request):
    """네이버 블로그 검색 크롤링 실행"""
    try:
        crawler = NaverBlogCrawler()
        data_list = crawler.run()
        
        # ins_dt가 이제 문자열이므로 직렬화 처리 불필요
        return JSONResponse({
            "message": "네이버 블로그 검색 완료", 
            "count": len(data_list), 
            "data": data_list
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "네이버 블로그 검색 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/youtube-comment-crawler")
async def run_youtube_comment_crawler(request: Request):
    """유튜브 댓글 검색 크롤링 실행"""
    try:
        crawler = YoutubeCommentCrawler()
        data_list = crawler.run()
        
        # ins_dt가 문자열이므로 직렬화 처리 불필요
        return JSONResponse({
            "message": "유튜브 댓글 검색 완료", 
            "count": len(data_list), 
            "data": data_list
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "유튜브 댓글 검색 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/kakao-talk-crawler")
async def run_kakao_talk_crawler(request: Request):
    """카카오톡 API 크롤러 실행"""
    try:
        import time
        crawler = KakaoTalkCrawler()
        result = crawler.run_crawl_kakao_talk()
        
        return JSONResponse({
            "message": "카카오톡 API 실행 완료",
            "result": result,
            "timestamp": time.strftime('%Y%m%d%H%M%S')
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "카카오톡 API 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/seoul-public-data-crawler")
async def run_seoul_public_data_crawler(request: Request):
    """서울 공공데이터 크롤러 실행"""
    try:
        import time
        crawler = SeoulPublicDataCrawler()
        result = crawler.run_seoul_api_crawler()
        
        return JSONResponse({
            "message": "서울 공공데이터 크롤러 실행 완료",
            "result": result,
            "timestamp": time.strftime('%Y%m%d%H%M%S')
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "서울 공공데이터 크롤러 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/kma-public-data-crawler")
async def run_kma_public_data_crawler(request: Request):
    """기상청 공공데이터 크롤러 실행"""
    try:
        import time
        crawler = KmaPublicDataCrawler()
        result = crawler.run_kma_api_crawler()
        
        return JSONResponse({
            "message": "기상청 공공데이터 크롤러 실행 완료",
            "result": result,
            "timestamp": time.strftime('%Y%m%d%H%M%S')
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "기상청 공공데이터 크롤러 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/jeju-public-data-crawler")
async def run_jeju_public_data_crawler(request: Request):
    """제주 공공데이터 크롤러 실행"""
    try:
        import time
        crawler = JejuPublicDataCrawler()
        result = crawler.run_jeju_api_crawler()
        
        return JSONResponse({
            "message": "제주 공공데이터 크롤러 실행 완료",
            "result": result,
            "timestamp": time.strftime('%Y%m%d%H%M%S')
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "제주 공공데이터 크롤러 실행 중 오류 발생", "details": str(e)}
        )

@router.post("/run/airflow-bash-operator")
async def run_airflow_bash_operator(request: Request):
    """Airflow dags_bash_operator DAG 실행"""
    try:
        import time
        runner = AirflowRunner()
        result = runner.run_bash_operator_dag()
        
        return JSONResponse({
            "message": "Airflow DAG 실행 완료",
            "result": result,
            "timestamp": time.strftime('%Y%m%d%H%M%S')
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Airflow DAG 실행 중 오류 발생", "details": str(e)}
        )

@router.get("/status/airflow")
def check_airflow_status():
    """Airflow Docker 상태를 확인합니다"""
    try:
        runner = AirflowRunner()
        status = runner.check_docker_status()
        return status
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Airflow 상태 확인 중 오류 발생", "details": str(e)}
        )

# DB 연결 테스트 엔드포인트
@router.get("/db/test")
def db_test(db: Session = Depends(get_db)):
    try:
        # SQLAlchemy의 text()로 쿼리 명시
        result = db.execute(text("SELECT 1")).scalar()
        return {"db_connection": "ok", "result": result}
    except Exception as e:
        return {"db_connection": "fail", "error": str(e)}

# 공통 페이징 파라미터
def paging(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return {"limit": limit, "offset": offset}


# 201: Naver Finance Top 5 (dbms_naver_finance)
@router.get("/stock/top5")
def stock_top5(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(NaverFinance).order_by(NaverFinance.ins_dt.desc(), NaverFinance.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [
        {
            "strd_dt": r.strd_dt,
            "stock_cd": r.stock_cd,
            "stock_nm": r.stock_nm,
            "pre_price": r.pre_price,
            "today_price": r.today_price,
            "trading_volume": r.trading_volume,
            "ins_dt": r.ins_dt,
        }
        for r in rows
    ]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 201-1: Naver Finance Top 5 (All)
@router.get("/stock/all")
def stock_all(db: Session = Depends(get_db)):
    rows = db.query(NaverFinance).order_by(NaverFinance.ins_dt.desc(), NaverFinance.id.desc()).all()
    items = [
        {
            "strd_dt": r.strd_dt,
            "stock_cd": r.stock_cd,
            "stock_nm": r.stock_nm,
            "pre_price": r.pre_price,
            "today_price": r.today_price,
            "trading_volume": r.trading_volume,
            "ins_dt": r.ins_dt,
        }
        for r in rows
    ]
    return {"items": items}

# 202: EV Top 10
@router.get("/ev/top10")
def ev_top10(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(EvTop).order_by(EvTop.ins_dt.desc(), EvTop.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, sido_nm=r.sido_nm, region=r.region, receipt_way=r.receipt_way,
        receipt_priority=r.receipt_priority, value=r.value, ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 202-1: EV Top 10 (All)
@router.get("/ev/all")
def ev_all(db: Session = Depends(get_db)):
    rows = db.query(EvTop).order_by(EvTop.ins_dt.desc(), EvTop.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, sido_nm=r.sido_nm, region=r.region, receipt_way=r.receipt_way,
        receipt_priority=r.receipt_priority, value=r.value, ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 203: Market Top 10
@router.get("/market/top10")
def market_top10(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(MarketTop).order_by(MarketTop.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, market=r.market, stock_day=r.stock_day, opening_price=r.opening_price,
        high_price=r.high_price, low_price=r.low_price, closing_price=r.closing_price, volume=r.volume
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 203-1: Market Top 10 (All)
@router.get("/market/all")
def market_all(db: Session = Depends(get_db)):
    rows = db.query(MarketTop).order_by(MarketTop.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, market=r.market, stock_day=r.stock_day, opening_price=r.opening_price,
        high_price=r.high_price, low_price=r.low_price, closing_price=r.closing_price, volume=r.volume
    ) for r in rows]
    return {"items": items}

# 204: Naver Blog
@router.get("/blog/naver")
def blog_naver(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(BlogCrawl).order_by(BlogCrawl.ins_dt.desc(), BlogCrawl.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, keword=r.keword, title=r.title, link=r.link,
        ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 204-1: Naver Blog (All)
@router.get("/blog/all")
def blog_all(db: Session = Depends(get_db)):
    rows = db.query(BlogCrawl).order_by(BlogCrawl.ins_dt.desc(), BlogCrawl.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, keword=r.keword, title=r.title, link=r.link,
        ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 205: YouTube comments
@router.get("/youtube/comments")
def youtube_comments(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(YoutubeComment).order_by(YoutubeComment.ins_dt.desc(), YoutubeComment.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, keword=r.keword, link=r.link, video_id=r.video_id,
        comment_author=r.comment_author,
        ins_dt=r.ins_dt.isoformat() if getattr(r, "ins_dt", None) and hasattr(r.ins_dt, "isoformat") else r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 205-1: YouTube comments (All)
@router.get("/youtube/all")
def youtube_all(db: Session = Depends(get_db)):
    rows = db.query(YoutubeComment).order_by(YoutubeComment.ins_dt.desc(), YoutubeComment.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, keword=r.keword, link=r.link, video_id=r.video_id,
        comment_author=r.comment_author,
        ins_dt=r.ins_dt.isoformat() if getattr(r, "ins_dt", None) and hasattr(r.ins_dt, "isoformat") else r.ins_dt
    ) for r in rows]
    return {"items": items}

# 206: Kakao AI Image
@router.get("/kakao/ai-image")
def kakao_ai_image(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(KakaoAIImage).order_by(KakaoAIImage.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(strd_dt=r.strd_dt, suggest_word=r.suggest_word) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 206-1: Kakao AI Image (All)
@router.get("/kakao/ai-image/all")
def kakao_ai_image_all(db: Session = Depends(get_db)):
    rows = db.query(KakaoAIImage).order_by(KakaoAIImage.id.desc()).all()
    items = [dict(strd_dt=r.strd_dt, suggest_word=r.suggest_word) for r in rows]
    return {"items": items}

# 207: Kakao Talk token
@router.get("/kakao/talk")
def kakao_talk(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(KakaoTalk).order_by(KakaoTalk.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, access_token=r.access_token, token_type=r.token_type,
        refresh_token=r.refresh_token, scope=r.scope, upd_dt=r.upd_dt, ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 207-1: Kakao Talk token (All)
@router.get("/kakao/talk/all")
def kakao_talk_all(db: Session = Depends(get_db)):
    rows = db.query(KakaoTalk).order_by(KakaoTalk.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, access_token=r.access_token, token_type=r.token_type,
        refresh_token=r.refresh_token, scope=r.scope, upd_dt=r.upd_dt, ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 208: Public apt trade
@router.get("/public/apt-trade")
def public_apt_trade(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(PublicAptTrade).order_by(PublicAptTrade.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, sgg_cd=r.sgg_cd, road_nm=r.road_nm, apt_nm=r.apt_nm,
        excul_use_area=r.excul_use_area, deal_year=r.deal_year, deal_amount=r.deal_amount,
        floor=r.floor, build_year=r.build_year, ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 208-1: Public apt trade (All)
@router.get("/public/apt-trade/all")
def public_apt_trade_all(db: Session = Depends(get_db)):
    rows = db.query(PublicAptTrade).order_by(PublicAptTrade.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, sgg_cd=r.sgg_cd, road_nm=r.road_nm, apt_nm=r.apt_nm,
        excul_use_area=r.excul_use_area, deal_year=r.deal_year, deal_amount=r.deal_amount,
        floor=r.floor, build_year=r.build_year, ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 209: KMA forecast
@router.get("/kma/forecast")
def kma_forecast(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(KmaForecast).order_by(KmaForecast.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, strd_tm=r.strd_tm, category=r.category,
        nx=r.nx, ny=r.ny, obsr_value=r.obsr_value, ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 209-1: KMA forecast (All)
@router.get("/kma/forecast/all")
def kma_forecast_all(db: Session = Depends(get_db)):
    rows = db.query(KmaForecast).order_by(KmaForecast.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, strd_tm=r.strd_tm, category=r.category,
        nx=r.nx, ny=r.ny, obsr_value=r.obsr_value, ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 210: Jeju visitors
@router.get("/jeju/flo-pop")
def jeju_flo_pop(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(JejuFloPop).order_by(JejuFloPop.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, regist_dt=r.regist_dt, city=r.city, emd=r.emd,
        gender=r.gender, age_group=r.age_group, resd_pop=r.resd_pop,
        work_pop=r.work_pop, visit_pop=r.visit_pop, ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 210-1: Jeju visitors (All)
@router.get("/jeju/flo-pop/all")
def jeju_flo_pop_all(db: Session = Depends(get_db)):
    rows = db.query(JejuFloPop).order_by(JejuFloPop.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, regist_dt=r.regist_dt, city=r.city, emd=r.emd,
        gender=r.gender, age_group=r.age_group, resd_pop=r.resd_pop,
        work_pop=r.work_pop, visit_pop=r.visit_pop, ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 211: Seoul foreign pop
@router.get("/seoul/for-pop")
def seoul_for_pop(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(SeoulForPop).order_by(SeoulForPop.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, stdr_de_id=r.stdr_de_id, tmzon_pd_se=r.tmzon_pd_se,
        adstrd_code_se=r.adstrd_code_se, tot_lvpop_co=r.tot_lvpop_co,
        china_staypop_co=r.china_staypop_co, etc_staypop_co=r.etc_staypop_co,
        ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 211-1: Seoul foreign pop (All)
@router.get("/seoul/for-pop/all")
def seoul_for_pop_all(db: Session = Depends(get_db)):
    rows = db.query(SeoulForPop).order_by(SeoulForPop.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, stdr_de_id=r.stdr_de_id, tmzon_pd_se=r.tmzon_pd_se,
        adstrd_code_se=r.adstrd_code_se, tot_lvpop_co=r.tot_lvpop_co,
        china_staypop_co=r.china_staypop_co, etc_staypop_co=r.etc_staypop_co,
        ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}

# 212: Batch stats
@router.get("/batch/stats")
def api_batch_stats(p: dict = Depends(paging), db: Session = Depends(get_db)):
    q = db.query(ApiBatchStat).order_by(ApiBatchStat.id.desc())
    total = q.count()
    rows = q.limit(p["limit"]).offset(p["offset"]).all()
    items = [dict(
        strd_dt=r.strd_dt, api_nm=r.api_nm, data_gb=r.data_gb,
        data_cnt=r.data_cnt, memo=r.memo, ins_dt=r.ins_dt
    ) for r in rows]
    return {"meta": {"total": total, "limit": p["limit"], "offset": p["offset"]}, "items": items}

# 212-1: Batch stats (All)
@router.get("/batch/stats/all")
def api_batch_stats_all(db: Session = Depends(get_db)):
    rows = db.query(ApiBatchStat).order_by(ApiBatchStat.id.desc()).all()
    items = [dict(
        strd_dt=r.strd_dt, api_nm=r.api_nm, data_gb=r.data_gb,
        data_cnt=r.data_cnt, memo=r.memo, ins_dt=r.ins_dt
    ) for r in rows]
    return {"items": items}