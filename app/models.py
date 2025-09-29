from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Float
from sqlalchemy.sql import func
from .db import Base

# 201: Naver Finance Top 5
class NaverFinance(Base):
    __tablename__ = "dbms_naver_finance"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    stock_cd = Column(String(20), index=True)
    stock_nm = Column(String(100))
    pre_price = Column(BigInteger)
    today_price = Column(BigInteger)
    trading_volume = Column(BigInteger)
    ins_dt = Column(String(14), nullable=False, index=True)  # 14자리 문자열 YYYYMMDDHHMMSS

# 202: EV Portal
class EvTop(Base):
    __tablename__ = "dbms_ev_car_portal"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    sido_nm = Column(String(50))
    region = Column(String(100))
    receipt_way = Column(String(50))
    receipt_priority = Column(String(10))
    value = Column(Integer)
    ins_dt = Column(String(14), nullable=False, index=True)  # 14자리 문자열 YYYYMMDDHHMMSS

# 203: Global Market (FinanceDataReader)
class MarketTop(Base):
    __tablename__ = "dbms_market_stock"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(8), index=True)
    market = Column(String(50))
    stock_day = Column(String(10))
    opening_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    closing_price = Column(Float)
    volume = Column(BigInteger)
    ins_dt = Column(String(20), nullable=False)

# 204: Naver Blog crawl
class BlogCrawl(Base):
    __tablename__ = "dbms_blog_keword"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    keword = Column(String(200))  # 원문 오타(keword) 유지
    title = Column(String(500))
    link = Column(String(1000))
    ins_dt = Column(String(14), nullable=False, index=True)  # 14자리 문자열 YYYYMMDDHHMMSS

# 205: YouTube comments
class YoutubeComment(Base):
    __tablename__ = "dbms_youtube_keword"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    keword = Column(String(200))
    link = Column(String(1000))
    video_id = Column(String(50))
    main_text = Column(String(500))  # 댓글 내용 필드 추가
    comment_author = Column(String(200))
    ins_dt = Column(String(14), nullable=False, index=True)

# 206: Kakao AI Image
class KakaoAIImage(Base):
    __tablename__ = "dbms_kakao_ai_image"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    suggest_word = Column(String(500))

# 207: Kakao Talk tokens
class KakaoTalk(Base):
    __tablename__ = "dbms_kakao_talk"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    access_token = Column(String(2000))
    token_type = Column(String(50))
    refresh_token = Column(String(2000))
    scope = Column(String(500))
    upd_dt = Column(String(19))  # 문자열로 보유
    ins_dt = Column(String(19))

# 208: Public apt trade
class PublicAptTrade(Base):
    __tablename__ = "dbms_public_apt_trade"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(10), index=True)
    sgg_cd = Column(String(20))
    road_nm = Column(String(200))
    apt_nm = Column(String(200))
    excul_use_area = Column(Float)
    deal_year = Column(String(4))
    deal_amount = Column(String(50))
    floor = Column(String(10))
    build_year = Column(String(4))
    ins_dt = Column(String(19))

# 209: KMA forecast
class KmaForecast(Base):
    __tablename__ = "dbms_kma_neighborhood_forecast"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(8), index=True)
    strd_tm = Column(String(4))
    category = Column(String(20))
    nx = Column(Integer)
    ny = Column(Integer)
    obsr_value = Column(String(20))
    ins_dt = Column(String(19))

# 210: Jeju hourly visitor
class JejuFloPop(Base):
    __tablename__ = "dbms_jeju_api_floating_population"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(8), index=True)
    regist_dt = Column(String(14))
    city = Column(String(50))
    emd = Column(String(50))
    gender = Column(String(10))
    age_group = Column(String(20))
    resd_pop = Column(Integer)
    work_pop = Column(Integer)
    visit_pop = Column(Integer)
    ins_dt = Column(String(19))

# 211: Seoul foreign population
class SeoulForPop(Base):
    __tablename__ = "dbms_seoul_api_spop_forn_long_resd_jachi"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(8), index=True)
    stdr_de_id = Column(String(8))
    tmzon_pd_se = Column(String(2))
    adstrd_code_se = Column(String(10))
    tot_lvpop_co = Column(Integer)
    china_staypop_co = Column(Integer)
    etc_staypop_co = Column(Integer)
    ins_dt = Column(String(19))

# 212: Batch stats
class ApiBatchStat(Base):
    __tablename__ = "api_batch_stat"
    id = Column(Integer, primary_key=True)
    strd_dt = Column(String(8), index=True)
    api_nm = Column(String(100))
    data_gb = Column(String(50))
    data_cnt = Column(Integer)
    memo = Column(String(500))
    ins_dt = Column(String(19))
