from pydantic import BaseModel
from typing import Optional

# 공통: 리스트 응답 형태
class PageMeta(BaseModel):
    total: int
    limit: int
    offset: int

class Page(BaseModel):
    meta: PageMeta

# 201
class StockOut(BaseModel):
    strd_dt: Optional[str] = None
    stock_cd: Optional[str] = None
    stock_nm: Optional[str] = None
    pre_price: Optional[int] = None
    today_price: Optional[int] = None
    trading_volume: Optional[int] = None
    ins_dt: Optional[str] = None

class StockList(BaseModel):
    meta: PageMeta
    items: list[StockOut]

# 나머지도 필요시 동일 패턴으로 정의(간단 시에는 dict 반환도 허용)
