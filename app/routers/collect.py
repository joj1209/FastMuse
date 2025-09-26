from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from ..db import get_db
from ..models import Item
from ..schemas import ItemCreate, ItemOut
from ..services.scraper import fetch_example

router = APIRouter(prefix="/collect", tags=["collect"])

@router.post("/run", response_model=list[ItemOut])
async def run_collect(body: ItemCreate, bg: BackgroundTasks, db: Session = Depends(get_db)):
    data = await fetch_example(body.keyword) if body.source == "example" else []
    items_out = []
    for d in data:
        m = Item(source=body.source, keyword=body.keyword, title=d.get("title"), url=d.get("url"), raw=d.get("raw"))
        db.add(m)
    db.commit()
    items_out = db.query(Item).order_by(Item.id.desc()).limit(len(data)).all()
    return items_out
