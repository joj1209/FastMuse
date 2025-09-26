from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .db import Base, engine
from .routers import api

app = FastAPI(title="Muse API (B-Option)")

# DB 초기화(개발용)
Base.metadata.create_all(bind=engine)

# 정적 파일
app.mount("/static", StaticFiles(directory="static"), name="static")

# API 라우터
app.include_router(api.router)

# 선택사항: 루트에서 index.html 제공 (프론트가 별도면 생략)
from fastapi.templating import Jinja2Templates
from fastapi import Request
from .routers import ui


# 템플릿
templates = Jinja2Templates(directory="app/templates")

app.include_router(ui.router)


