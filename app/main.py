from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .db import Base, engine
from .routers import api, ui, collect

# FastAPI 애플리케이션 인스턴스 생성
app = FastAPI(title="Muse API (B-Option)")

# DB 테이블 생성 (애플리케이션 시작 시)
# 참고: 프로덕션 환경에서는 Alembic과 같은 마이그레이션 도구를 사용하는 것이 더 안전합니다.
Base.metadata.create_all(bind=engine)

# 정적 파일 마운트
# 'static' 폴더의 파일들을 '/static' 경로로 제공합니다.
app.mount("/static", StaticFiles(directory="static"), name="static")

# API 라우터 포함
app.include_router(api.router)
app.include_router(ui.router)
app.include_router(collect.router)

# 서버 실행
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
