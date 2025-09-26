from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Muse FastAPI"
    DB_URL: str = "mysql+pymysql://muse:muse@localhost:3306/muse"
    SCHEDULER_TIMEZONE: str = "Asia/Seoul"

settings = Settings()
