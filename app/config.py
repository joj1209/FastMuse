import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent / "common" / ".env"
load_dotenv(env_path)

class Settings:
    APP_NAME = "Muse FastAPI"
    DB_URL = "mysql+pymysql://muse:muse@localhost:3306/muse"
    SCHEDULER_TIMEZONE = "Asia/Seoul"
    
    X_NAVER_CLIENT_ID = os.getenv("X_Naver_Client_Id", "XEZdHo2kX5CdhiJFbgfL")
    X_NAVER_CLIENT_SECRET = os.getenv("X_Naver_Client_Secret", "tpP8PH1Cud")
    DEVELOPER_KEY = os.getenv("DEVELOPER_KEY", "AIzaSyAH00WKaO2C6g7QSY8Chy4tYuU4SAswyc4")
    SERVICE_KEY = os.getenv("SERVICE_KEY", "daf3006d413c5a56854e0d492649ac37")
    KAKAO_CLIENT_SECRET = os.getenv("KAKAO_CLIENT_SECRET", "724c28ce911accf6ec9d2cb0fa5e9898")
    PUBLIC_SERVICE_KEY = os.getenv("PUBLIC_SERVICE_KEY", "wZGYpzXWq52dGL%2BPdY4haekI7B%2FfMs8HaYjX0EWeoQn8dO3g3mES8Z5nHbEIJFpImMOtOoX2OjrXbAaAZghFIw%3D%3D")
    API_KEY = os.getenv("API_KEY", "nBUFejf4RvmVBXo3-Pb56A")
    YOUR_APPKEY = os.getenv("YOUR_APPKEY", "t_0pcpt_22ejt0p0p2b0o_r12j5e08_t")
    SEOUL_API_KEY = os.getenv("SEOUL_API_KEY", "70517359706a6f6a3731477969764a")

settings = Settings()

X_NAVER_CLIENT_ID = settings.X_NAVER_CLIENT_ID
X_NAVER_CLIENT_SECRET = settings.X_NAVER_CLIENT_SECRET
DEVELOPER_KEY = settings.DEVELOPER_KEY
SERVICE_KEY = settings.SERVICE_KEY
KAKAO_CLIENT_SECRET = settings.KAKAO_CLIENT_SECRET
PUBLIC_SERVICE_KEY = settings.PUBLIC_SERVICE_KEY
API_KEY = settings.API_KEY
YOUR_APPKEY = settings.YOUR_APPKEY
SEOUL_API_KEY = settings.SEOUL_API_KEY
