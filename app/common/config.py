from pathlib import Path
from dotenv import load_dotenv, dotenv_values

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_LOC = BASE_DIR / "muse/.env"
ENV_LOAD = load_dotenv(ENV_LOC)


if ENV_LOAD:
    config = dotenv_values(ENV_LOC)
    print('***************** .env config load success *****************')    
    
    # 01.Naver Finance
    # 02.EV Portal
    # 03.Finace Data Reader

    # 04.Naver API
    X_NAVER_CLIENT_ID = config.get("X_Naver_Client_Id")
    X_NAVER_CLIENT_SECRET = config.get("X_Naver_Client_Secret")
    
    # 05.Google API
    DEVELOPER_KEY = config.get("DEVELOPER_KEY")
    
    # 06.Kakao API
    SERVICE_KEY = config.get("SERVICE_KEY")
    
    # 07.Kakao API(KaTalk)
    KAKAO_CLIENT_SECRET = config.get("KAKAO_CLIENT_SECRET")
    
    # 08.Public API
    PUBLIC_SERVICE_KEY = config.get("PUBLIC_SERVICE_KEY")
    
    # 09.KMA API
    API_KEY = config.get("API_KEY")
    
    # 10.Jeju API
    YOUR_APPKEY = config.get("YOUR_APPKEY")
    
    # 11.Seoul API
    SEOUL_API_KEY = config.get("SEOUL_API_KEY")
else:
    print('env none!!')

print("------------------------------------------------------------")