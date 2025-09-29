# NextMuse Project Configuration
# API 키 및 환경변수 설정 파일
# .env 파일에서 실제 키 값들을 읽어옵니다.

import os
from pathlib import Path
from dotenv import load_dotenv, dotenv_values

# 프로젝트 경로 설정
BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE_PATH = BASE_DIR / "app/common/.env"

# .env 파일 로드
load_dotenv(ENV_FILE_PATH)

try:
    config = dotenv_values(ENV_FILE_PATH)
    print('***************** .env config load success *****************')
    
    # =================================================================
    # API 키 설정 (각 서비스별)
    # =================================================================
    
    # 01. 네이버 금융 크롤링 - 별도 API 키 불필요 (HTML 파싱)
    
    # 02. EV 포털 크롤링 - 별도 API 키 불필요 (Selenium)
    
    # 03. FinanceDataReader - 별도 API 키 불필요 (라이브러리)
    
    # 04. 네이버 검색 API
    # https://developers.naver.com/apps/#/register 에서 발급
    X_NAVER_CLIENT_ID = config.get("X_Naver_Client_Id") or os.getenv('NAVER_CLIENT_ID')
    X_NAVER_CLIENT_SECRET = config.get("X_Naver_Client_Secret") or os.getenv('NAVER_CLIENT_SECRET')
    
    # 05. Google API (YouTube 등)
    # https://console.cloud.google.com/ 에서 발급
    DEVELOPER_KEY = config.get("DEVELOPER_KEY") or os.getenv('GOOGLE_API_KEY')
    
    # 06. Kakao API
    # https://developers.kakao.com/ 에서 발급
    SERVICE_KEY = config.get("SERVICE_KEY") or os.getenv('KAKAO_SERVICE_KEY')
    
    # 07. Kakao Talk API
    KAKAO_CLIENT_SECRET = config.get("KAKAO_CLIENT_SECRET") or os.getenv('KAKAO_CLIENT_SECRET')
    
    # 08. 공공데이터포털 API
    # https://www.data.go.kr/ 에서 발급
    PUBLIC_SERVICE_KEY = config.get("PUBLIC_SERVICE_KEY") or os.getenv('PUBLIC_API_KEY')
    
    # 09. 기상청 API
    API_KEY = config.get("API_KEY") or os.getenv('KMA_API_KEY')
    
    # 10. 제주 API
    YOUR_APPKEY = config.get("YOUR_APPKEY") or os.getenv('JEJU_API_KEY')
    
    # 11. 서울시 API
    SEOUL_API_KEY = config.get("SEOUL_API_KEY") or os.getenv('SEOUL_API_KEY')
    
    print("OK 모든 API 키 로드 완료")
    
except Exception as e:
    print(f'ERROR .env 파일 로드 실패: {e}')
    
    # 개발/테스트용 더미 값들
    X_NAVER_CLIENT_ID = "YOUR_NAVER_CLIENT_ID"
    X_NAVER_CLIENT_SECRET = "YOUR_NAVER_CLIENT_SECRET"
    DEVELOPER_KEY = "YOUR_GOOGLE_API_KEY"
    SERVICE_KEY = "YOUR_KAKAO_SERVICE_KEY"
    KAKAO_CLIENT_SECRET = "YOUR_KAKAO_CLIENT_SECRET"
    PUBLIC_SERVICE_KEY = "YOUR_PUBLIC_API_KEY"
    API_KEY = "YOUR_KMA_API_KEY"
    YOUR_APPKEY = "YOUR_JEJU_API_KEY"
    SEOUL_API_KEY = "YOUR_SEOUL_API_KEY"
    
    print("WARNING 더미 API 키 사용 중 - 실제 운영 시 .env 파일을 설정하세요")

print("------------------------------------------------------------")