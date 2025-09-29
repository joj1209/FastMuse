# Config 통합 마이그레이션 가이드

## 개요

NextMuse 프로젝트의 설정 관리를 효율화하기 위해 `app.common.config`와 `app.config`를 통합하였습니다.

## 주요 변경사항

### Before (기존 구조)
```python
# app/config.py - 기본 앱 설정만
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Muse FastAPI"
    DB_URL: str = "mysql+pymysql://muse:muse@localhost:3306/muse"
    # ...

# app/common/config.py - API 키들만
X_NAVER_CLIENT_ID = config.get("X_Naver_Client_Id")
DEVELOPER_KEY = config.get("DEVELOPER_KEY")
# ...
```

### After (통합된 구조)
```python
# app/config.py - 모든 설정 통합
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # 기본 앱 설정
    APP_NAME: str = "Muse FastAPI"
    DB_URL: str = "mysql+pymysql://muse:muse@localhost:3306/muse"
    
    # API 키들 (모든 서비스별로 정리)
    X_NAVER_CLIENT_ID: str = "XEZdHo2kX5CdhiJFbgfL"
    DEVELOPER_KEY: str = "AIzaSyAH00WKaO2C6g7QSY8Chy4tYuU4SAswyc4"
    # ...
    
    class Config:
        env_file = "app/common/.env"  # .env 파일 자동 로드
```

## 장점

### 1. **일관된 설정 관리**
- 모든 설정이 한 곳에서 관리됨
- pydantic_settings의 타입 검증 및 환경변수 자동 로드 활용

### 2. **향상된 개발 경험**
- IDE 자동완성 지원
- 타입 힌트 제공
- 런타임 타입 검증

### 3. **간소화된 Import**
```python
# Before
from app.common.config import X_NAVER_CLIENT_ID, X_NAVER_CLIENT_SECRET

# After  
from app.config import settings
# settings.X_NAVER_CLIENT_ID 사용
```

### 4. **호환성 보장**
- 기존 코드의 직접 import 방식도 지원
- 점진적 마이그레이션 가능

## 사용 방법

### 권장 방식 (새로운 코드)
```python
from app.config import settings

class SomeService:
    def __init__(self):
        self.api_key = settings.X_NAVER_CLIENT_ID
        self.secret = settings.X_NAVER_CLIENT_SECRET
```

### 호환성 방식 (기존 코드)
```python
from app.config import X_NAVER_CLIENT_ID, X_NAVER_CLIENT_SECRET  # 여전히 작동함

class SomeService:
    def __init__(self):
        self.api_key = X_NAVER_CLIENT_ID
        self.secret = X_NAVER_CLIENT_SECRET
```

## API 키 매핑 테이블

| 서비스 | 변수명 | 환경변수 | 용도 |
|--------|--------|----------|------|
| 네이버 검색 | X_NAVER_CLIENT_ID | X_Naver_Client_Id | 네이버 검색 API 클라이언트 ID |
| 네이버 검색 | X_NAVER_CLIENT_SECRET | X_Naver_Client_Secret | 네이버 검색 API 클라이언트 시크릿 |
| Google/YouTube | DEVELOPER_KEY | DEVELOPER_KEY | YouTube Data API v3 키 |
| Kakao | SERVICE_KEY | SERVICE_KEY | Kakao API 서비스 키 |
| Kakao Talk | KAKAO_CLIENT_SECRET | KAKAO_CLIENT_SECRET | Kakao Talk API 시크릿 |
| 공공데이터 | PUBLIC_SERVICE_KEY | PUBLIC_SERVICE_KEY | 공공데이터포털 API 키 |
| 기상청 | API_KEY | API_KEY | 기상청 날씨 API 키 |
| 제주 | YOUR_APPKEY | YOUR_APPKEY | 제주 데이터허브 API 키 |
| 서울시 | SEOUL_API_KEY | SEOUL_API_KEY | 서울시 공공데이터 API 키 |

## 환경 파일 설정

`.env` 파일은 `app/common/.env`에 위치하며, 다음과 같이 구성됩니다:

```properties
# 네이버 API
X_Naver_Client_Id=your_naver_client_id
X_Naver_Client_Secret=your_naver_client_secret

# Google API
DEVELOPER_KEY=your_google_api_key

# Kakao API
SERVICE_KEY=your_kakao_service_key
KAKAO_CLIENT_SECRET=your_kakao_client_secret

# 기타 API 키들...
```

## 마이그레이션 체크리스트

- [x] `app/config.py`에 통합 Settings 클래스 구현
- [x] 모든 API 키와 환경변수 매핑 완료
- [x] 호환성을 위한 변수 export 추가
- [x] 기존 서비스 파일들의 import 문 업데이트
- [x] `app/common/config.py` 파일 삭제
- [ ] 전체 애플리케이션 테스트 실행
- [ ] 운영 환경 배포 전 검증

## 주의사항

1. **환경변수 우선순위**: .env 파일 → 시스템 환경변수 → 기본값
2. **보안**: 실제 API 키는 .env 파일에서 관리, 코드에는 기본값만 포함
3. **타입 안전성**: pydantic_settings가 자동으로 타입 변환 및 검증 수행

## 문제해결

### Import 오류 발생 시
```python
# 오류 발생하는 코드
from app.common.config import SOME_KEY  

# 해결책
from app.config import settings
# settings.SOME_KEY 사용
```

### 환경변수 인식 안됨
1. `.env` 파일 경로 확인: `app/common/.env`
2. 환경변수명 대소문자 확인
3. Settings 클래스의 Config.env_file 경로 확인

이제 NextMuse의 모든 설정이 중앙집중식으로 관리되며, 더 안전하고 유지보수하기 쉬운 구조가 되었습니다.