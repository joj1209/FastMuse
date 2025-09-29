# Naver API Configuration
# 실제 운영 시에는 환경변수나 별도의 설정 파일에서 관리해야 합니다.

# 네이버 개발자센터에서 발급받은 API 키
# https://developers.naver.com/apps/#/register 에서 발급 가능

# 개발/테스트용 더미 키 (실제 운영 시 변경 필요)
X_NAVER_CLIENT_ID = "YOUR_NAVER_CLIENT_ID"
X_NAVER_CLIENT_SECRET = "YOUR_NAVER_CLIENT_SECRET"

# 네이버 API 키 설정 방법:
# 1. https://developers.naver.com/ 접속
# 2. 로그인 후 "Application 등록"
# 3. "검색" API 선택
# 4. Client ID와 Client Secret 복사
# 5. 위의 값들을 실제 키로 변경

# 환경변수로 설정하는 경우:
import os
X_NAVER_CLIENT_ID = os.getenv('NAVER_CLIENT_ID', 'YOUR_NAVER_CLIENT_ID')
X_NAVER_CLIENT_SECRET = os.getenv('NAVER_CLIENT_SECRET', 'YOUR_NAVER_CLIENT_SECRET')