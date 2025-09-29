import pendulum
import json
import requests
import time
import logging
from app.db import SessionLocal
from app.models import KakaoTalk
from app.config import settings

REDIRECT_URL = 'https://example.com/oauth'
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class KakaoTalkCrawler:
    def __init__(self):
        self.client_id = settings.KAKAO_CLIENT_SECRET
        # 기존 토큰 정보 (실제 운영시에는 DB나 환경변수에서 관리)
        self.kakao_tokens = {
            'access_token': '59ddcc_OxMeIuYrgkNQAwtWsNTO1H3WaAAAAAQopyNgAAAGVG9CE1xKZRqbpl2cW',
            'token_type': 'bearer',
            'refresh_token': '8Mc_YXaWgKg2JMxBFNwAxOCrHh5rdhzhAAAAAgopyNgAAAGVG9CE0xKZRqbpl2cW',
            'expires_in': 21599,
            'scope': 'talk_message',
            'refresh_token_expires_in': 5183999
        }

    def _refresh_token_to_variable(self):
        """액세스 토큰 갱신"""
        try:
            refresh_token = self.kakao_tokens.get('refresh_token')
            url = "https://kauth.kakao.com/oauth/token"
            
            data = {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "refresh_token": refresh_token
            }
            
            response = requests.post(url, data=data)
            rslt = response.json()
            
            new_access_token = rslt.get('access_token')
            new_refresh_token = rslt.get('refresh_token')
            
            if new_access_token:
                self.kakao_tokens['access_token'] = new_access_token
            if new_refresh_token:
                self.kakao_tokens['refresh_token'] = new_refresh_token

            now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
            self.kakao_tokens['updated'] = now
            
            logger.info('[카카오 API] 토큰 업데이트 완료')
            return self.kakao_tokens
            
        except Exception as e:
            logger.error(f"[카카오 API] 토큰 갱신 오류: {e}")
            return self.kakao_tokens

    def send_kakao_msg(self, talk_title: str, content: dict):
        """카카오톡 메시지 전송"""
        logger.info(f"[카카오 API] 메시지 전송 시작 - 제목: {talk_title}")
        
        try_tokens = {}
        try_cnt = 0
        try_tokens_exits = 0
        
        while True:
            # Access 토큰 설정
            if try_tokens_exits == 0:
                tokens = self.kakao_tokens
            else:
                tokens = try_tokens
                
            access_token = tokens.get('access_token')
            content_lst = []
            button_lst = []

            # 메시지 내용 구성
            for title, msg in content.items():
                content_lst.append({
                    'title': f'{title}',
                    'description': f'{msg}',
                    'image_url': '',
                    'image_width': 40,
                    'image_height': 40,
                    'link': {
                        'web_url': '',
                        'mobile_web_url': ''
                    }
                })
                button_lst.append({
                    'title': '',
                    'link': {
                        'web_url': '',
                        'mobile_web_url': ''
                    }
                })

            list_data = {
                'object_type': 'list',
                'header_title': f'{talk_title}',
                'header_link': {
                    'web_url': '',
                    'mobile_web_url': '',
                    'android_execution_params': 'main',
                    'ios_execution_params': 'main'
                },
                'contents': content_lst,
                'buttons': button_lst
            }

            # 메시지 전송
            send_url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
            headers = {
                "Authorization": f'Bearer {access_token}'
            }
            data = {'template_object': json.dumps(list_data)}
            response = requests.post(send_url, headers=headers, data=data)
            
            logger.info(f'[카카오 API] 시도 횟수: {try_cnt}, 응답 상태: {response.status_code}')
            try_cnt += 1

            if response.status_code == 200:  # 성공
                logger.info('[카카오 API] 메시지 전송 성공')
                return response.status_code, self.kakao_tokens
            elif response.status_code == 400:  # Bad Request
                logger.warning('[카카오 API] 잘못된 요청')
                return response.status_code, self.kakao_tokens
            elif response.status_code == 401 and try_cnt <= 2:  # 토큰 만료
                logger.info('[카카오 API] 토큰 갱신 중...')
                try_tokens = self._refresh_token_to_variable()
                try_tokens_exits = 1
            elif response.status_code != 200 and try_cnt >= 3:  # 3회 시도 실패
                logger.error(f'[카카오 API] 메시지 전송 실패 - 상태코드: {response.status_code}')
                return response.status_code, self.kakao_tokens
        
        return response.status_code, self.kakao_tokens

    def save_to_db(self, tokens):
        """토큰 정보를 데이터베이스에 저장"""
        logger.info("[카카오 API] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            strd_dt = time.strftime('%Y%m%d')
            ins_dt = time.strftime('%Y%m%d%H%M%S')
            
            # 기존 데이터 삭제 (중복 방지)
            logger.info(f"[카카오 API] 기존 데이터 삭제 - strd_dt: {strd_dt}")
            session.query(KakaoTalk).filter(KakaoTalk.strd_dt == strd_dt).delete()
            session.commit()
            
            # 새 데이터 저장
            obj = KakaoTalk(
                strd_dt=strd_dt,
                access_token=tokens['access_token'],
                token_type=tokens['token_type'],
                refresh_token=tokens['refresh_token'],
                scope=tokens['scope'],
                upd_dt=tokens.get('updated', ins_dt),
                ins_dt=ins_dt
            )
            session.add(obj)
            session.commit()
            
            logger.info("[카카오 API] 토큰 정보 저장 완료")
            
        except Exception as e:
            session.rollback()
            logger.error(f"[카카오 API] 데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()

    def run_crawl_kakao_talk(self):
        """카카오톡 API 실행"""
        logger.info('--07 [run_crawl_kakao_talk] Start !!')
        
        try:
            # 메시지 내용 구성
            title1 = "NextMuse 알림1"
            title2 = "NextMuse 알림2"
            content1 = "카카오API로 나에게 카톡전송1!!"
            content2 = "카카오API로 나에게 카톡전송2!!"
            talk_title = "NextMuse - 카카오톡 전송"
            
            content = {title1: content1, title2: content2}
            
            # 메시지 전송
            status_code, tokens = self.send_kakao_msg(talk_title=talk_title, content=content)
            
            if status_code == 200:
                # 토큰 정보 저장
                self.save_to_db(tokens)
                logger.info('--07 [run_crawl_kakao_talk] End !!')
                return {"status": "success", "message": "카카오톡 메시지 전송 및 토큰 저장 완료"}
            else:
                logger.warning(f'[카카오 API] 메시지 전송 실패 - 상태코드: {status_code}')
                return {"status": "error", "message": f"메시지 전송 실패 - 상태코드: {status_code}"}
                
        except Exception as e:
            logger.error(f"[카카오 API] 실행 오류: {e}")
            return {"status": "error", "message": f"카카오톡 API 실행 오류: {str(e)}"}

    def run(self):
        """기존 호환성을 위한 run 메서드"""
        return self.run_crawl_kakao_talk()