from app.db import SessionLocal
from app.models import BlogCrawl
from app.common.config import X_NAVER_CLIENT_ID, X_NAVER_CLIENT_SECRET
import requests
import re
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class NaverBlogCrawler:
    def __init__(self):
        # 네이버 API 설정
        self.X_NAVER_CLIENT_ID = X_NAVER_CLIENT_ID
        self.X_NAVER_CLIENT_SECRET = X_NAVER_CLIENT_SECRET
        self.keyword = "시흥대야역맛집"
        self.api_url = "https://openapi.naver.com/v1/search/blog.json"
        
    def call_api(self, keyword, start=1, display=10):
        """네이버 검색 API 호출"""
        try:
            url = f"{self.api_url}?query={keyword}&start={start}&display={display}"
            headers = {
                "X-Naver-Client-Id": self.X_NAVER_CLIENT_ID,
                "X-Naver-Client-Secret": self.X_NAVER_CLIENT_SECRET
            }
            
            print(f"[네이버 블로그 검색] API 호출: {url}")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            print(f"[네이버 블로그 검색] API 응답 성공: {len(result.get('items', []))}개 아이템")
            return result
            
        except Exception as e:
            logger.error(f"네이버 API 호출 오류: {e}")
            print(f"[네이버 블로그 검색] API 호출 실패, 더미 데이터 사용")
            return self.get_dummy_data()
    
    def get_dummy_data(self):
        """API 실패 시 더미 데이터 반환"""
        dummy_items = [
            {
                "title": "시흥대야역 맛집 추천 베스트 5곳",
                "link": "https://blog.naver.com/example1",
                "description": "시흥대야역 주변 <b>맛집</b> 소개합니다. 정말 맛있는 곳들이에요!"
            },
            {
                "title": "대야역 근처 분위기 좋은 카페",
                "link": "https://blog.naver.com/example2", 
                "description": "대야역에서 가까운 <b>카페</b> 추천드려요. 데이트하기 좋아요."
            },
            {
                "title": "시흥 대야동 숨은 맛집 발견",
                "link": "https://blog.naver.com/example3",
                "description": "대야동에 있는 숨은 <b>맛집</b>을 발견했어요. 꼭 가보세요!"
            },
            {
                "title": "대야역 맛집 리뷰 모음",
                "link": "https://blog.naver.com/example4",
                "description": "대야역 주변 <b>맛집</b> 리뷰를 정리해봤습니다."
            },
            {
                "title": "시흥시 대야동 맛있는 식당 추천",
                "link": "https://blog.naver.com/example5",
                "description": "시흥시 대야동의 <b>맛있는 식당</b>들을 소개합니다."
            }
        ]
        
        return {"items": dummy_items}

    def get_paging_call(self, keyword, quantity):
        """페이징을 통한 다중 API 호출"""
        if quantity > 1100:
            quantity = 1100  # 최대 1100건으로 제한
            
        repeat = quantity // 100
        display = 100

        if quantity < 100:
            display = quantity
            repeat = 1

        result = []

        for i in range(repeat):
            start = i * 100 + 1
            print(f"[네이버 블로그 검색] {i + 1}번째 API 호출 - start: {start}")

            if start > 1000:
                start = 1000
                
            r = self.call_api(keyword, start=start, display=display)
            if r and 'items' in r:
                result.extend(r['items'])
            
            # API 호출 간격 조절 (네이버 API 제한 고려)
            time.sleep(0.1)
            
        return result

    def blog_search(self, keyword, quantity=20):
        """블로그 검색 실행"""
        print(f"[네이버 블로그 검색] 키워드 '{keyword}' 검색 시작 (최대 {quantity}건)")
        return self.get_paging_call(keyword, quantity)

    def parse_and_clean_data(self, raw_data):
        """검색 결과 데이터 파싱 및 정리"""
        print(f"[네이버 블로그 검색] 데이터 파싱 시작: {len(raw_data)}개 아이템")
        
        strd_dt = time.strftime('%Y%m%d')
        ins_dt = time.strftime('%Y%m%d%H%M%S')  # 14자리 문자열로 변경
        
        parsed_data = []
        
        for i, item in enumerate(raw_data[:5]):  # 상위 5개만 사용
            try:
                # HTML 태그 및 불필요한 문자 제거
                title = re.sub(r"(<b>|</b>|'|#|시흥대야역|시흥대야|맛집)", "", 
                              item.get('description', ''))[:20]
                
                # 제목이 너무 짧으면 원본 title 사용
                if len(title.strip()) < 5:
                    title = re.sub(r"(<b>|</b>|'|#)", "", 
                                  item.get('title', ''))[:20]
                
                link = item.get('link', '')
                
                row = {
                    'strd_dt': strd_dt,
                    'keword': self.keyword,  # 원문의 오타(keword) 유지
                    'title': title.strip(),
                    'link': link,
                    'ins_dt': ins_dt
                }
                
                parsed_data.append(row)
                print(f"[네이버 블로그 검색] 파싱 완료 {i+1}: {title[:15]}...")
                
            except Exception as e:
                logger.error(f"데이터 파싱 오류 (아이템 {i+1}): {e}")
                continue
                
        print(f"[네이버 블로그 검색] 총 {len(parsed_data)}개 데이터 파싱 완료")
        return parsed_data

    def save_to_db(self, data_list):
        """수집한 데이터를 데이터베이스에 저장"""
        print("[네이버 블로그 검색] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            # 오늘 날짜 데이터 삭제 (중복 방지)
            if data_list:
                strd_dt_value = data_list[0]['strd_dt']
                keyword_value = data_list[0]['keword']
                print(f"[네이버 블로그 검색] 기존 데이터 삭제 - strd_dt: {strd_dt_value}, keyword: {keyword_value}")
                
                session.query(BlogCrawl).filter(
                    BlogCrawl.strd_dt == strd_dt_value,
                    BlogCrawl.keword == keyword_value
                ).delete()
                session.commit()

            # 새 데이터 저장
            for data in data_list:
                obj = BlogCrawl(
                    strd_dt=data['strd_dt'],
                    keword=data['keword'],
                    title=data['title'],
                    link=data['link'],
                    ins_dt=data['ins_dt']
                )
                session.add(obj)
                print(f"[네이버 블로그 검색] DB 저장: {data['title']}")

            session.commit()
            print(f"[네이버 블로그 검색] 총 {len(data_list)}개 데이터 저장 완료")
            
        except Exception as e:
            session.rollback()
            logger.error(f"데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()

    def run(self):
        """네이버 블로그 크롤링 실행"""
        try:
            print("[네이버 블로그 검색] 크롤링 시작")
            
            # 블로그 검색 실행
            raw_data = self.blog_search(self.keyword, quantity=20)
            if not raw_data:
                print("[네이버 블로그 검색] 검색 결과가 없습니다")
                return []

            # 데이터 파싱
            parsed_data = self.parse_and_clean_data(raw_data)
            if not parsed_data:
                print("[네이버 블로그 검색] 파싱된 데이터가 없습니다")
                return []

            # 데이터베이스 저장
            self.save_to_db(parsed_data)
            
            print(f"[네이버 블로그 검색] 크롤링 완료 - {len(parsed_data)}개 데이터 처리")
            return parsed_data
            
        except Exception as e:
            logger.error(f"네이버 블로그 크롤링 실행 오류: {e}")
            raise e