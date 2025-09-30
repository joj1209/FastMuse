from app.db import SessionLocal
from app.models import NaverFinance
import time
import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class NaverFinanceCrawler:
    def __init__(self):
        self.stock_codes = ["035720", "005930", "379780", "433330", "418660"]
        # 035720 카카오
        # 005930 삼성전자
        # 379780 RISE 미국S&P500
        # 433330 SOL 미국S&P500
        # 418660 TIGER 미국나스닥100레버리지(합성)

    def crawl_stock_data(self, code):
        """네이버 금융에서 주식 데이터 크롤링"""
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        try:
            res = requests.get(url)
            res.raise_for_status()
            bsobj = BeautifulSoup(res.text, "html.parser")
            return bsobj
        except Exception as e:
            logger.error(f"크롤링 오류 - 종목코드 {code}: {e}")
            return None

    def parse_data(self, bsobj):
        """크롤링한 HTML에서 주식 데이터 파싱"""
        try:
            # 현재가
            div_today = bsobj.find("div", {"class": "today"})
            em = div_today.find("em")
            price = em.find("span", {"class": "blind"}).text.replace(",", "")

            # 종목명
            h_company = bsobj.find("div", {"class": "h_company"})
            name = h_company.a.text.strip()
            
            # 종목코드
            div_description = h_company.find("div", {"class": "description"})
            code = div_description.span.text.strip()

            # 거래량
            table_no_info = bsobj.find("table", {"class": "no_info"})
            tds = table_no_info.tr.find_all("td")
            volume = tds[2].find("span", {"class": "blind"}).text.replace(",", "")

            # 전일가
            td_first = bsobj.find("td", {"class": "first"})
            yesterday_price = td_first.find("em").find("span", {"class": "blind"}).text.replace(",", "")

            # 기준일자, 입력일시
            strd_dt = time.strftime('%Y%m%d')
            ins_dt = time.strftime('%Y%m%d%H%M%S')  # 14자리 문자열로 변경

            return {
                "strd_dt": strd_dt,
                "stock_cd": code,
                "stock_nm": name,
                "pre_price": int(yesterday_price),
                "today_price": int(price),
                "trading_volume": int(volume),
                "ins_dt": ins_dt
            }
        except Exception as e:
            logger.error(f"데이터 파싱 오류: {e}")
            return None

    def get_stock_data(self):
        """모든 주식 종목의 데이터를 수집"""
        logger.info("[네이버 크롤링] 주식 데이터 수집 시작")
        results = []

        for code in self.stock_codes:
            logger.info(f"[네이버 크롤링] 종목 {code} 데이터 수집 중...")
            bsobj = self.crawl_stock_data(code)
            
            if bsobj:
                data = self.parse_data(bsobj)
                if data:
                    results.append(data)
                    logger.info(f"[네이버 크롤링] {data['stock_nm']}({code}) 수집 완료")
                else:
                    logger.warning(f"[네이버 크롤링] {code} 파싱 실패")
            
                logger.warning(f"[네이버 크롤링] {code} 크롤링 실패")
            
            time.sleep(0.5)  # 서버 부하 방지

        logger.info(f"[네이버 크롤링] 총 {len(results)}개 종목 데이터 수집 완료")
        return results

    def save_to_db(self, data_list):
        """수집한 데이터를 데이터베이스에 저장"""
        logger.info("[네이버 크롤링] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            # 오늘 날짜 데이터 삭제 (중복 방지)
            if data_list:
                strd_dt_value = data_list[0]['strd_dt']
                logger.info(f"[네이버 크롤링] 기존 데이터 삭제 - strd_dt: {strd_dt_value}")
                session.query(NaverFinance).filter(NaverFinance.strd_dt == strd_dt_value).delete()
                session.commit()

            # 새 데이터 저장
            for data in data_list:
                obj = NaverFinance(
                    strd_dt=data['strd_dt'],
                    stock_cd=data['stock_cd'],
                    stock_nm=data['stock_nm'],
                    pre_price=data['pre_price'],
                    today_price=data['today_price'],
                    trading_volume=data['trading_volume'],
                    ins_dt=data['ins_dt']
                )
                session.add(obj)
                logger.info(f"[네이버 크롤링] DB 저장: {data['stock_nm']}({data['stock_cd']})")

            session.commit()
            logger.info(f"[네이버 크롤링] 총 {len(data_list)}개 데이터 저장 완료")
            
        except Exception as e:
            session.rollback()
            logger.error(f"데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()

    def run(self):
        """네이버 금융 크롤링 실행"""
        try:
            data_list = self.get_stock_data()
            if data_list:
                self.save_to_db(data_list)
                return data_list
            else:
                logger.warning("[네이버 크롤링] 수집된 데이터가 없습니다.")
                return []
        except Exception as e:
            logger.error(f"네이버 금융 크롤링 실행 오류: {e}")
            raise e