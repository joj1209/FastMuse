import pandas as pd
import requests
import time
import json
import logging
from datetime import date, timedelta
from app.db import SessionLocal
from app.models import SeoulForPop
from app.config import settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class SeoulPublicDataCrawler:
    def __init__(self):
        # 설정에서 API 키 가져오기 (없으면 기본값 사용)
        self.apikey = getattr(settings, 'SEOUL_API_KEY', '70517359706a6f6a3731477969764a')
        
        # 자치구단위 서울 생활인구(장기체류 외국인) API 설정
        self.base_url = 'http://openapi.seoul.go.kr:8088'
        self.api_endpoint = 'SPOP_FORN_LONG_RESD_JACHI'
        
        # 날짜 계산
        self.pre_7_dt = (date.today() - timedelta(days=10)).strftime('%Y%m%d')
        self.strd_dt = time.strftime('%Y%m%d')
        self.ins_dt = time.strftime('%Y%m%d%H%M%S')

    def fetch_data_from_api(self, start_num, end_num):
        """서울 공공데이터 API에서 데이터를 가져옵니다"""
        try:
            url = f'{self.base_url}/{self.apikey}/json/{self.api_endpoint}/{start_num}/{end_num}/{self.pre_7_dt}'
            logger.info(f"[서울공공데이터] API 호출: {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            json_data = response.json()
            
            # 응답 데이터 확인
            if self.api_endpoint not in json_data:
                logger.error(f"[서울공공데이터] API 응답에 {self.api_endpoint} 키가 없습니다")
                return None
                
            if 'row' not in json_data[self.api_endpoint]:
                logger.error(f"[서울공공데이터] API 응답에 row 데이터가 없습니다")
                return None
            
            raw_data = json_data[self.api_endpoint]['row']
            logger.info(f"[서울공공데이터] 데이터 {len(raw_data)}건 조회 완료")
            
            return pd.DataFrame(raw_data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"[서울공공데이터] API 호출 오류: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"[서울공공데이터] JSON 파싱 오류: {e}")
            return None
        except Exception as e:
            logger.error(f"[서울공공데이터] 데이터 조회 오류: {e}")
            return None

    def process_data(self, dataframes):
        """데이터를 가공하고 정리합니다"""
        try:
            # 데이터프레임 병합
            data = pd.concat(dataframes, ignore_index=True)
            logger.info(f"[서울공공데이터] 전체 데이터 {len(data)}건 병합 완료")
            
            # 필요한 컬럼 추가
            data.insert(0, 'strd_dt', self.strd_dt)
            data['ins_dt'] = self.ins_dt
            
            # 인덱스 재배열
            df = data.reset_index(drop=True)
            
            # 기존 컬럼 순서에 맞게 정렬 (SeoulForPop 모델 기준)
            # 원본 컬럼들을 기존 모델 필드에 매핑
            if len(df.columns) >= 8:
                df.columns = [
                    'strd_dt',           # 기준일자
                    'stdr_de_id',        # 기준_일_ID
                    'tmzon_pd_se',       # 시간대_코드_구분
                    'adstrd_code_se',    # 자치구_코드_구분
                    'tot_lvpop_co',      # 총_생활인구_수
                    'china_staypop_co',  # 중국_체류인구_수
                    'etc_staypop_co',    # 기타_체류인구_수
                    'ins_dt'             # 입력일시
                ]
            else:
                # 컬럼이 부족한 경우 기본값으로 채움
                required_columns = ['strd_dt', 'stdr_de_id', 'tmzon_pd_se', 'adstrd_code_se', 
                                  'tot_lvpop_co', 'china_staypop_co', 'etc_staypop_co', 'ins_dt']
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = ''
                df = df[required_columns]
            
            logger.info(f"[서울공공데이터] 데이터 가공 완료 - 최종 {len(df)}건")
            return df
            
        except Exception as e:
            logger.error(f"[서울공공데이터] 데이터 가공 오류: {e}")
            return None

    def save_to_db(self, df):
        """가공된 데이터를 데이터베이스에 저장합니다"""
        logger.info("[서울공공데이터] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            # 기존 데이터 삭제 (중복 방지)
            logger.info(f"[서울공공데이터] 기존 데이터 삭제 - strd_dt: {self.strd_dt}")
            session.query(SeoulForPop).filter(
                SeoulForPop.strd_dt == self.strd_dt
            ).delete()
            session.commit()
            
            # DataFrame을 dict 레코드로 변환하여 저장
            records = df.to_dict('records')
            
            for record in records:
                # 정수형 변환 처리
                try:
                    record['tot_lvpop_co'] = int(record.get('tot_lvpop_co', 0) or 0)
                    record['china_staypop_co'] = int(record.get('china_staypop_co', 0) or 0)
                    record['etc_staypop_co'] = int(record.get('etc_staypop_co', 0) or 0)
                except (ValueError, TypeError):
                    # 변환 실패시 0으로 설정
                    record['tot_lvpop_co'] = 0
                    record['china_staypop_co'] = 0
                    record['etc_staypop_co'] = 0
                
                obj = SeoulForPop(**record)
                session.add(obj)
            
            session.commit()
            logger.info(f"[서울공공데이터] 데이터베이스 저장 완료 - {len(records)}건")
            return len(records)
            
        except Exception as e:
            session.rollback()
            logger.error(f"[서울공공데이터] 데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()

    def run_seoul_api_crawler(self):
        """서울 공공데이터 크롤링 메인 실행 함수"""
        logger.info('--[run_seoul_api_crawler] Start !!')
        
        try:
            # API 키 확인
            if not self.apikey:
                error_msg = "서울 API 키가 설정되지 않았습니다. SEOUL_API_KEY 환경변수를 확인하세요."
                logger.error(f"[서울공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            dataframes = []
            total_records = 0
            
            # 1-50, 51-100 구간으로 데이터 조회
            data_ranges = [(1, 50), (51, 100)]
            
            for start_num, end_num in data_ranges:
                logger.info(f"[서울공공데이터] 데이터 조회 중: {start_num}-{end_num}")
                
                df = self.fetch_data_from_api(start_num, end_num)
                
                if df is not None and not df.empty:
                    dataframes.append(df)
                    total_records += len(df)
                    logger.info(f"[서울공공데이터] {start_num}-{end_num} 구간: {len(df)}건 조회")
                else:
                    logger.warning(f"[서울공공데이터] {start_num}-{end_num} 구간: 데이터 없음")
            
            if not dataframes:
                error_msg = "API에서 데이터를 가져올 수 없습니다. API 키나 날짜를 확인하세요."
                logger.error(f"[서울공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 데이터 가공
            processed_df = self.process_data(dataframes)
            
            if processed_df is None:
                error_msg = "데이터 가공 중 오류가 발생했습니다."
                logger.error(f"[서울공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 데이터베이스 저장
            saved_count = self.save_to_db(processed_df)
            
            logger.info('--[run_seoul_api_crawler] End !!')
            
            return {
                "status": "success",
                "message": f"서울 공공데이터 크롤링 완료",
                "total_records": total_records,
                "saved_records": saved_count,
                "api_endpoint": self.api_endpoint,
                "date_range": self.pre_7_dt
            }
            
        except Exception as e:
            logger.error(f"[서울공공데이터] 실행 오류: {e}")
            return {"status": "error", "message": f"서울 공공데이터 크롤링 오류: {str(e)}"}

    def run(self):
        """기존 호환성을 위한 run 메서드"""
        return self.run_seoul_api_crawler()