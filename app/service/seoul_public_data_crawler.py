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
        
        # 날짜 계산 - 더 넓은 범위로 시도 (1년 전까지)
        self.today_dt = date.today().strftime('%Y%m%d')  # 오늘 날짜
        self.pre_1_dt = (date.today() - timedelta(days=1)).strftime('%Y%m%d')  # 1일 전
        self.pre_7_dt = (date.today() - timedelta(days=7)).strftime('%Y%m%d')  # 7일 전
        self.pre_30_dt = (date.today() - timedelta(days=30)).strftime('%Y%m%d')  # 30일 전
        self.pre_90_dt = (date.today() - timedelta(days=90)).strftime('%Y%m%d')  # 90일 전
        self.pre_365_dt = (date.today() - timedelta(days=365)).strftime('%Y%m%d')  # 1년 전
        
        self.strd_dt = time.strftime('%Y%m%d')
        self.ins_dt = time.strftime('%Y%m%d%H%M%S')
        
        logger.info(f"[서울공공데이터] 사용할 날짜들: 오늘={self.today_dt}, 1일전={self.pre_1_dt}, 7일전={self.pre_7_dt}, 30일전={self.pre_30_dt}, 90일전={self.pre_90_dt}, 1년전={self.pre_365_dt}")

    def fetch_data_from_api(self, start_num, end_num, date_str=None):
        """서울 공공데이터 API에서 데이터를 가져옵니다"""
        try:
            # 날짜가 지정되지 않으면 기본값 사용
            if date_str is None:
                date_str = self.pre_7_dt
                
            url = f'{self.base_url}/{self.apikey}/json/{self.api_endpoint}/{start_num}/{end_num}/{date_str}'
            logger.info(f"[서울공공데이터] API 호출: {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            json_data = response.json()
            
            # 디버깅: 실제 응답 구조 확인
            logger.info(f"[서울공공데이터] API 응답 키 목록: {list(json_data.keys())}")
            logger.info(f"[서울공공데이터] 전체 응답 내용: {json_data}")
            
            # 응답 데이터 확인
            if self.api_endpoint not in json_data:
                logger.error(f"[서울공공데이터] API 응답에 {self.api_endpoint} 키가 없습니다")
                logger.error(f"[서울공공데이터] 사용 가능한 키: {list(json_data.keys())}")
                return None
                
            if 'row' not in json_data[self.api_endpoint]:
                logger.error(f"[서울공공데이터] API 응답에 row 데이터가 없습니다")
                logger.error(f"[서울공공데이터] {self.api_endpoint} 내용: {json_data[self.api_endpoint]}")
                return None
            
            raw_data = json_data[self.api_endpoint]['row']
            logger.info(f"[서울공공데이터] 데이터 {len(raw_data)}건 조회 완료")
            
            # 디버깅: 실제 데이터 구조 확인
            if raw_data:
                logger.info(f"[서울공공데이터] 첫 번째 레코드 키: {list(raw_data[0].keys())}")
                logger.info(f"[서울공공데이터] 첫 번째 레코드 샘플: {raw_data[0]}")
            
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
            
            # 디버깅: 원본 데이터 컬럼명 확인
            logger.info(f"[서울공공데이터] 원본 컬럼명: {list(data.columns)}")
            logger.info(f"[서울공공데이터] 첫 번째 레코드 샘플: {data.iloc[0].to_dict() if len(data) > 0 else 'No data'}")
            
            # 컬럼명을 소문자로 변환 (API 응답이 대문자로 오는 경우 처리)
            data.columns = [col.lower() for col in data.columns]
            logger.info(f"[서울공공데이터] 소문자 변환 후 컬럼명: {list(data.columns)}")
            
            # 필요한 컬럼 추가
            data.insert(0, 'strd_dt', self.strd_dt)
            data['ins_dt'] = self.ins_dt
            
            # 인덱스 재배열
            df = data.reset_index(drop=True)
            
            # 실제 API 응답 컬럼명에 맞춰서 매핑 (임의 재배열 제거)
            # 컬럼명을 직접 확인하여 매핑하도록 수정
            logger.info(f"[서울공공데이터] 매핑 전 전체 컬럼: {list(df.columns)}")
            
            # SeoulForPop 모델에 맞는 컬럼명으로 매핑
            column_mapping = {
                'strd_dt': 'strd_dt',           # 기준일자 (추가된 컬럼)
                'ins_dt': 'ins_dt'              # 입력일시 (추가된 컬럼)
            }
            
            # API 응답 컬럼명을 모델 컬럼명으로 매핑 (실제 컬럼명 확인 후 매핑)
            for col in df.columns:
                if col not in ['strd_dt', 'ins_dt']:  # 추가한 컬럼 제외
                    logger.info(f"[서울공공데이터] 원본 컬럼 '{col}' 값 샘플: {df[col].iloc[0] if len(df) > 0 else 'No data'}")
            
            # 필수 컬럼이 있는지 확인하고 없으면 기본값으로 채움
            required_columns = ['strd_dt', 'stdr_de_id', 'tmzon_pd_se', 'adstrd_code_se', 
                              'tot_lvpop_co', 'china_staypop_co', 'etc_staypop_co', 'ins_dt']
            
            for col in required_columns:
                if col not in df.columns:
                    logger.warning(f"[서울공공데이터] 필수 컬럼 '{col}' 없음 - 기본값으로 설정")
                    df[col] = '' if col in ['stdr_de_id', 'tmzon_pd_se', 'adstrd_code_se'] else 0
            
            # 필요한 컬럼만 선택
            df = df[required_columns]
            
            logger.info(f"[서울공공데이터] 데이터 가공 완료 - 최종 {len(df)}건")
            logger.info(f"[서울공공데이터] 최종 컬럼명: {list(df.columns)}")
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
            
            logger.info(f"[서울공공데이터] 저장할 레코드 수: {len(records)}")
            if records:
                logger.info(f"[서울공공데이터] 첫 번째 레코드 예시: {records[0]}")
            
            for i, record in enumerate(records):
                # 디버깅: 각 레코드의 주요 값 확인
                if i < 3:  # 처음 3개 레코드만 로깅
                    logger.info(f"[서울공공데이터] 레코드 {i+1} 원본: {record}")
                
                # 정수형 변환 처리 - 실제 컬럼명 확인 후 수정 필요
                try:
                    # 기존 방식 유지하되 값이 있는지 확인
                    tot_val = record.get('tot_lvpop_co', 0)
                    china_val = record.get('china_staypop_co', 0)
                    etc_val = record.get('etc_staypop_co', 0)
                    
                    logger.info(f"[서울공공데이터] 레코드 {i+1} 변환 전 값: tot={tot_val}, china={china_val}, etc={etc_val}")
                    
                    record['tot_lvpop_co'] = int(tot_val or 0)
                    record['china_staypop_co'] = int(china_val or 0)
                    record['etc_staypop_co'] = int(etc_val or 0)
                    
                    if i < 3:
                        logger.info(f"[서울공공데이터] 레코드 {i+1} 변환 후: tot={record['tot_lvpop_co']}, china={record['china_staypop_co']}, etc={record['etc_staypop_co']}")
                        
                except (ValueError, TypeError) as e:
                    logger.warning(f"[서울공공데이터] 레코드 {i+1} 변환 실패: {e}")
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
            
            # 여러 날짜를 시도해보기 - 1년 전까지 확장
            date_candidates = [
                self.today_dt, 
                self.pre_1_dt, 
                self.pre_7_dt, 
                self.pre_30_dt, 
                self.pre_90_dt, 
                self.pre_365_dt
            ]
            successful_date = None
            
            for test_date in date_candidates:
                logger.info(f"[서울공공데이터] 날짜 {test_date}로 API 테스트 중...")
                
                # 1-5 구간으로 작은 범위 테스트
                test_url = f'{self.base_url}/{self.apikey}/json/{self.api_endpoint}/1/5/{test_date}'
                logger.info(f"[서울공공데이터] 테스트 URL: {test_url}")
                
                try:
                    test_response = requests.get(test_url, timeout=30)
                    test_response.raise_for_status()
                    test_json = test_response.json()
                    
                    logger.info(f"[서울공공데이터] 날짜 {test_date} 응답: {test_json}")
                    
                    if self.api_endpoint in test_json and 'row' in test_json[self.api_endpoint]:
                        successful_date = test_date
                        logger.info(f"[서울공공데이터] 성공! 사용할 날짜: {successful_date}")
                        break
                    else:
                        logger.warning(f"[서울공공데이터] 날짜 {test_date}: 유효한 데이터 없음")
                        
                except Exception as e:
                    logger.warning(f"[서울공공데이터] 날짜 {test_date} 테스트 실패: {e}")
                    continue
            
            if not successful_date:
                error_msg = f"모든 날짜({date_candidates})에서 데이터를 찾을 수 없습니다. API 키나 서비스를 확인하세요."
                logger.error(f"[서울공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 성공한 날짜로 실제 데이터 수집
            self.pre_7_dt = successful_date  # 성공한 날짜로 업데이트
            
            # 1-50, 51-100 구간으로 데이터 조회
            data_ranges = [(1, 50), (51, 100)]
            
            for start_num, end_num in data_ranges:
                logger.info(f"[서울공공데이터] 데이터 조회 중: {start_num}-{end_num} (날짜: {successful_date})")
                
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
                "date_range": successful_date
            }
            
        except Exception as e:
            logger.error(f"[서울공공데이터] 실행 오류: {e}")
            return {"status": "error", "message": f"서울 공공데이터 크롤링 오류: {str(e)}"}

    def run(self):
        """기존 호환성을 위한 run 메서드"""
        return self.run_seoul_api_crawler()