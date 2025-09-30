import pandas as pd
import requests
import json
import urllib
import time
import logging
from urllib.parse import quote
from datetime import date, timedelta
from app.db import SessionLocal
from app.models import JejuFloPop
from app.config import settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class JejuPublicDataCrawler:
    def __init__(self):
        # 설정에서 API 키 가져오기 (없으면 기본값 사용)
        self.apikey = getattr(settings, 'JEJU_API_KEY', 't_0pcpt_22ejt0p0p2b0o_r12j5e08_t')
        
        # 제주 유동인구 API 설정
        self.base_url = 'https://open.jejudatahub.net/api/proxy/Daaa1t3at3tt8a8DD3t55538t35Dab1t'
        
        # 날짜 계산
        self.pre_90_dt = (date.today() - timedelta(days=90)).strftime('%Y%m%d')
        self.pre_150_dt = (date.today() - timedelta(days=150)).strftime('%Y%m%d')
        self.strd_dt = time.strftime('%Y%m%d')
        self.ins_dt = time.strftime('%Y%m%d%H%M%S')
        
        logger.info(f"[제주공공데이터] 사용할 날짜 범위: {self.pre_150_dt} ~ {self.pre_90_dt}")

    def fetch_data_from_api(self, start_date, end_date, emd_name='아라동'):
        """제주 공공데이터 API에서 데이터를 가져옵니다"""
        try:
            # 읍면동 이름을 URL 인코딩
            query = quote(emd_name)
            
            # API URL 구성
            api_url = f"{self.base_url}/{self.apikey}?startDate={start_date}&endDate={end_date}&emd={query}"
            logger.info(f"[제주공공데이터] API 호출: {api_url}")
            
            # API 호출
            weburl = urllib.request.urlopen(api_url)
            data = weburl.read()
            
            # JSON 파싱
            contents = json.loads(data)
            
            # 디버깅: 응답 구조 확인
            logger.info(f"[제주공공데이터] API 응답 키 목록: {list(contents.keys())}")
            logger.info(f"[제주공공데이터] 전체 응답 내용: {contents}")
            
            # 데이터 추출
            if 'data' not in contents:
                logger.error(f"[제주공공데이터] API 응답에 data 키가 없습니다")
                logger.error(f"[제주공공데이터] 사용 가능한 키: {list(contents.keys())}")
                return None
                
            row_data = contents['data']
            
            if not row_data:
                logger.warning(f"[제주공공데이터] {emd_name}({start_date}~{end_date}) 데이터가 없습니다")
                return None
            
            logger.info(f"[제주공공데이터] {emd_name} 지역 데이터 {len(row_data)}건 조회 완료")
            
            # DataFrame 생성
            df = pd.DataFrame(row_data)
            
            # 디버깅: 원본 데이터 구조 확인
            if len(df) > 0:
                logger.info(f"[제주공공데이터] 원본 컬럼명: {list(df.columns)}")
                logger.info(f"[제주공공데이터] 첫 번째 레코드 샘플: {df.iloc[0].to_dict()}")
            
            return df
            
        except Exception as e:
            logger.error(f"[제주공공데이터] API 호출 오류: {e}")
            return None

    def process_data(self, dataframes):
        """데이터를 가공하고 정리합니다"""
        try:
            if not dataframes:
                logger.warning("[제주공공데이터] 가공할 데이터가 없습니다")
                return None
                
            # 데이터프레임 병합
            data = pd.concat(dataframes, ignore_index=True)
            logger.info(f"[제주공공데이터] 전체 데이터 {len(data)}건 병합 완료")
            
            # 필요한 컬럼 추가
            data.insert(0, 'strd_dt', self.strd_dt)
            data['ins_dt'] = self.ins_dt
            
            # 인덱스 재배열
            df = data.reset_index(drop=True)
            
            # 컬럼명을 JejuFloPop 모델에 맞게 정의
            expected_columns = ['strd_dt', 'regist_dt', 'city', 'emd', 'gender', 'age_group', 'resd_pop', 'work_pop', 'visit_pop', 'ins_dt']
            
            if len(df.columns) == len(expected_columns):
                df.columns = expected_columns
                logger.info(f"[제주공공데이터] 컬럼명 매핑 완료: {list(df.columns)}")
            else:
                logger.error(f"[제주공공데이터] 컬럼 수 불일치 - 예상: {len(expected_columns)}, 실제: {len(df.columns)}")
                logger.error(f"[제주공공데이터] 실제 컬럼: {list(df.columns)}")
                return None
            
            # 데이터 타입 변환
            for record_idx in range(min(3, len(df))):  # 처음 3개 레코드만 로깅
                logger.info(f"[제주공공데이터] 레코드 {record_idx+1} 샘플: {df.iloc[record_idx].to_dict()}")
            
            logger.info(f"[제주공공데이터] 데이터 가공 완료 - 최종 {len(df)}건")
            return df
            
        except Exception as e:
            logger.error(f"[제주공공데이터] 데이터 가공 오류: {e}")
            return None

    def save_to_db(self, df):
        """가공된 데이터를 데이터베이스에 저장합니다"""
        logger.info("[제주공공데이터] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            # 기존 데이터 삭제 (중복 방지)
            logger.info(f"[제주공공데이터] 기존 데이터 삭제 - strd_dt: {self.strd_dt}")
            session.query(JejuFloPop).filter(
                JejuFloPop.strd_dt == self.strd_dt
            ).delete()
            session.commit()
            
            # DataFrame을 dict 레코드로 변환하여 저장
            records = df.to_dict('records')
            
            logger.info(f"[제주공공데이터] 저장할 레코드 수: {len(records)}")
            if records:
                logger.info(f"[제주공공데이터] 첫 번째 레코드 예시: {records[0]}")
            
            for i, record in enumerate(records):
                # 정수형 변환 처리
                try:
                    # 인구 수 관련 컬럼을 정수로 변환
                    def safe_int_convert(value):
                        if value is None or value == '':
                            return 0
                        try:
                            if isinstance(value, str):
                                return int(float(value))
                            else:
                                return int(value)
                        except (ValueError, TypeError):
                            return 0
                    
                    record['resd_pop'] = safe_int_convert(record.get('resd_pop', 0))
                    record['work_pop'] = safe_int_convert(record.get('work_pop', 0))
                    record['visit_pop'] = safe_int_convert(record.get('visit_pop', 0))
                    
                    if i < 3:  # 처음 3개 레코드만 로깅
                        logger.info(f"[제주공공데이터] 레코드 {i+1} 변환 후: resd={record['resd_pop']}, work={record['work_pop']}, visit={record['visit_pop']}")
                        
                except Exception as e:
                    logger.warning(f"[제주공공데이터] 레코드 {i+1} 변환 실패: {e}")
                    record['resd_pop'] = 0
                    record['work_pop'] = 0
                    record['visit_pop'] = 0
                
                obj = JejuFloPop(**record)
                session.add(obj)
            
            session.commit()
            logger.info(f"[제주공공데이터] 데이터베이스 저장 완료 - {len(records)}건")
            return len(records)
            
        except Exception as e:
            session.rollback()
            logger.error(f"[제주공공데이터] 데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()

    def run_jeju_api_crawler(self):
        """제주 공공데이터 크롤링 메인 실행 함수"""
        logger.info('--[run_jeju_api_crawler] Start !!')
        
        try:
            # API 키 확인
            if not self.apikey:
                error_msg = "제주 API 키가 설정되지 않았습니다. JEJU_API_KEY 환경변수를 확인하세요."
                logger.error(f"[제주공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            dataframes = []
            total_records = 0
            
            # 여러 읍면동 지역을 순회 (예시: 아라동, 연동, 화북동, 삼양동, 노형동, 애월읍)
            emd_list = ['아라동', '연동', '화북동', '삼양동', '노형동', '애월읍']
            
            for emd_name in emd_list:
                logger.info(f"[제주공공데이터] {emd_name} 지역 데이터 조회 중...")
                
                df = self.fetch_data_from_api(self.pre_150_dt, self.pre_90_dt, emd_name)
                
                if df is not None and not df.empty:
                    dataframes.append(df)
                    total_records += len(df)
                    logger.info(f"[제주공공데이터] {emd_name}: {len(df)}건 조회")
                else:
                    logger.warning(f"[제주공공데이터] {emd_name}: 데이터 없음")
            
            if not dataframes:
                error_msg = "API에서 데이터를 가져올 수 없습니다. API 키나 날짜를 확인하세요."
                logger.error(f"[제주공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 데이터 가공
            processed_df = self.process_data(dataframes)
            
            if processed_df is None:
                error_msg = "데이터 가공 중 오류가 발생했습니다."
                logger.error(f"[제주공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 데이터베이스 저장
            saved_count = self.save_to_db(processed_df)
            
            logger.info('--[run_jeju_api_crawler] End !!')
            
            return {
                "status": "success",
                "message": f"제주 공공데이터 크롤링 완료",
                "total_records": total_records,
                "saved_records": saved_count,
                "date_range": f"{self.pre_150_dt} ~ {self.pre_90_dt}",
                "regions": emd_list
            }
            
        except Exception as e:
            logger.error(f"[제주공공데이터] 실행 오류: {e}")
            return {"status": "error", "message": f"제주 공공데이터 크롤링 오류: {str(e)}"}

    def run(self):
        """기존 호환성을 위한 run 메서드"""
        return self.run_jeju_api_crawler()