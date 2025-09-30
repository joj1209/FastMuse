import json
import requests
import pandas as pd
import time
import logging
from urllib.request import urlopen
from datetime import date, timedelta
from app.db import SessionLocal
from app.models import KmaForecast
from app.config import settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class KmaPublicDataCrawler:
    def __init__(self):
        # 설정에서 API 키 가져오기 (없으면 기본값 사용)
        self.api_key = getattr(settings, 'API_KEY', 'nBUFejf4RvmVBXo3-Pb56A')
        
        # 기상청 초단기실황조회 API 설정
        self.base_url = 'https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst'
        
        # 날짜 및 시간 설정
        self.strd_dt = time.strftime('%Y%m%d')
        self.ins_dt = time.strftime('%Y%m%d%H%M%S')
        
        # 기본 좌표 (서울 지역)
        self.nx = 55
        self.ny = 127
        
        logger.info(f"[기상청공공데이터] 기준일자: {self.strd_dt}, 좌표: nx={self.nx}, ny={self.ny}")

    def fetch_data_from_api(self, base_date, base_time='0600', page_no=1, num_rows=10):
        """기상청 공공데이터 API에서 데이터를 가져옵니다"""
        try:
            # API URL 구성
            params = {
                'authKey': self.api_key,
                'dataType': 'JSON',
                'numOfRows': num_rows,
                'pageNo': page_no,
                'base_date': base_date,
                'base_time': base_time,
                'nx': self.nx,
                'ny': self.ny
            }
            
            # URL 조립
            param_str = '&'.join([f"{key}={value}" for key, value in params.items()])
            api_url = f"{self.base_url}?{param_str}"
            
            logger.info(f"[기상청공공데이터] API 호출: {api_url}")
            
            # API 호출
            with urlopen(api_url) as response:
                html_bytes = response.read()
                html = html_bytes.decode('utf-8')
            
            # JSON 파싱
            json_object = json.loads(html)
            
            # 디버깅: 응답 구조 확인
            logger.info(f"[기상청공공데이터] API 응답 키 목록: {list(json_object.keys())}")
            
            # 응답 데이터 확인
            if 'response' not in json_object:
                logger.error(f"[기상청공공데이터] API 응답에 response 키가 없습니다")
                logger.error(f"[기상청공공데이터] 전체 응답: {json_object}")
                return None
            
            response_data = json_object['response']
            
            if 'body' not in response_data or 'items' not in response_data['body']:
                logger.error(f"[기상청공공데이터] API 응답 구조가 예상과 다릅니다")
                logger.error(f"[기상청공공데이터] response 내용: {response_data}")
                return None
            
            items = response_data['body']['items']
            
            if 'item' not in items or not items['item']:
                logger.warning(f"[기상청공공데이터] {base_date} {base_time} 데이터가 없습니다")
                return None
            
            raw_data = items['item']
            logger.info(f"[기상청공공데이터] 데이터 {len(raw_data)}건 조회 완료")
            
            # DataFrame 생성
            df = pd.DataFrame(raw_data)
            
            # 디버깅: 원본 데이터 구조 확인
            if len(df) > 0:
                logger.info(f"[기상청공공데이터] 원본 컬럼명: {list(df.columns)}")
                logger.info(f"[기상청공공데이터] 첫 번째 레코드 샘플: {df.iloc[0].to_dict()}")
            
            return df
            
        except Exception as e:
            logger.error(f"[기상청공공데이터] API 호출 오류: {e}")
            return None

    def process_data(self, dataframes):
        """데이터를 가공하고 정리합니다"""
        try:
            if not dataframes:
                logger.warning("[기상청공공데이터] 가공할 데이터가 없습니다")
                return None
                
            # 데이터프레임 병합
            data = pd.concat(dataframes, ignore_index=True)
            logger.info(f"[기상청공공데이터] 전체 데이터 {len(data)}건 병합 완료")
            
            # 필요한 컬럼 추가
            data.insert(0, 'strd_dt', self.strd_dt)
            data['ins_dt'] = self.ins_dt
            
            # 인덱스 재배열
            df = data.reset_index(drop=True)
            
            # 컬럼명을 KmaForecast 모델에 맞게 정의
            # 기존 컬럼들을 확인하고 매핑
            logger.info(f"[기상청공공데이터] 매핑 전 컬럼: {list(df.columns)}")
            
            # 컬럼 순서 조정 (KmaForecast 모델 기준)
            if len(df.columns) >= 6:
                # 필요한 컬럼만 선택하고 순서 조정
                required_columns = ['strd_dt', 'strd_tm', 'category', 'nx', 'ny', 'obsr_value', 'ins_dt']
                
                # 누락된 컬럼 체크 및 기본값 설정
                for col in required_columns:
                    if col not in df.columns:
                        if col in ['strd_dt', 'ins_dt']:
                            continue  # 이미 추가됨
                        elif col == 'strd_tm':
                            df[col] = '0600'  # 기본 시간
                        else:
                            df[col] = ''
                
                # 필요한 컬럼만 선택
                df = df[required_columns]
                
            else:
                logger.error(f"[기상청공공데이터] 컬럼 수 부족 - 예상: 6+, 실제: {len(df.columns)}")
                return None
            
            # 데이터 타입 변환 및 샘플 로깅
            for record_idx in range(min(3, len(df))):  # 처음 3개 레코드만 로깅
                logger.info(f"[기상청공공데이터] 레코드 {record_idx+1} 샘플: {df.iloc[record_idx].to_dict()}")
            
            logger.info(f"[기상청공공데이터] 데이터 가공 완료 - 최종 {len(df)}건")
            return df
            
        except Exception as e:
            logger.error(f"[기상청공공데이터] 데이터 가공 오류: {e}")
            return None

    def save_to_db(self, df):
        """가공된 데이터를 데이터베이스에 저장합니다"""
        logger.info("[기상청공공데이터] 데이터베이스 저장 시작")
        session = SessionLocal()
        
        try:
            # 기존 데이터 삭제 (중복 방지)
            logger.info(f"[기상청공공데이터] 기존 데이터 삭제 - strd_dt: {self.strd_dt}")
            session.query(KmaForecast).filter(
                KmaForecast.strd_dt == self.strd_dt
            ).delete()
            session.commit()
            
            # DataFrame을 dict 레코드로 변환하여 저장
            records = df.to_dict('records')
            
            logger.info(f"[기상청공공데이터] 저장할 레코드 수: {len(records)}")
            if records:
                logger.info(f"[기상청공공데이터] 첫 번째 레코드 예시: {records[0]}")
            
            for i, record in enumerate(records):
                try:
                    # 필요시 데이터 타입 변환
                    if 'nx' in record:
                        record['nx'] = int(record['nx']) if record['nx'] else 0
                    if 'ny' in record:
                        record['ny'] = int(record['ny']) if record['ny'] else 0
                    
                    if i < 3:  # 처음 3개 레코드만 로깅
                        logger.info(f"[기상청공공데이터] 레코드 {i+1} 저장 데이터: {record}")
                        
                except Exception as e:
                    logger.warning(f"[기상청공공데이터] 레코드 {i+1} 변환 실패: {e}")
                
                obj = KmaForecast(**record)
                session.add(obj)
            
            session.commit()
            logger.info(f"[기상청공공데이터] 데이터베이스 저장 완료 - {len(records)}건")
            return len(records)
            
        except Exception as e:
            session.rollback()
            logger.error(f"[기상청공공데이터] 데이터베이스 저장 오류: {e}")
            raise e
        finally:
            session.close()

    def run_kma_api_crawler(self):
        """기상청 공공데이터 크롤링 메인 실행 함수"""
        logger.info('--[run_kma_api_crawler] Start !!')
        
        try:
            # API 키 확인
            if not self.api_key:
                error_msg = "기상청 API 키가 설정되지 않았습니다. API_KEY 환경변수를 확인하세요."
                logger.error(f"[기상청공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            dataframes = []
            total_records = 0
            
            # 여러 시간대를 시도 (현재 시간 기준으로 역산)
            time_candidates = ['0600', '0700', '0800', '0900', '1000', '1100']
            successful_time = None
            
            for base_time in time_candidates:
                logger.info(f"[기상청공공데이터] {base_time} 시간대 데이터 조회 중...")
                
                df = self.fetch_data_from_api(self.strd_dt, base_time)
                
                if df is not None and not df.empty:
                    dataframes.append(df)
                    total_records += len(df)
                    successful_time = base_time
                    logger.info(f"[기상청공공데이터] {base_time}: {len(df)}건 조회 성공")
                    break  # 첫 번째 성공한 시간대 사용
                else:
                    logger.warning(f"[기상청공공데이터] {base_time}: 데이터 없음")
            
            if not dataframes:
                # 오늘 데이터가 없으면 어제 데이터 시도
                yesterday = (date.today() - timedelta(days=1)).strftime('%Y%m%d')
                logger.info(f"[기상청공공데이터] 어제({yesterday}) 데이터로 재시도...")
                
                for base_time in time_candidates:
                    df = self.fetch_data_from_api(yesterday, base_time)
                    if df is not None and not df.empty:
                        dataframes.append(df)
                        total_records += len(df)
                        successful_time = base_time
                        logger.info(f"[기상청공공데이터] 어제 {base_time}: {len(df)}건 조회 성공")
                        break
            
            if not dataframes:
                error_msg = "API에서 데이터를 가져올 수 없습니다. API 키나 시간을 확인하세요."
                logger.error(f"[기상청공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 데이터 가공
            processed_df = self.process_data(dataframes)
            
            if processed_df is None:
                error_msg = "데이터 가공 중 오류가 발생했습니다."
                logger.error(f"[기상청공공데이터] {error_msg}")
                return {"status": "error", "message": error_msg}
            
            # 데이터베이스 저장
            saved_count = self.save_to_db(processed_df)
            
            logger.info('--[run_kma_api_crawler] End !!')
            
            return {
                "status": "success",
                "message": f"기상청 공공데이터 크롤링 완료",
                "total_records": total_records,
                "saved_records": saved_count,
                "base_time": successful_time,
                "coordinates": f"nx={self.nx}, ny={self.ny}"
            }
            
        except Exception as e:
            logger.error(f"[기상청공공데이터] 실행 오류: {e}")
            return {"status": "error", "message": f"기상청 공공데이터 크롤링 오류: {str(e)}"}

    def run(self):
        """기존 호환성을 위한 run 메서드"""
        return self.run_kma_api_crawler()