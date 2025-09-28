import pandas as pd
import requests
import json
import time
from urllib.parse import quote
from datetime import date, timedelta, datetime
from app.common import config
from app.common.common_func import save_data
from app.common.logger import get_logger
# from dbms.models.api_models import JejuApiFloatingPopulation

logger = get_logger(__name__)

def api_call(start_dt: str, end_dt: str, emd_list: list[str]):
    """
    제주 데이터 허브 유동인구 API를 호출하고 데이터를 DataFrame으로 반환합니다.
    """
    api_key = config.YOUR_APPKEY
    if not api_key:
        logger.error("Jeju API key(YOUR_APPKEY)가 .env 파일에 설정되지 않았습니다.")
        raise ValueError("Jeju API key가 설정되지 않았습니다.")

    all_rows = []
    strd_dt = datetime.today().strftime('%Y%m%d')
    ins_dt = datetime.now().strftime('%Y%m%d%H%M%S')
    base_url = "https://open.jejudatahub.net/api/proxy/Daaa1t3at3tt8a8DD3t55538t35Dab1t"

    for emd in emd_list:
        try:
            query = quote(emd)
            api_url = f"{base_url}/{api_key}?startDate={start_dt}&endDate={end_dt}&emd={query}"
            logger.info(f"Calling Jeju API for emd='{emd}', date='{start_dt}~{end_dt}'")

            response = requests.get(api_url, timeout=30)
            response.raise_for_status()  # 200 OK가 아니면 예외 발생

            contents = response.json()
            row_data = contents.get('data')

            if not row_data:
                logger.warning(f"'{emd}'에 대한 데이터가 없습니다.")
                continue

            # 각 row에 strd_dt와 ins_dt 추가
            for row in row_data:
                row['strd_dt'] = strd_dt
                row['ins_dt'] = ins_dt
            all_rows.extend(row_data)

        except requests.exceptions.RequestException as e:
            logger.error(f"API 호출 중 네트워크 오류 발생 (emd: {emd}): {e}")
            continue
        except Exception as e:
            logger.error(f"API 호출 중 오류 발생 (emd: {emd}): {e}")
            continue

    if not all_rows:
        return pd.DataFrame()

    df_total = pd.DataFrame(all_rows)
    return df_total

def run_api_jeju_floating_population():
    logger.info('--10 [run_api_jeju_floating_population] Start !!')
    # 예시: 90일 전부터 오늘까지의 '아라동', '연동' 데이터 수집
    start_dt = (date.today() - timedelta(days=90)).strftime('%Y%m%d')
    end_dt = date.today().strftime('%Y%m%d')
    target_emds = ['아라동', '연동', '노형동']
    df = api_call(start_dt, end_dt, target_emds)
    if not df.empty:
        print(df)
        # save_data(df.to_dict('records'), JejuApiFloatingPopulation)
    logger.info('--10 [run_api_jeju_floating_population] End !!')