from app.db import SessionLocal
from app.models import MarketTop
import pandas as pd
import time
import datetime
from datetime import date, timedelta


class FinanceDataReaderParser():
    def save_to_dbms_market_stock(self, df):
        print("[DB 적재] 함수 진입: save_to_dbms_market_stock 호출됨")
        session = SessionLocal()
        try:
            # DataFrame에서 strd_dt 값 추출 (모든 row가 동일한 strd_dt라고 가정)
            if not df.empty:
                strd_dt_value = df.iloc[0]['strd_dt']
                print(f"[DB 적재] 삭제 대상 strd_dt: {strd_dt_value}")
                # 기존 데이터 삭제
                session.query(MarketTop).filter(MarketTop.strd_dt == strd_dt_value).delete()
                session.commit()
            # 새 데이터 적재
            for _, row in df.iterrows():
                print(f"[DB 적재] 저장 row: {row.to_dict()}")
                obj = MarketTop(
                    strd_dt=row['strd_dt'],
                    market=row['market'],
                    stock_day=row['stock_day'],
                    opening_price=row['opening_price'],
                    high_price=row['high_price'],
                    low_price=row['low_price'],
                    closing_price=row['closing_price'],
                    volume=row['volume'],
                    ins_dt=row['ins_dt']
                )
                session.add(obj)
            session.commit()
        finally:
            session.close()

    def get_data(self):
        # import inside the function so import-time failures don't crash the app
        import FinanceDataReader as fdr

        # 영업일 찾기 함수
        def get_business_date(target_date, days_back=0):
            current_date = target_date - timedelta(days=days_back)
            # 주말인 경우 금요일로 조정
            while current_date.weekday() >= 5:  # 5=토요일, 6=일요일
                current_date -= timedelta(days=1)
            return current_date.strftime('%Y%m%d')

        today = date.today()
        strd_dt = time.strftime('%Y%m%d')
        ins_dt = time.strftime('%Y%m%d%H%M%S')
        
        # 한국 시장용 영업일 (오늘이 주말이면 금요일)
        kr_business_date = get_business_date(today)
        # 미국 시장용 영업일 (어제가 주말이면 그 전 금요일)
        us_business_date = get_business_date(today, 1)

        # Helper to normalize a dataframe into a standard schema
        def normalize(df_src, market_name):
            df_local = df_src.copy()
            df_local = df_local.reset_index()
            if 'Date' in df_local.columns:
                date_col = 'Date'
            else:
                date_col = df_local.columns[0]
            standardized = pd.DataFrame()
            standardized['stock_day'] = df_local[date_col]
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in df_local.columns:
                    standardized[col] = df_local[col]
                else:
                    standardized[col] = pd.NA
            standardized.insert(0, 'market', market_name)
            standardized.insert(0, 'strd_dt', strd_dt)
            return standardized

        # 각 데이터 소스를 개별적으로 처리하여 오류 시 이전 영업일 시도
        def safe_data_reader(symbol, start_date, end_date=None, market_name='', max_retry=5):
            for i in range(max_retry):
                try:
                    current_date = datetime.datetime.strptime(start_date, '%Y%m%d').date()
                    # i일 전 영업일로 조정
                    retry_date = get_business_date(current_date, i)
                    
                    if end_date is None:
                        df = fdr.DataReader(symbol, retry_date)
                    else:
                        retry_end_date = retry_date  # 한국 시장은 시작일=종료일
                        df = fdr.DataReader(symbol, retry_date, retry_end_date)
                    
                    if not df.empty:
                        print(f"[성공] {market_name}({symbol}) 데이터 조회 성공 - 날짜: {retry_date}")
                        return normalize(df, market_name)
                except Exception as e:
                    print(f"[재시도 {i+1}/{max_retry}] {market_name}({symbol}) 데이터 조회 실패 ({retry_date}): {e}")
                    continue
            
            # 모든 재시도 실패 시 빈 DataFrame 반환
            print(f"[실패] {market_name}({symbol}) 모든 재시도 실패")
            empty_df = pd.DataFrame(columns=['strd_dt', 'market', 'stock_day', 'Open', 'High', 'Low', 'Close', 'Volume'])
            empty_df['strd_dt'] = strd_dt
            empty_df['market'] = market_name
            return empty_df

        df_kospi = safe_data_reader('KS11', kr_business_date, kr_business_date, 'KOSPI')
        df_kosdaq = safe_data_reader('KQ11', kr_business_date, kr_business_date, 'KOSDAQ')
        df_nasdaq = safe_data_reader('IXIC', us_business_date, None, 'NASDAQ')
        df_sp500 = safe_data_reader('S&P500', us_business_date, None, 'S&P500')
        df_dji = safe_data_reader('DJI', us_business_date, None, 'DowJones')

        dfs = [df_kospi, df_kosdaq, df_nasdaq, df_sp500, df_dji]
        dfs = [df for df in dfs if not df.empty and not df.isna().all().all()]
        df_total = pd.concat(dfs, ignore_index=True)
        df_total.replace({pd.NaT: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, inplace=True)
        df_total = df_total.infer_objects(copy=False)

        rename_map = {
            'Open': 'opening_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'closing_price',
            'Volume': 'volume'
        }
        df_total = df_total.rename(columns=rename_map)
        expected_cols = ['strd_dt', 'market', 'stock_day', 'opening_price', 'high_price', 'low_price', 'closing_price', 'volume']
        for c in expected_cols:
            if c not in df_total.columns:
                df_total[c] = pd.NA
        df = df_total[expected_cols].copy()
        df['ins_dt'] = ins_dt
        self.df = df
        return self.df
        # ...existing code...
