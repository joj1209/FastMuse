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
        try:
            import FinanceDataReader as fdr
        except Exception:
            # fall back to lowercase package name if available
            try:
                import finance_datareader as fdr
            except Exception:
                raise ImportError(
                    "FinanceDataReader package is not installed in the running Python environment. "
                    "Install it with: pip install finance-datareader"
                )

        pre_dt = (date.today()-timedelta(days=1)).strftime('%Y%m%d')
        strd_dt = time.strftime('%Y%m%d')
        ins_dt = time.strftime('%Y%m%d%H%M%S')
        week_nm = date.today().weekday()
        if week_nm == 1:
            us_pre_dt = (date.today()-timedelta(days=3)).strftime('%Y%m%d')

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

        df_kospi = normalize(fdr.DataReader('KS11', strd_dt, strd_dt), 'KOSPI')
        df_kosdaq = normalize(fdr.DataReader('KQ11', strd_dt, strd_dt), 'KOSDAQ')
        df_nasdaq = normalize(fdr.DataReader('IXIC', pre_dt), 'NASDAQ')
        df_sp500 = normalize(fdr.DataReader('S&P500', pre_dt), 'S&P500')
        df_dji = normalize(fdr.DataReader('DJI', pre_dt), 'DowJones')

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
