import pandas as pd
import time
import datetime
from datetime import date, timedelta


class FinanceDataReaderParser():
    """Lazy FinanceDataReader wrapper.

    The original implementation performed imports and network calls at module
    import time which caused server startup failures when the FinanceDataReader
    package was not yet available. This version defers importing the
    FinanceDataReader module and the data retrieval until get_data() is called.
    """
    def __init__(self):
        # keep __init__ lightweight; actual work happens in get_data()
        self.df = None

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

            # Determine which column is the date/index column
            # Common names: 'Date' or the first column after reset_index
            if 'Date' in df_local.columns:
                date_col = 'Date'
            else:
                # assume the first column is the date/index column
                date_col = df_local.columns[0]

            # Standard columns we want to preserve
            want = ['stock_day', 'Open', 'High', 'Low', 'Close', 'Volume']

            standardized = pd.DataFrame()
            standardized['stock_day'] = df_local[date_col]
            # For each expected column, use it if present, otherwise fill with NaN
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in df_local.columns:
                    standardized[col] = df_local[col]
                else:
                    standardized[col] = pd.NA

            # attach metadata columns
            standardized.insert(0, 'market', market_name)
            standardized.insert(0, 'strd_dt', strd_dt)
            return standardized

        df_kospi = normalize(fdr.DataReader('KS11', strd_dt, strd_dt), 'KOSPI')
        df_kosdaq = normalize(fdr.DataReader('KQ11', strd_dt, strd_dt), 'KOSDAQ')
        df_nasdaq = normalize(fdr.DataReader('IXIC', pre_dt), 'NASDAQ')
        df_sp500 = normalize(fdr.DataReader('S&P500', pre_dt), 'S&P500')
        df_dji = normalize(fdr.DataReader('DJI', pre_dt), 'DowJones')

        df_total = pd.concat([df_kospi, df_kosdaq, df_nasdaq, df_sp500, df_dji], ignore_index=True)
        # replace pandas NaT with a timestamp string for consistency
        df_total.replace({pd.NaT: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, inplace=True)

        # Now map to your expected output column names
        rename_map = {
            'Open': 'opening_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'closing_price',
            'Volume': 'volume'
        }

        df_total = df_total.rename(columns=rename_map)

        # Ensure all target columns exist
        expected_cols = ['strd_dt', 'market', 'stock_day', 'opening_price', 'high_price', 'low_price', 'closing_price', 'volume']
        for c in expected_cols:
            if c not in df_total.columns:
                df_total[c] = pd.NA

        # Reorder columns
        df = df_total[expected_cols].copy()
        df['ins_dt'] = ins_dt

        self.df = df
        return self.df
