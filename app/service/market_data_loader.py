from app.service.finance_data_reader_parser import FinanceDataReaderParser
from app.db import engine
from app.models import MarketTop

def load_market_data_to_db():
    """
    Fetches market data using FinanceDataReaderParser and loads it into the
    dbms_market_stock table.
    """
    fdr_parser = FinanceDataReaderParser()
    market_df = fdr_parser.get_data()

    if market_df is not None and not market_df.empty:
        table_name = MarketTop.__tablename__
        market_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        return len(market_df)
    return 0
