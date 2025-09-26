from app.service.finance_data_reader_parser import FinanceDataReaderParser

if __name__ == '__main__':
    try:
        p = FinanceDataReaderParser()
        df = p.get_data()
        print('OK', type(df), getattr(df, 'shape', None))
    except Exception as e:
        import traceback
        traceback.print_exc()
        print('ERROR:', e)
