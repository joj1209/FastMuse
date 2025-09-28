from app.service.finance_data_reader_parser import FinanceDataReaderParser
import pdb

parser = FinanceDataReaderParser()

# This line will start the debugger
pdb.set_trace()

df = parser.get_data()
print(df)
