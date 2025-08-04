from core import create_connection
from orchestrator import DataOrchestrator
from fundamentals import FundamentalDataLoader
from global_prices import PriceDataLoader
from metadata import MetaDataJoiner
from output import PanelOutput
import polars as pl

class FactsetAPI:
    def __init__(self):
        self.conn = create_connection()
        self.meta = MetaDataJoiner(self.conn)
        self.prices = PriceDataLoader(self.conn)
        self.fundamentals = FundamentalDataLoader(self.conn)
        self.orchestrator = DataOrchestrator(self.conn)

    def load(self, isins: list[str], start: str = '1990-01-01', end: str ='2030-12-31',
                 price_fields: list[str] = None, fund_fields: list[str] = None, adjust: bool = True) -> dict[str, pl.DataFrame]:
        frames = self.orchestrator.load(isins=isins,start=start,end=end,price_fields=price_fields,fund_fields=fund_fields,adjust=adjust)
        #return PanelOutput(frames)
        return frames
    
    @property
    def list_tables(self) -> list[str]:
        return self.meta.list_tables()
    
    def get_table_columns(self, table: str) -> list[str]:
        return self.meta.get_table_columns(table)
    
    def get_column_tables(self, column: str) -> list[str]:
        return self.meta.get_column_tables(column)
    
    def get_field_description(self, field_name: str) -> str:
        return self.meta.get_field_description(field_name)
    
    def get_code_description(self, code: str) -> str:
        return self.meta.get_code_description(code)
    
    def get_table_description(self, table_name: str) -> str:
        return self.meta.get_table_description(table_name)
    
    def search_field(self, description: str) -> pl.DataFrame:
        return self.meta.search_field(description)
