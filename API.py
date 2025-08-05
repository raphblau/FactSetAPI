from core import create_connection
from orchestrator import DataOrchestrator
from fundamentals import FundamentalDataLoader
from global_prices import PriceDataLoader
from metadata import MetaDataJoiner
from output import PanelOutput
import polars as pl
import warnings
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")

class FactsetAPI:
    def __init__(self):
        self.conn = create_connection()
        self.meta = MetaDataJoiner(self.conn)
        self.prices = PriceDataLoader(self.conn)
        self.fundamentals = FundamentalDataLoader(self.conn)
        self.orchestrator = DataOrchestrator(self.conn)

    def load(self, isins: list[str], start: str = '1990-01-01', end: str ='2030-12-31',
                 price_fields: list[str] = None, fund_fields: list[str] = None, adjust: bool = True, frequency: str = "qf", fallback: bool = False) -> dict[str, pl.DataFrame]:
        frames = self.orchestrator.load(isins=isins,start=start,end=end,price_fields=price_fields,fund_fields=fund_fields,adjust=adjust,frequency=frequency,fallback=fallback)
        return PanelOutput(frames)
        #return frames
    
    @property
    def list_tables(self) -> list[str]:
        """
        List all base tables in the database.

        Examples
        --------
        >>> meta.list_tables()
        ['ff_v3.ff_basic_qf', 'sym_v1.sym_coverage', ...]
        """
        return self.meta.list_tables()
    
    def get_table_columns(self, table: str) -> list[str]:
        """
        Get column names for a specific table.

        Examples
        --------
        >>> meta.get_table_columns("ff_v3.ff_basic_qf")
        ['fsym_id', 'date', 'ff_sales', ...]
        """
        return self.meta.get_table_columns(table)
    
    def get_column_tables(self, column: str) -> list[str]:
        """
        Find all tables that contain a specific column.

        Examples
        --------
        >>> meta.get_column_tables("ff_report_date")
        ['ff_v3.ff_basic_qf', 'ff_v3.ff_advanced_af', ...]
        """
        return self.meta.get_column_tables(column)
    
    def get_field_description(self, field_name: str) -> str:
        """
        Get description of a field from metadata.

        Examples
        --------
        >>> meta.get_field_description("ff_sales")
        'Total Revenue (Sales) reported by the company'
        """
        return self.meta.get_field_description(field_name)
    
    def get_code_description(self, code: str) -> str:
        """
        Get description of a metadata code.

        Examples
        --------
        >>> meta.get_code_description("GAAP")
        'Generally Accepted Accounting Principles'
        """
        return self.meta.get_code_description(code)
    
    def get_table_description(self, table_name: str) -> str:
        """
        Get description of a table from metadata.

        Examples
        --------
        >>> meta.get_table_description("ff_basic_qf")
        'Quarterly fundamental data - basic fields'
        """
        return self.meta.get_table_description(table_name)
    
    def search_field(self, description: str) -> pl.DataFrame:
        """
        Search for fields based on a partial description.

        Examples
        --------
        >>> meta.search_field("price to earnings")
        shape: (n_matches, 2)
        """
        return self.meta.search_field(description)