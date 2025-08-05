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
        """
        Load and merge price and fundamental data for the specified ISINs and date range.

        This method orchestrates the retrieval of price and fundamental data, applies calendar alignment,
        optional adjustment of price fields, and handles fallback logic for fundamental field resolution.
        The result is wrapped in a `PanelOutput` object for easier exploration and filtering.

        Parameters
        ----------
        isins : list[str]
            List of ISINs to retrieve data for.

        start : str default = '1990-01-01'
            Start date (inclusive) of the time window in 'YYYY-MM-DD' format.

        end : str default = '2030-12-31'
            End date (inclusive) of the time window in 'YYYY-MM-DD' format.

        price_fields : list[str], optional
            List of fields to extract from the prices table (e.g. ['price', 'volume']).
            If None, no price data is loaded.

        fund_fields : list[str], optional
            List of fundamental fields to extract (e.g. ['ff_sales', 'ff_eps']).
            If None, no fundamental data is loaded.

        adjust : bool, default=True
            Whether to adjust price fields for corporate actions (splits/dividends).
            Only applies to adjustable price fields like 'price', 'volume', etc.

        frequency : str, default="qf"
            Frequency keyword used to select fundamental tables (e.g. 'qf', 'af', 'cf').
            Tables matching this keyword are prioritized for field resolution.

        fallback : bool, default=False
            If True, allows fallback to other tables when fields are not found in frequency-matched ones.
            If False, missing fields are filled with nulls.

        Returns
        -------
        PanelOutput
            An object containing:
            - prices: Polars DataFrame with adjusted or raw price data
            - fundamentals: Polars DataFrame with cleaned and calendar-aligned fundamentals

        Examples
        --------
        Example 1: Load adjusted prices only
        >>> api = FactsetAPI()
        >>> data = api.load(
        ...     isins=["US2910111044", "US2220702037"],
        ...     start="2020-01-01",
        ...     end="2021-01-01",
        ...     price_fields=["price", "volume"]
        ... )
        >>> data.get("prices").head()

        Example 2: Load fundamentals only with fallback enabled
        >>> data = api.load(
        ...     isins=["US92939U1060"],
        ...     start="2022-01-01",
        ...     end="2022-12-31",
        ...     fund_fields=["ff_amort_cf", "ff_eps", "ff_com_eq_par"],
        ...     frequency="af",
        ...     fallback=True
        ... )
        >>> data.get("fundamentals").head()

        Example 3: Load both prices and fundamentals with quarterly frequency
        >>> data = api.load(
        ...     isins=["US03784Y2000", "US2910111044"],
        ...     start="2021-01-01",
        ...     end="2023-01-01",
        ...     price_fields=["price", "vwap"],
        ...     fund_fields=["ff_real_gain", "ff_pe"],
        ...     frequency="qf"
        ... )
        >>> data.get("prices").shape, data.get("fundamentals").shape
        """        
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