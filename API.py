from core import create_connection
from orchestrator import DataOrchestrator
from fundamentals import FundamentalDataLoader
from global_prices import PriceDataLoader
from estimates import EstimatesLoader
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
                 price_fields: list[str] = None, fund_fields: list[str] = None, adjust: bool = True, frequency: str = "qf", fallback: bool = False,
                 est_tables: list[str] = None, est_items: list[str] = None, est_frequency: str = None) -> dict[str, pl.DataFrame]:
        """
        Load and merge price, fundamental, and estimate data for the specified ISINs and date range.

        This method orchestrates the retrieval of price, fundamental, and estimate data, applies calendar alignment,
        handles adjustment logic for price fields, fallback logic for fundamental resolution, and allows flexible
        extraction of multiple estimate types (e.g. consensus, guidance, actuals).

        Parameters
        ----------
        isins : list[str]
            List of ISINs to retrieve data for.

        start : str, default='1990-01-01'
            Start date (inclusive) in 'YYYY-MM-DD' format.

        end : str, default='2030-12-31'
            End date (inclusive) in 'YYYY-MM-DD' format.

        price_fields : list[str], optional
            List of fields to extract from the prices table (e.g. ['close', 'volume']).
            If None, no price data is loaded.

        fund_fields : list[str], optional
            List of fundamental fields to extract (e.g. ['ff_sales', 'ff_eps']).
            If None, no fundamental data is loaded.

        adjust : bool, default=True
            Whether to adjust price fields for splits/dividends.

        frequency : str, default="qf"
            Frequency used for fundamental data (e.g. 'qf', 'af').

        fallback : bool, default=False
            If True, allows fallback to other tables when fundamental fields are missing.
            If False, missing fields are filled with nulls.

        est_tables : list[str], optional
            List of estimate types to load, such as ["conh", "act", "guid"].
            If None, no estimate data is loaded.

        est_items : list[str], optional
            List of estimate `fe_item` codes to extract (e.g. ["EPS", "BPS"]).
            If None, no estimate data is loaded.

        est_frequency : str, optional
            Frequency used for estimates (e.g. 'qf', 'af'). If None, uses `frequency`.

        Returns
        -------
        PanelOutput
            An object containing:
            - prices: pl.DataFrame with adjusted or raw price data
            - fundamentals: pl.DataFrame with cleaned and calendar-aligned fundamental data
            - estimates: dict[str, pl.DataFrame] (one dataframe per estimate type)

        Examples
        --------
        Example 1: Load prices only
        >>> api = FactsetAPI()
        >>> data = api.load(
        ...     isins=["US2910111044"],
        ...     start="2021-01-01",
        ...     end="2022-01-01",
        ...     price_fields=["close", "volume"]
        ... )
        >>> data.get("prices").head()

        Example 2: Load fundamentals only
        >>> data = api.load(
        ...     isins=["US92939U1060"],
        ...     fund_fields=["ff_eps", "ff_sales"],
        ...     frequency="af",
        ...     fallback=True
        ... )
        >>> data.get("fundamentals").head()

        Example 3: Load estimates (consensus + actuals)
        >>> data = api.load(
        ...     isins=["US0378331005"],
        ...     est_tables=["conh", "act"],
        ...     est_items=["EPS", "BPS"],
        ...     est_frequency="af"
        ... )
        >>> data.get("estimates")["conh"].head()

        Example 4: Load everything
        >>> data = api.load(
        ...     isins=["US03784Y2000"],
        ...     start="2020-01-01",
        ...     end="2023-01-01",
        ...     price_fields=["close"],
        ...     fund_fields=["ff_eps"],
        ...     est_tables=["conh"],
        ...     est_items=["EPS"],
        ...     est_frequency="af"
        ... )
        >>> data.get("fundamentals").shape
        >>> data.get("estimates")["conh"].shape
        """
        frames = self.orchestrator.load(isins=isins,start=start,end=end,price_fields=price_fields,fund_fields=fund_fields,
                                        adjust=adjust,frequency=frequency,fallback=fallback,est_tables=est_tables,
                                        est_items=est_items,est_frequency=est_frequency)
        
        if "estimates" in frames and isinstance(frames["estimates"], dict):
            estimates = frames.pop("estimates")
            for k, v in estimates.items():
                frames[f"estimates.{k}"] = v

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