from core import create_connection
from orchestrator import DataOrchestrator
from metadata import MetaDataJoiner
from output import PanelOutput
import polars as pl
import warnings
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")



class FactsetAPI:
    def __init__(self):
        self.conn = create_connection()
        self.meta = MetaDataJoiner(self.conn)
        self.orchestrator = DataOrchestrator(self.conn)

    def load_all(
        self, 
        isins: list[str], 
        start_date: str = '1990-01-01', 
        end_date: str = '2030-12-31',
        price_fields: list[str] = None, 
        fund_fields: list[str] = None, 
        price_adjust: bool = True, 
        frequency: str = "qf", 
        fund_fallback: bool = False,
        est_tables: list[str] = None, 
        est_items: list[str] = None, 
        est_frequency: str = None
    ) -> dict[str, pl.DataFrame]:
        """
        Load all available datasets (prices, fundamentals, estimates) for the specified ISINs and date range.

        This method loads and returns all data types supported by the API, with granular control over 
        adjustment, frequency, fallback logic, and estimate configurations. It provides a unified 
        interface for full data panel extraction.

        Parameters
        ----------
        isins : list[str]
            List of ISINs to retrieve data for.

        start_date : str, default='1990-01-01'
            Start date (inclusive) in 'YYYY-MM-DD' format.

        end_date : str, default='2030-12-31'
            End date (inclusive) in 'YYYY-MM-DD' format.

        price_fields : list[str], optional
            List of price fields to retrieve (e.g. ['close', 'volume']).
            If None, price data is not loaded.

        fund_fields : list[str], optional
            List of fundamental fields to retrieve (e.g. ['ff_sales', 'ff_eps']).
            If None, fundamental data is not loaded.

        price_adjust : bool, default=True
            Whether to adjust price data for splits and dividends.

        frequency : str, default="qf"
            Frequency used for both fundamental and estimate data (unless overridden for estimates).

        fund_fallback : bool, default=False
            Whether to fallback to other fundamental sources if data is missing.

        est_tables : list[str], optional
            List of estimate table types (e.g. ['conh', 'act', 'guid']).
            If None, estimates are not loaded.

        est_items : list[str], optional
            List of estimate item codes to extract (e.g. ['EPS', 'BPS']).
            If None, estimates are not loaded.

        est_frequency : str, optional
            Frequency for estimates (e.g. 'qf', 'af'). If None, uses `frequency`.

        Returns
        -------
        PanelOutput
            An object containing:
            - prices: pl.DataFrame with price data (or empty if not loaded)
            - fundamentals: pl.DataFrame with aligned fundamental data (or empty if not loaded)
            - estimates.{table}: pl.DataFrame for each estimate type requested (or none if not loaded)

        Examples
        --------
        >>> api = FactsetAPI()
        >>> data = api.load_all(
        ...     isins=["US0378331005"],
        ...     start_date="2020-01-01",
        ...     end_date="2023-01-01",
        ...     price_fields=["close", "volume"],
        ...     fund_fields=["ff_eps", "ff_sales"],
        ...     price_adjust=True,
        ...     fund_fallback=True,
        ...     est_tables=["conh", "act"],
        ...     est_items=["EPS"],
        ...     est_frequency="af"
        ... )
        >>> data.get("prices").head()
        >>> data.get("fundamentals").head()
        >>> data.get("estimates.conh").head()
        >>> data.get("estimates.act").shape
        """
        frames = self.orchestrator.load_all(
            isins=isins,
            start_date=start_date,
            end_date=end_date,
            price_fields=price_fields,
            fund_fields=fund_fields,
            adjust=price_adjust,
            frequency=frequency,
            fallback=fund_fallback,
            est_tables=est_tables,
            est_items=est_items,
            est_frequency=est_frequency
        )
        
        if "estimates" in frames and isinstance(frames["estimates"], dict):
            estimates = frames.pop("estimates")
            for k, v in estimates.items():
                frames[f"estimates.{k}"] = v

        return PanelOutput(frames)

    def load_prices(
        self,
        isins: list[str],
        fields: list[str],
        start_date: str = '1990-01-01',
        end_date: str = '2030-12-31',
        adjust: bool = True
    ) -> pl.DataFrame:
        return self.orchestrator.load_prices(isins, start_date, end_date, fields, adjust)

    def load_fundamentals(
        self,
        isins: list[str],
        fields: list[str],
        start_date: str= '1990-01-01',
        end_date: str= '2030-12-31',
        frequency: str = "qf",
        fallback: bool = False
    ) -> pl.DataFrame:
        return self.orchestrator.load_fundamentals(isins, start_date, end_date, fields, frequency, fallback)

    def load_estimates(
        self,
        isins: list[str],
        tables: list[str],
        items: list[str],
        start_date: str= '1990-01-01',
        end_date: str= '2030-12-31',
        frequency: str = "qf"
    ) -> dict[str, pl.DataFrame]:
        return self.orchestrator.load_estimates(isins, start_date, end_date, items, tables, frequency)

    
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