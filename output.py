from typing import Optional, Union, List, Dict
import polars as pl
import pandas as pd

class PanelOutput:
    """
    Container for panel data returned by the FactsetAPI.
    Keys in `frames` can be 'prices', 'fundamentals', etc.
    Each is a Polars DataFrame in long format with at least ['date', 'ISIN', ...]
    """

    frames: Dict[str, pl.DataFrame]

    def __init__(self, frames: Dict[str, pl.DataFrame]):
        self.frames = frames

    def get(self, key: str) -> pl.DataFrame:
        """
        Retrieve a specific DataFrame by its key (e.g., "prices", "fundamentals").

        Parameters
        ----------
        key : str
            The key corresponding to the desired DataFrame.

        Returns
        -------
        pl.DataFrame
            The requested DataFrame, or an empty one if the key is not found.

        Examples
        --------
        >>> df_prices = po.get("prices")
        >>> print(df_prices.columns)
        """
        return self.frames.get(key, pl.DataFrame())

    def filter(
        self,
        isins: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> 'PanelOutput':
        """
        Filter all contained DataFrames by ISIN and/or date range.

        Parameters
        ----------
        isins : str or list[str], optional
            One or multiple ISINs to keep. If None, no ISIN filtering is applied.
        start_date : str, optional
            Minimum date (inclusive) in 'YYYY-MM-DD' format.
        end_date : str, optional
            Maximum date (inclusive) in 'YYYY-MM-DD' format.

        Returns
        -------
        PanelOutput
            A new PanelOutput instance with filtered DataFrames.

        Examples
        --------
        >>> filtered_po = po.filter(isins="US0378331005", start_date="2022-01-01", end_date="2022-12-31")
        >>> print(filtered_po.get("prices").shape)
        """
        new_frames = {}
        for key, df in self.frames.items():
            filtered = df
            if isins is not None:
                isins = [isins] if isinstance(isins, str) else isins
                filtered = filtered.filter(pl.col("ISIN").is_in(isins))
            if start_date is not None:
                filtered = filtered.filter(pl.col("date") >= start_date)
            if end_date is not None:
                filtered = filtered.filter(pl.col("date") <= end_date)
            new_frames[key] = filtered
        return PanelOutput(new_frames)

    def to_matrix(
        self,
        source: str,
        feature: str,
        index: str = "date",
        entity: str = "ISIN"
    ) -> pl.DataFrame:
        """
        Pivot a single feature column into a wide matrix format.

        Parameters
        ----------
        source : str
            The key of the DataFrame to use (e.g., 'prices').
        feature : str
            The name of the feature column to pivot.
        index : str, default="date"
            The column to use as row index (typically 'date').
        entity : str, default="ISIN"
            The column to use as columns in the pivoted matrix.

        Returns
        -------
        pl.DataFrame
            A pivoted DataFrame with rows as dates and columns as ISINs (or other entities).
            If the feature is not found, an empty DataFrame is returned.

        Examples
        --------
        >>> matrix = po.to_matrix(source="prices", feature="price")
        >>> print(matrix.head())
        """
        df = self.get(source)
        if df.is_empty() or feature not in df.columns:
            return pl.DataFrame()
        return df.select([index, entity, feature]).pivot(index=index, columns=entity, values=feature)

    def to_matrices(
        self,
        source: str,
        index: str = "date",
        entity: str = "ISIN"
    ) -> Dict[str, pl.DataFrame]:
        """
        Convert all features from a given source into separate pivoted matrices.

        Parameters
        ----------
        source : str
            The key of the DataFrame to use (e.g., 'prices', 'fundamentals').
        index : str, default="date"
            The column to use as row index in each matrix.
        entity : str, default="ISIN"
            The column to use as column headers in each matrix.

        Returns
        -------
        Dict[str, pl.DataFrame]
            A dictionary where each key is a feature name, and each value is a pivoted DataFrame.

        Examples
        --------
        >>> matrices = po.to_matrices("prices")
        >>> for name, df in matrices.items():
        ...     print(f"{name} matrix shape: {df.shape}")
        """
        df = self.get(source)
        base_cols = {index, entity}
        features = [col for col in df.columns if col not in base_cols]

        return {
            feat: self.to_matrix(source, feat, index=index, entity=entity)
            for feat in features
        }

    def to_pandas(self) -> Dict[str, pd.DataFrame]:
        return {k: v.to_pandas() for k, v in self.frames.items()}
