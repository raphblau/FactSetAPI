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
        return self.frames.get(key, pl.DataFrame())

    def filter(
        self,
        isins: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> 'PanelOutput':
        """
        Filter all DataFrames by ISIN and date range.
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
        Pivot a feature from a source (e.g., 'prices') to [date x ISIN] format.
        """
        df = self.get(source)
        if df.is_empty() or feature not in df.columns:
            return pl.DataFrame()
        return df.select([index, entity, feature]).pivot(index=index, columns=entity, values=feature)

    def to_feature_matrices(
        self,
        source: str,
        index: str = "date",
        entity: str = "ISIN"
    ) -> Dict[str, pl.DataFrame]:
        """
        Return all [index x entity] feature matrices from a given source.
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
