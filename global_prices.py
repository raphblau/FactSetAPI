import polars as pl
import pandas as pd
from utils import SharedUtils
import warnings

class PriceDataLoader:
    """
    Chargement et ajustement des données de prix.
    """
    TABLE = 'fgp_v1.fgp_global_prices'

    ADJUSTABLE_COLUMNS = {
        'price', 'price_open', 'price_high', 'price_low', 'volume', 
        'turnover', 'vwap'
    }

    NON_ADJUSTABLE_COLUMNS = {
        'fsym_id', 'price_date', 'currency', 'trade_count', 
        'one_day_pct', 'wtd_pct', 'mtd_pct', 'qtd_pct', 'ytd_pct',
        'one_mth_pct', 'three_mth_pct', 'six_mth_pct', 'nine_mth_pct',
        'one_yr_pct', 'two_yr_pct', 'three_yr_pct', 'five_yr_pct', 'ten_yr_pct'
    }

    def __init__(self, conn):
        self.conn = conn
    
    
    def get_prices(self, isins: list[str], start_date: str, end_date: str, fields: list[str] = ['price', 'volume'], adjust:bool = True) -> pl.DataFrame:

        df_isin_map = SharedUtils.get_isin_map(isins,self.conn)
        fsym_ids = df_isin_map['fsym_regional_id'].unique().tolist()

        available_columns = self._get_available_price_columns()
        resolved_fields = self._resolve_price_fields(fields, available_columns)

        if not resolved_fields:
            warnings.warn("Aucun champ valide à traiter. Un DataFrame vide sera retourne.")
            return pl.DataFrame()

        df_prices = self._get_prices_raw(fsym_ids, resolved_fields, start_date, end_date)
        df_adj = self._get_adjustment_factors(fsym_ids, start_date)

        pl_prices, pl_adj, pl_isin_map = self._prepare_pl_dfs(df_prices, df_adj, df_isin_map)

        if adjust:
            pl_prices = self.compute_adjustment_factors(pl_prices, pl_adj)
        else:
            pl_prices = pl_prices.with_columns(pl.lit(1.0).alias("cum_factor"))

        base_columns = ["price_date", "ISIN"]
        adjustable_fields, non_adjustable_fields = self._split_fields(resolved_fields)

        result_columns = base_columns + non_adjustable_fields + adjustable_fields

        if adjust and adjustable_fields:
            pl_prices = self.adjust_columns(pl_prices, adjustable_fields)
            result_columns += [f"{col}_adj" for col in adjustable_fields]

        return pl_prices.select(result_columns)

    def _get_prices_raw(self, fsym_ids: list[str], fields: list[str], start_date: str, end_date: str) -> pd.DataFrame:
        if not fsym_ids:
            return pd.DataFrame()
        fsym_id_list = ', '.join(f"'{fsym}'" for fsym in fsym_ids)
        field_str = ', '.join(fields)
        return pd.read_sql(f"""
            SELECT fsym_id, price_date, {field_str}
            FROM {self.TABLE}
            WHERE fsym_id IN ({fsym_id_list})
            AND price_date BETWEEN '{start_date}' AND '{end_date}'
            AND fsym_id LIKE '%-R'
        """, self.conn)

    def _get_adjustment_factors(self, fsym_ids: list[str], start_date: str) -> pd.DataFrame:
        if not fsym_ids:
            return pd.DataFrame()
        fsym_id_list = ', '.join(f"'{fsym}'" for fsym in fsym_ids)
        return pd.read_sql(f"""
            SELECT fsym_id, effective_date, div_spl_spin_adj_factor
            FROM fgp_v1.fgp_ca_adj_factors
            WHERE fsym_id IN ({fsym_id_list})
            AND effective_date >= '{start_date}'
        """, self.conn)

    def _prepare_pl_dfs(self, df_prices: pd.DataFrame, df_adj: pd.DataFrame, df_isin_map: pd.DataFrame):
        pl_prices = pl.from_pandas(df_prices).with_columns(
            pl.col("price_date").cast(pl.Date)
        )
        pl_adj = pl.from_pandas(df_adj).with_columns(
            pl.col("effective_date").cast(pl.Date)
        )
        pl_isin_map = pl.from_pandas(df_isin_map).rename({
            "fsym_regional_id": "fsym_id"
        }).unique(subset=["fsym_id"], keep="first")

        pl_prices = pl_prices.join(pl_isin_map, on="fsym_id", how="inner")
        pl_adj = pl_adj.join(pl_isin_map, on="fsym_id", how="inner")

        if "isin" in pl_prices.columns:
            pl_prices = pl_prices.rename({"isin": "ISIN"})
        if "isin" in pl_adj.columns:
            pl_adj = pl_adj.rename({"isin": "ISIN"})

        return (
            pl_prices.sort(["fsym_id", "price_date"]),
            pl_adj.sort(["fsym_id", "effective_date"]),
            pl_isin_map
        )

    def _split_fields(self, fields: list[str]):
        adjustable_fields = [col for col in fields if col in self.ADJUSTABLE_COLUMNS]
        non_adjustable_fields = [col for col in fields if col not in self.ADJUSTABLE_COLUMNS]
        return adjustable_fields, non_adjustable_fields

    def compute_adjustment_factors(self, pl_prices: pl.DataFrame, pl_adj: pl.DataFrame) -> pl.DataFrame:
        """
        Calcule le facteur d'ajustement cumulé pour chaque date de prix.
        Pour chaque date de prix, on calcule le produit de tous les facteurs d'ajustement
        qui se sont produits APRÈS cette date.
        """
        result_frames = []
        
        for isin in pl_prices["ISIN"].unique():
            prices_isin = pl_prices.filter(pl.col("ISIN") == isin).sort("price_date")
            adj_isin = pl_adj.filter(pl.col("ISIN") == isin).sort("effective_date")
            
            if adj_isin.height == 0:
                prices_isin = prices_isin.with_columns(pl.lit(1.0).alias("cum_factor"))
            else:
                price_dates = prices_isin["price_date"].to_list()
                adj_dates = adj_isin["effective_date"].to_list()
                adj_factors = adj_isin["div_spl_spin_adj_factor"].to_list()
                
                cum_factors = []
                for price_date in price_dates:
                    future_factors = [
                        factor for date, factor in zip(adj_dates, adj_factors)
                        if date > price_date
                    ]
                    cum_factor = 1.0
                    for factor in future_factors:
                        if factor is not None:
                            cum_factor *= factor
                    cum_factors.append(cum_factor)
                
                prices_isin = prices_isin.with_columns(
                    pl.Series("cum_factor", cum_factors)
                )
            
            result_frames.append(prices_isin)
        
        return pl.concat(result_frames) if result_frames else pl_prices.with_columns(pl.lit(1.0).alias("cum_factor"))

    def adjust_columns(self,
                       df: pl.DataFrame,
                       value_cols: list[str],
                       factor_col: str = 'cum_factor') -> pl.DataFrame:
        """
        Applique le facteur cumulatif à plusieurs colonnes numériques.
        """
        exprs = [
            (pl.col(col) * pl.col(factor_col)).alias(f"{col}_adj")
            for col in value_cols
        ]
        return df.with_columns(exprs)
    
    def _resolve_price_fields(self, fields: list[str], available_columns: list[str]) -> list[str]:
        """
        Vérifie que chaque champ demandé existe bien dans les colonnes disponibles (issues des prix).
        Ignore les champs introuvables avec un warning.
        """
        resolved = []

        for field in fields:
            if field not in available_columns:
                warnings.warn(
                    f"Le champ '{field}' est introuvable dans les données de prix disponibles. Il sera ignore."
                )
            else:
                resolved.append(field)

        return resolved
    
    def _get_available_price_columns(self) -> list[str]:
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'fgp_global_prices' AND TABLE_SCHEMA = 'fgp_v1'
        """
        df = pd.read_sql(query, self.conn)
        return df['COLUMN_NAME'].tolist()
