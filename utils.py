import pandas as pd
import polars as pl

class SharedUtils:

    def __init__(self, conn):
        self.conn = conn

    def get_isin_map(isins: list[str], conn) -> pd.DataFrame:
        isin_list = "', '".join(isins)
        query = f"""
            SELECT symi.isin, cov.fsym_regional_id
            FROM sym_v1.sym_isin AS symi
            JOIN sym_v1.sym_coverage AS cov
                ON symi.fsym_id = cov.fsym_security_id
            WHERE symi.isin IN ('{isin_list}')
        """
        return pd.read_sql(query, conn)
    
    def _read_sql(self, query: str) -> pl.DataFrame:
        df_pd = pd.read_sql(query, self.conn)
        return pl.from_pandas(df_pd)