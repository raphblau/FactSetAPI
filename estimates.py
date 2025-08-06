import polars as pl
from utils import SharedUtils
from typing import Dict,List

class EstimatesLoader:
    def __init__(self, conn):
        self.conn = conn

    def get_estimates(
        self, table_type: str, fe_items: list[str], isins: list[str], start: str, end: str, frequency: str = "qf"
    ) -> pl.DataFrame:

        isin_df = SharedUtils.get_isin_map(isins, self.conn)
        isin_map = dict(zip(isin_df["isin"], isin_df["fsym_regional_id"]))
        fsym_ids = list(isin_map.values())
        if not fsym_ids:
            return pl.DataFrame([])

        fsym_filter = "', '".join(fsym_ids)

        tables_to_try = [
            f"fe_v4.fe_advanced_{table_type}_{frequency}",
            f"fe_v4.fe_basic_{table_type}_{frequency}"
        ]

        collected = []
        found_items = set()

        for table in tables_to_try:
            missing_items = list(set(fe_items) - found_items)
            if not missing_items:
                break

            query = f"""
                SELECT * FROM {table}
                WHERE fe_item IN ({','.join(f"'{item}'" for item in missing_items)})
                AND fe_fp_end BETWEEN '{start}' AND '{end}'
                AND fsym_id IN ('{fsym_filter}')
            """

            try:
                df = pl.read_database(query, connection=self.conn)
                if df.is_empty():
                    continue
                found = df["fe_item"].unique().to_list()
                found_items.update(found)
                collected.append(df)
            except Exception:
                continue

        if not collected:
            return pl.DataFrame([])

        df_all = pl.concat(collected)

        missing_items = list(set(fe_items) - found_items)
        if missing_items:
            base = df_all.select(["fsym_id", "fe_fp_end", "currency"]).unique()
            for item in missing_items:
                df_null = base.with_columns([
                    pl.lit(item).alias("fe_item"),
                    pl.lit(None).alias("value")
                ])
                df_all = pl.concat([df_all, df_null], how="diagonal")

        reverse_map = {v: k for k, v in isin_map.items()}

        df_all = df_all.with_columns([
            pl.col("fsym_id").map_elements(lambda x: reverse_map.get(x, None), return_dtype=pl.Utf8).alias("ISIN")
        ])

        first_cols = ["fe_fp_end", "ISIN", "fe_item"]
        remaining_cols = [col for col in df_all.columns if col not in first_cols]

        df_all = df_all.select(first_cols + remaining_cols)

        return df_all