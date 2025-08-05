import polars as pl

class EstimatesLoader:
    def __init__(self, conn):
        self.conn = conn

    def load(
        self,
        table_type: str,
        fe_items: list[str],
        start: str,
        end: str,
        frequency: str = "qf"
    ) -> pl.DataFrame:

        tables_to_try = [
            f"fe_v4.fe_advanced_{table_type}_{frequency}",
            f"fe.fe_{table_type}_{frequency}"
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

        return df_all