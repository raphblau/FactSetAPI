import polars as pl
from typing import Dict
from global_prices import PriceDataLoader
from fundamentals import FundamentalDataLoader
from metadata import MetaDataJoiner
from core import create_connection


class DataOrchestrator:
    """
    Orchestrateur qui combine metadata, prices et fundamentals,
    puis effectue regroupements, calendrier, et renvoie deux DataFrames.
    """
    def __init__(self, conn):
        self.conn = conn
        self.meta = MetaDataJoiner(conn)
        self.prices_loader = PriceDataLoader(conn)
        self.fund_loader = FundamentalDataLoader(conn)

    def load(
        self,
        isins: list[str],
        start: str,
        end: str,
        price_fields: list[str] = None,
        fund_fields: list[str] = None,
        adjust: bool = True,
        frequency: str ="qf",
        fallback: bool = False
    ) -> dict[str, pl.DataFrame]:
        df_price = (
            self.prices_loader.get_prices(isins, start, end, price_fields, adjust)
            if price_fields
            else pl.DataFrame([])
        )

        if fund_fields:
            result = self.fund_loader.get_fundamentals(isins, start, end, fund_fields, frequency, fallback)
            df_fund_list = result['dataframes']
            if df_fund_list:

                df_fund_all = self.meta.join_on_calendar(df_fund_list)
                drop_candidates = [c for c in df_fund_all.columns if c.lower().startswith("date") and c != "ref_date"]
                df_fund_all = df_fund_all.drop(drop_candidates)
                df_fund_all = df_fund_all.with_columns(pl.col("ref_date").alias("date")).drop("ref_date")
                df_fund_all = df_fund_all.filter(
                    pl.col("date").is_between(pl.lit(start).cast(pl.Date), pl.lit(end).cast(pl.Date))
                ).drop_nulls("ISIN")
                ordered_fields = [f for f in fund_fields if f in df_fund_all.columns]
                final_cols = ['date', 'ISIN'] + ordered_fields
                df_fund_all = df_fund_all.select([c for c in final_cols if c in df_fund_all.columns])
            else:
                df_fund_all = pl.DataFrame([])
        else:
            df_fund_all = pl.DataFrame([])

        df_price_all = df_price.rename({"price_date":"date"})

        return {"prices": df_price_all, "fundamentals": df_fund_all}

    def __repr__(self):
        return f"<DataOrchestrator with tables: prices={self.prices_loader.TABLE}, fund={self.fund_loader.fundamental_tables}>"