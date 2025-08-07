import polars as pl
from typing import Dict
from global_prices import PriceDataLoader
from fundamentals import FundamentalDataLoader
from metadata import MetaDataJoiner
from estimates import EstimatesLoader
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
        self.estimates_loader = EstimatesLoader(conn)

    def load(
        self,
        isins: list[str],
        start_date: str,
        end_date: str,
        price_fields: list[str] = None,
        fund_fields: list[str] = None,
        adjust: bool = True,
        frequency: str ="qf",
        fallback: bool = False,
        est_items: list[str] = None,
        est_tables: list[str] = None,
        est_frequency: str = None

    ) -> dict[str, pl.DataFrame]:
        # Load price data if price_fields are specified
        df_price = (
            self.prices_loader.get_prices(isins, start_date, end_date, price_fields, adjust)
            if price_fields
            else pl.DataFrame([])   # Return empty DataFrame if no price fields provided
        )

        if fund_fields:
            # Retrieve fundamental data as a list of DataFrames per ISIN or entity
            result = self.fund_loader.get_fundamentals(isins, start_date, end_date, fund_fields, frequency, fallback)
            df_fund_list = result['dataframes']
            if df_fund_list:

                # Align all fundamental data on a unified calendar using metadata joiner
                df_fund_all = self.meta.join_on_calendar(df_fund_list)
                
                # Remove redundant or duplicate date columns except 'ref_date'
                drop_candidates = [c for c in df_fund_all.columns if c.lower().startswith("date") and c != "ref_date"]
                df_fund_all = df_fund_all.drop(drop_candidates)

                # Rename 'ref_date' to 'date' for consistency
                df_fund_all = df_fund_all.with_columns(pl.col("ref_date").alias("date")).drop("ref_date")

                # Filter rows to keep only those within the specified date range and non-null ISIN
                df_fund_all = df_fund_all.filter(
                    pl.col("date").is_between(pl.lit(start_date).cast(pl.Date), pl.lit(end_date).cast(pl.Date))
                ).drop_nulls("ISIN")

                # Reorder and select only relevant columns in the final output
                ordered_fields = [f for f in fund_fields if f in df_fund_all.columns]
                final_cols = ['date', 'ISIN'] + ordered_fields
                df_fund_all = df_fund_all.select([c for c in final_cols if c in df_fund_all.columns])
            else:
                df_fund_all = pl.DataFrame([])  # Return empty if no dataframes were returned
        else:
            df_fund_all = pl.DataFrame([])  # Return empty if no fundamental fields requested

        df_price_all = df_price.rename({"price_date":"date"})

        estimates_result={}
        if est_tables and est_items:
            est_freq = est_frequency or frequency
            for table_type in est_tables:
                df = self.estimates_loader.get_estimates(table_type=table_type,isins=isins,fe_items=est_items,start=start,end=end,frequency=est_freq)
                estimates_result[table_type] = df
        else:
            estimates_result = {}

        # Return both datasets as a dictionary
        return {"prices": df_price_all, "fundamentals": df_fund_all, "estimates": estimates_result}

    def __repr__(self):
        return f"<DataOrchestrator with tables: prices={self.prices_loader.TABLE}, fund={self.fund_loader.fundamental_tables}>"