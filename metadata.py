import polars as pl
import pandas as pd
from functools import lru_cache
from utils import SharedUtils

class MetaDataJoiner:
    """
    Classe utilitaire pour lister tables, colonnes, et effectuer des jointures calendrier.
    """
    def __init__(self, conn):
        self.conn = conn
        self.utils = SharedUtils(self.conn)

    def list_tables(self) -> list[str]:
        query = (
            "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_table "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE='BASE TABLE';"
        )
        df = pd.read_sql(query, self.conn)
        return df['full_table'].tolist()

    def get_table_columns(self, table: str) -> list[str]:
        schema, tbl = table.split('.')
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{tbl}';
        """
        df = pd.read_sql(query, self.conn)
        return df['COLUMN_NAME'].tolist()

    def join_on_calendar(
    self,
    dfs: list[pl.DataFrame],
    calendar_table: str = 'ref_v2.ref_calendar_dates',
    date_col: str = 'ref_date'
    ) -> pl.DataFrame:
        """
        Joint une liste de DataFrames sur le calendrier FactSet.
        Remplit en null si pas de correspondance, puis forward-fill par ISIN.
        """
        cal_pdf = pd.read_sql(f"SELECT {date_col} FROM {calendar_table}", self.conn)
        cal = pl.from_pandas(cal_pdf)
        result = cal

        for df in dfs:
            dc = 'price_date' if 'price_date' in df.columns else 'date'

            join_keys_left = [date_col]
            join_keys_right = [dc]

            if 'ISIN' in result.columns and 'ISIN' in df.columns:
                join_keys_left.append('ISIN')
                join_keys_right.append('ISIN')

            common = set(result.columns) & set(df.columns)
            cols_to_drop = [c for c in common if c not in join_keys_right]
            df = df.drop(cols_to_drop)

            result = result.join(df, left_on=join_keys_left, right_on=join_keys_right, how='left')
        return result
    
    def get_column_tables(self, column: str) -> list[str]:
        """
        Vérifie quelles tables contiennent une colonne donnée.
        Retourne la liste de schéma.tables.
        """
        query = f"""
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_table
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE COLUMN_NAME = '{column}';
        """
        df = self.utils._read_sql(query)
        return df['full_table'].to_list()
    
    def get_field_description(self, field_name: str) -> str:
        query = f"""
        SELECT field_description
        FROM [ref_v2].[ref_metadata_fields]
        WHERE field_name = '{field_name}'
        """
        df = pd.read_sql(query,self.conn)
        return df['field_description'].iloc[0] if not df.empty else None
    
    def get_code_description(self, code: str) -> str:
        query = f"""
        SELECT code_description
        FROM [ref_v2].[ref_metadata_codes]
        WHERE code = '{code}'
        """
        df = pd.read_sql(query,self.conn)
        return df['code_description'].iloc[0] if not df.empty else None
    
    def get_table_description(self, table_name: str) -> str:
        query = f"""
        SELECT table_description
        FROM [ref_v2].[ref_metadata_tables]
        WHERE table_name = '{table_name}'
        """
        df = pd.read_sql(query,self.conn)
        return df['table_description'].iloc[0] if not df.empty else None
    
    def search_field(self, description: str) -> pl.DataFrame:
        query = """
        SELECT field_name, field_description
        FROM ref_v2.ref_metadata_fields
        """
        pl_df = self.utils._read_sql(query)

        result = pl_df.filter(
            pl_df["field_description"].str.to_lowercase().str.contains(description.lower())
        )
        return result.unique("field_name")


