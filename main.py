import os
import pyodbc
import polars as pl
import pandas as pd
from orchestrator import DataOrchestrator

# core.py

def create_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=XW12LBIZ0013;"
        "DATABASE=AIDB;"            
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# factsetapi.py


class FactSetAPI:
    """
    Client principal pour interroger la base FactSet via SSMS.
    Expose des méthodes pour les données prices, estimates, fundamentals,
    et des utilitaires de formatage et de transformation.
    """

    DATASETS = {
        'prices': 'fgp_v1.fgp_global_prices',
        'estimates': 'fe_v4.fe_basic_qf',
        'fundamentals': 'ff_v3.ff_basic_qf'
    }

    def __init__(self):
        self.conn = create_connection()

    def _read_sql(self, query: str) -> pl.DataFrame:
        df_pd = pd.read_sql(query, self.conn)
        return pl.from_pandas(df_pd)
    
    def list_tables(self) -> list[str]:
        """
        Retourne la liste des schéma.tables accessibles dans la BDD.
        """
        query = (
            "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_table "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE='BASE TABLE';"
        )
        df = self._read_sql(query)
        return df['full_table'].to_list()
    
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
        df = self._read_sql(query)
        return df['full_table'].to_list()

    def choose_dataset_by_columns(self, columns: list[str]) -> list[str]:
        """
        Détermine quel(s) dataset(s) utiliser en fonction des colonnes demandées.
        Retourne la liste des clés 'prices', 'estimates', 'fundamentals'.
        """
        choices = []
        for ds, table in self.DATASETS.items():

            cols = self.get_table_columns(table)
            if any(col in cols for col in columns):
                choices.append(ds)
        return choices
    
    def get_table_columns(self, table: str) -> list[str]:
        """
        Renvoie la liste des colonnes d'une table.
        """
        schema, tbl = table.split('.')
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{tbl}';
        """
        df = self._read_sql(query)
        return df['COLUMN_NAME'].to_list()

    def validate_isins(self, isins: list[str], table: str = 'sym_v1.sym_isin') -> list[str]:
        """
        Vérifie que chaque ISIN existe dans la table sym_isin. Retourne la liste valide.
        """
        isin_list = ",".join(f"'{i}'" for i in isins)
        query = f"SELECT ISIN FROM {table} WHERE ISIN IN ({isin_list});"
        df = self._read_sql(query)
        return df['ISIN'].to_list()

    def get_available_dates(self, table: str, date_col: str = 'date') -> list[str]:
        """
        Récupère et retourne toutes les dates disponibles dans une table.
        Utile pour valider les plages demandées.
        """
        query = f"SELECT DISTINCT {date_col} FROM {table} ORDER BY {date_col};"
        df = self._read_sql(query)
        return df[date_col].to_list()

    def get_prices(
        self,
        isins: list[str],
        start_date: str,
        end_date: str,
        fields: list[str],
    ) -> pl.DataFrame:
        """
        Récupère l'historique des prix (fgp).
        """
        valid_isins = self.validate_isins(isins)
        cols = ", ".join(fields)
        isin_list = ",".join(f"'{i}'" for i in valid_isins)
        query = f"""
            SELECT TOP 100 symi.ISIN, fgp.price_date, {cols}
            FROM fgp_v1.fgp_global_prices AS fgp
            JOIN sym_v1.sym_coverage symh ON fgp.fsym_id = symh.FSYM_ID
            JOIN sym_v1.sym_isin symi ON symh.FSYM_SECURITY_ID = symi.FSYM_ID
            WHERE symi.ISIN IN ({isin_list})
              AND fgp.price_date BETWEEN '{start_date}' AND '{end_date}'
        """
        return self._read_sql(query)

    def get_estimates(
        self,
        isins: list[str],
        start_date: str,
        end_date: str,
        fields: list[str],
        table: str = 'fe_v4.fe_basic_qf'
    ) -> pl.DataFrame:
        """
        Récupère les estimations (fe).
        """
        valid_isins = self.validate_isins(isins)
        cols = ", ".join(fields)
        isin_list = ",".join(f"'{i}'" for i in valid_isins)
        query = f"""
            SELECT TOP 100 symi.ISIN, fe.date, {cols}
            FROM {table} AS fe
            JOIN sym_v1.sym_coverage symh ON fe.fsym_id = symh.FSYM_ID
            JOIN sym_v1.sym_isin symi ON symh.FSYM_SECURITY_ID = symi.FSYM_ID
            WHERE symi.ISIN IN ({isin_list})
              AND fe.fe_fp_end BETWEEN '{start_date}' AND '{end_date}'
        """
        return self._read_sql(query)

    def get_fundamentals(
        self,
        isins: list[str],
        start_date: str,
        end_date: str,
        fields: list[str],
        table: str = 'ff_v3.ff_basic_qf'
    ) -> pl.DataFrame:
        """
        Récupère les fondamentaux (ff).
        """
        valid_isins = self.validate_isins(isins)
        cols = ", ".join(fields)
        isin_list = ",".join(f"'{i}'" for i in valid_isins)
        query = f"""
            SELECT TOP 100 symi.ISIN, ff.date, {cols}
            FROM {table} AS ff
            JOIN sym_v1.sym_coverage symh ON ff.fsym_id = symh.FSYM_ID
            JOIN sym_v1.sym_isin symi ON symh.FSYM_SECURITY_ID = symi.FSYM_ID
            WHERE symi.ISIN IN ({isin_list})
              AND ff.date BETWEEN '{start_date}' AND '{end_date}'
        """
        return self._read_sql(query)

    def format_output(
        self,
        df: pl.DataFrame,
        output: str = 'polars',
        structure: str = 'long',
        excel_path: str = None
    ):
        """
        Convertit et/ou exporte le DataFrame selon les paramètres.
        output: 'polars'|'pandas'|'excel'
        structure: 'long'|'wide'
        """
        if structure == 'wide':
            df = df.pivot(
                index='date',
                columns='ISIN',
                values=[c for c in df.columns if c not in ['date','ISIN']]
            )

        if output == 'pandas' or output == 'excel':
            df_pd = df.to_pandas()
            if output == 'excel':
                df_pd.to_excel(excel_path or 'output.xlsx', index=False)
                return excel_path or 'output.xlsx'
            return df_pd
        if output == 'polars':
            if excel_path:
                df.write_excel(excel_path)
            return df
        

    def get_data(self,
                 isins: list[str],
                 start_date: str,
                 end_date: str,
                 columns: list[str]) -> pl.DataFrame:
        """
        Import intelligent selon les colonnes demandées :
        - Identifie les datasets nécessaires
        - Construit et exécute les requêtes
        - Concatène les résultats
        """
        ds_list = self.choose_dataset_by_columns(columns)
        frames = []
        isin_list = ",".join(f"'{i}'" for i in isins)
        cols = ", ".join(columns)
        for ds in ds_list:
            table = self.DATASETS[ds]

            date_col = 'date' if ds != 'prices' else 'price_date'
            query = f"""
                SELECT symi.ISIN, tbl.{date_col}, {cols}
                FROM {table} tbl
                JOIN sym_v1.sym_coverage cov ON tbl.fsym_id = cov.FSYM_ID
                JOIN sym_v1.sym_isin symi ON cov.FSYM_SECURITY_ID = symi.FSYM_ID
                WHERE symi.ISIN IN ({isin_list})
                  AND tbl.{date_col} BETWEEN '{start_date}' AND '{end_date}'
            """
            df = self._read_sql(query).with_columns(pl.lit(ds).alias('dataset'))
            frames.append(df)
        if not frames:
            return pl.DataFrame()

        return pl.concat(frames)

    def join_on_calendar(self,
                         frames: list[pl.DataFrame],
                         calendar_table: str = '[ref_v2].[ref_calendar_dates]') -> pl.DataFrame:
        """
        Joint plusieurs DataFrames sur le calendrier FactSet,
        en laissant des nulls si pas de correspondance.
        """
        cal = self._read_sql(f"SELECT ref_date FROM {calendar_table}")
        result = cal
        for df in frames:

            date_col = 'price_date' if 'price_date' in df.columns else 'date'
            result = result.join(
                df,
                left_on='ref_date',
                right_on=date_col,
                how='left'
            )
        return result


    def fill_missing(
        self,
        df: pl.DataFrame,
        method: str = 'ffill',
        limit: int = None
    ) -> pl.DataFrame:
        """
        Remplit les valeurs manquantes.
        method: 'ffill'|'bfill'|'zero'
        """
        if method == 'ffill':
            return df.fill_null(strategy='forward')
        if method == 'bfill':
            return df.fill_null(strategy='backward')
        if method == 'zero':
            return df.fill_null(0)
        return df

    def close(self):
        """
        Ferme la connexion SQL.
        """
        self.conn.close()




if __name__ == '__main__':
    conn = create_connection()
    orch = DataOrchestrator(conn)
    out = orch.load(
        isins=['US0378331005','FR0000133308'],
        start='2021-01-01',
        end='2021-12-31',
        price_fields=['price','volume','div_spl_spin_adj_factor'],
        fund_fields=['ff_eps_reported','ff_rev_ttm']
    )
    print('Prices shape:', out['prices'].shape)
    print('Fundamentals shape:', out['fundamentals'].shape)
    conn.close()