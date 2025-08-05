import polars as pl
import pandas as pd
from typing import List, Dict, Optional, Tuple
import warnings

class FundamentalDataLoader:
    """
    Chargement des données fondamentales avec résolution de provenance des champs
    (priorisation de tables, inspection des tables contenant un champ, etc.).
    """

    TABLE_PRIORITY_MAP = {          #Mapping for all priority tables, always search in basic before advanced
    "qf": [
        'ff_v3.ff_basic_qf', 'ff_v3.ff_advanced_qf','ff_v3.ff_basic_der_qf', 'ff_v3.ff_advanced_der_qf'
    ],
    "af": [
        'ff_v3.ff_basic_af', 'ff_v3.ff_advanced_af','ff_v3.ff_basic_der_af', 'ff_v3.ff_advanced_der_af'
    ],
    "ltm": [
        'ff_v3.ff_basic_ltm', 'ff_v3.ff_advanced_ltm','ff_v3.ff_basic_der_ltm', 'ff_v3.ff_advanced_der_ltm'
    ],
    "ytd": [
        'ff_v3.ff_basic_ytd', 'ff_v3.ff_advanced_ytd','ff_v3.ff_basic_der_ytd', 'ff_v3.ff_advanced_der_ytd'
    ],
    "saf": [
        'ff_v3.ff_basic_af', 'ff_v3.ff_advanced_af','ff_v3.ff_basic_der_af', 'ff_v3.ff_advanced_der_af'
    ],
    }

    def __init__(
        self,
        conn,
        schema: str = 'ff_v3'
    ):
        self.conn = conn
        self.schema = schema
        self.fundamental_tables = self._load_all_tables(schema)
        self._table_columns_cache: Dict[str, set[str]] = {}

    def _load_all_tables(self, schema: str) -> List[str]:
        """
        Charge toutes les tables du schéma donné.
        """
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type = 'BASE TABLE'
        """
        pdf = pd.read_sql(query, self.conn)
        return [f"{schema}.{tbl}" for tbl in pdf['table_name']]


    def _parse_table_fullname(self, full: str) -> Tuple[str, str]:
        """
        Sépare 'schema.table' en (schema, table). Si pas de '.', on laisse schema NULL selon la base.
        """
        if '.' in full:
            schema, tbl = full.split('.', 1)
            return schema, tbl
        return None, full

    def _load_table_columns(self, table: str) -> set[str]:
        """
        Charge et met en cache les colonnes d'une table via information_schema.
        ATTENTION : adaptation possible selon ton SGBD si ce n'est pas du Postgres-compatible.
        """
        if table in self._table_columns_cache:
            return self._table_columns_cache[table]

        schema, tbl = self._parse_table_fullname(table)
        if schema:
            query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = '{tbl}'
            """
        else:
            query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{tbl}'
            """

        pdf = pd.read_sql(query, self.conn)
        cols = set(col.lower() for col in pdf['column_name'] if isinstance(col, str))
        self._table_columns_cache[table] = cols
        return cols

    def field_locations(self, fields: List[str]) -> Dict[str, List[str]]:
        """
        Pour chaque champ demandé, retourne la liste des tables où il existe.
        """
        mapping: Dict[str, List[str]] = {}
        lowered_fields = [f.lower() for f in fields]
        for field, lower in zip(fields, lowered_fields):
            present_in = []
            for table in self.fundamental_tables:
                cols = self._load_table_columns(table)
                if lower in cols:
                    present_in.append(table)
            mapping[field] = present_in
        return mapping

    def _resolve_field_table_mapping(self, fields: List[str], frequency: str, fallback: bool) -> Dict[str, str]:
        """
        Pour chaque champ, choisit la table source selon la priorité.
        Si fallback=False : on regarde uniquement dans les tables prioritaires.
        Si fallback=True : on autorise n'importe quelle table si non trouvée en priorité.
        """
        
        table_priority = self.TABLE_PRIORITY_MAP.get(frequency.lower(), [])
        locations = self.field_locations(fields)
        resolved: Dict[str, str] = {}

        for field, tables in locations.items():
            """if fallback:
                for preferred in table_priority:
                    if preferred in tables:
                        resolved[field] = preferred
                        break
                else:
                    if tables:
                        resolved[field] = tables[0]
                    else:
                        warnings.warn(
                            f"Le champ '{field}' n'est trouve dans aucune table. Il sera rempli avec des valeurs NULL."
                        )
                        resolved[field] = None"""
            if fallback:
                for preferred in table_priority:
                    if preferred in tables:
                        resolved[field] = preferred
                        break
                else:
                    if tables:
                        resolved[field] = tables[0]
                    else:
                        resolved[field] = None

            else:
                filtered = [t for t in tables if t in table_priority]
                if filtered:
                    resolved[field] = filtered[0]
                else:
                    warnings.warn(
                        f"Le champ '{field}' n'est pas trouve dans les tables prioritaires {table_priority}. Il sera ignore."
                    )
                    resolved[field] = None

        return resolved

    def get_fundamentals(
        self,
        isins: List[str],
        start_date: str,
        end_date: str,
        fields: List[str],
        frequency:str = "qf",
        fallback:bool = False
    ) -> Dict[str, object]:
        """
        Retrieve raw fundamental data for a list of ISINs over a given time period and list of fields.

        This method dynamically resolves which tables contain the requested fundamental fields 
        based on the specified frequency (e.g. quarterly, annual). It fetches data from multiple 
        tables (if needed), handling missing fields with optional fallback logic, and returns a 
        list of DataFrames grouped by table source. The result is **not aligned on a calendar**.

        Parameters
        ----------
        isins : List[str]
            List of ISINs for which to retrieve data.

        start_date : str
            Start of the date range (inclusive), in 'YYYY-MM-DD' format.

        end_date : str
            End of the date range (inclusive), in 'YYYY-MM-DD' format.

        fields : List[str]
            List of fundamental fields to retrieve (e.g., ['ff_sales', 'ff_eps']).

        frequency : str, default="qf"
            Frequency tag used to prioritize table selection ('qf', 'af', 'cf', etc.).

        fallback : bool, default=False
            If True, fields not found in frequency-matching tables will be searched 
            in all other available tables. If False, such fields are filled with nulls.

        Returns
        -------
        Dict[str, object]
            A dictionary with:
            - "dataframes": List[pl.DataFrame], one per source table, each containing ISIN, date, and fields.
            - "field_table_map": Dict[str, str or None], mapping each requested field to the table it came from 
            (or None if not found).

        Examples
        --------
        >>> result = fund_loader.get_fundamentals(
        ...     isins=["US2910111044", "US2220702037"],
        ...     start_date="2020-01-01",
        ...     end_date="2022-12-31",
        ...     fields=["ff_sales", "ff_eps", "ff_report_date"],
        ...     frequency="af",
        ...     fallback=True
        ... )
        
        >>> result["dataframes"][0].head()
        shape: (5, 5)
        ┌────────────┬────────────┬──────────┬─────────┬────────────────┐
        │ ISIN       │ date       │ ff_sales │ ff_eps  │ ff_report_date │
        └────────────┴────────────┴──────────┴─────────┴────────────────┘

        >>> result["field_table_map"]
        {'ff_sales': 'ff_v3.ff_basic_af', 'ff_eps': 'ff_v3.ff_basic_af', 'ff_report_date': 'ff_v3.ff_basic_af'}
        """
        if not fields:
            return {'dataframes': [], 'field_table_map': {}}

        resolved = self._resolve_field_table_mapping(fields,frequency,fallback)

        table_to_fields: Dict[str, List[str]] = {}
        missing_fields: List[str] = []

        for field, table in resolved.items():
            if table:
                table_to_fields.setdefault(table, []).append(field)
            else:
                missing_fields.append(field)

        dfs: List[pl.DataFrame] = []
        isin_list = ','.join(f"'{i}'" for i in isins)

        for table, fields_in_table in table_to_fields.items():
            cols = ', '.join(fields_in_table)
            query = f"""
                SELECT symi.ISIN, ff.date, {cols}
                FROM {table} ff
                JOIN sym_v1.sym_coverage cov ON ff.fsym_id = cov.FSYM_ID
                JOIN sym_v1.sym_isin symi ON cov.FSYM_SECURITY_ID = symi.FSYM_ID
                WHERE symi.ISIN IN ({isin_list})
                  AND ff.date BETWEEN '{start_date}' AND '{end_date}'
            """
            pdf = pd.read_sql(query, self.conn)
            if pdf.empty:
                continue
            df = pl.from_pandas(pdf)
            dfs.append(df)

        if missing_fields:
            if dfs:
                for i in range(len(dfs)):
                    for mf in missing_fields:
                        dfs[i] = dfs[i].with_columns(pl.lit(None).alias(mf))
            else:
                df_empty = pl.DataFrame({mf: [None] for mf in missing_fields})
                dfs.append(df_empty)

        return {'dataframes': dfs, 'field_table_map': resolved}
