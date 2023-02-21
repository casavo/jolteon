import pandas as pd
import psycopg2


def get_pg_connection(**kwargs) -> psycopg2.extensions.connection:
    return psycopg2.connect(**kwargs)


def get_df_from_query(query: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql(query, conn)
