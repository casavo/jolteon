import pandas as pd
import psycopg2


def get_pg_connection(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


def get_df_from_query(query: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql(query, conn)
