import sys

import pandas as pd
import psycopg2
from pydantic import BaseSettings


class ConnectionParams(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"

    class Config:
        env_file = ".env"
        env_prefix = "pg_"


def get_connection() -> psycopg2.extensions.connection:
    params = ConnectionParams()
    while True:
        ans = input(
            f"You are connecting to {params.host}:{params.port}/{params.database}. Do you want to continue? y/n "
        )
        if ans.lower() == "y":
            return psycopg2.connect(
                host=params.host,
                port=params.port,
                database=params.database,
                user=params.user,
                password=params.password,
            )
        if ans.lower() == "n":
            sys.exit(0)


def get_df_from_query(query: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql(query, conn)
