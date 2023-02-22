import os

import pandas as pd
import psycopg2
from pydantic import BaseModel, root_validator


class ConnectionParams(BaseModel):
    params_from_env: bool = False
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"

    @root_validator(pre=True)
    def populate_params(cls, values: dict) -> dict:
        if values["params_from_env"]:
            for k, v in cls.schema()["properties"].items():
                if k != "params_from_env":
                    values[k] = os.getenv(f"PG_{k.upper()}", v["default"])
        return values


def get_pg_connection(
    connection_params: ConnectionParams,
) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=connection_params.host,
        port=connection_params.port,
        database=connection_params.database,
        user=connection_params.user,
        password=connection_params.password,
    )


def get_df_from_query(query: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    return pd.read_sql(query, conn)
