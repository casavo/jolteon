import typer
import yaml

from jolteon import Config
from jolteon.modules import Updater, print_query_ids
from jolteon.utils import get_pg_connection

app = typer.Typer()


@app.command()
def get_ids(
    pg_host: str = "localhost",
    pg_port: str = "5432",
    pg_database: str = "postgres",
    pg_user: str = "postgres",
    pg_password: str = "postgres",
) -> None:
    conn = get_pg_connection(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password,
    )
    print_query_ids(conn)


@app.command()
def update_db(
    config_path: str,
    pg_host: str = "localhost",
    pg_port: str = "5432",
    pg_database: str = "postgres",
    pg_user: str = "postgres",
    pg_password: str = "postgres",
) -> None:
    with open(config_path) as f:
        config = Config(**yaml.safe_load(f))

    conn = get_pg_connection(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password,
    )
    updater = Updater(config, conn)
    updater.overwrite_db()


if __name__ == "__main__":
    app()
