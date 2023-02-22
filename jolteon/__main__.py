import typer
import yaml

from jolteon import Config
from jolteon.modules import Updater, print_query_ids
from jolteon.utils import ConnectionParams, get_pg_connection

app = typer.Typer()


@app.command()
def get_ids(connection_params_from_env: bool = False) -> None:
    conn = get_pg_connection(ConnectionParams(params_from_env=connection_params_from_env))
    print_query_ids(conn)


@app.command()
def update_db(config_path: str, connection_params_from_env: bool = False) -> None:
    with open(config_path) as f:
        config = Config(**yaml.safe_load(f))

    conn = get_pg_connection(ConnectionParams(params_from_env=connection_params_from_env))
    updater = Updater(config, conn)
    updater.overwrite_db()


if __name__ == "__main__":
    app()
