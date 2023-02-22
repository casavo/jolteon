import typer
import yaml

from jolteon import Config
from jolteon.modules import Updater, print_query_ids
from jolteon.utils import get_connection

app = typer.Typer()


@app.command()
def get_ids() -> None:
    conn = get_connection()
    print_query_ids(conn)


@app.command()
def update_db(config_path: str) -> None:
    with open(config_path) as f:
        config = Config(**yaml.safe_load(f))

    conn = get_connection()
    updater = Updater(config, conn)
    updater.overwrite_db()


if __name__ == "__main__":
    app()
