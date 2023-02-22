import json

import pandas as pd
import psycopg2
from pydantic import BaseModel

from jolteon import Config
from jolteon.utils import get_df_from_query


def print_query_ids(conn: psycopg2.extensions.connection) -> None:
    print(
        get_df_from_query(
            """
            SELECT
                saved_query_id,
                name,
                space_id
            FROM saved_queries
            """,
            conn,
        ).to_markdown()
    )


class UpdateParams(BaseModel):
    ids: pd.Series
    new_values: pd.Series
    table_name: str
    field_id_name: str
    field_name: str
    field_type: str

    class Config:
        arbitrary_types_allowed = True


class Updater:
    def __init__(self, config: Config, conn: psycopg2.extensions.connection) -> None:
        self.config = config
        self.conn = conn

    @staticmethod
    def get_where_clause(ids: tuple[int, ...]) -> str:
        if len(ids) == 0:
            return "= -42"
        if len(ids) == 1:
            return f"= {ids[0]}"
        return f"IN {ids}"

    def write_on_postgres(self, tasks: list[UpdateParams]) -> None:
        with self.conn.cursor() as cur:
            for task in tasks:
                zipped_vals = zip(task.ids, task.new_values)
                tuple_to_str = str(tuple(zipped_vals))
                entries_to_update = tuple_to_str[1 : len(tuple_to_str) - 1].strip(",")
                if "[" in task.field_type:
                    entries_to_update = entries_to_update.replace("[", "ARRAY[")
                if entries_to_update:
                    update_sql_query = f"""
                        UPDATE {task.table_name} AS t
                        SET {task.field_name} = v.value::{task.field_type}
                        FROM (VALUES {entries_to_update}) AS v (id, value)
                        WHERE t.{task.field_id_name} = v.id
                    """
                    cur.execute(update_sql_query)
            self.conn.commit()

    def get_saved_queries_version_ids(self) -> tuple[int, ...]:
        return tuple(
            get_df_from_query(
                f"""
                SELECT
                    saved_query_id,
                    MAX(saved_queries_version_id) AS saved_queries_version_id
                FROM saved_queries_versions
                WHERE saved_query_id {self.get_where_clause(self.config.query_ids)}
                GROUP BY 1
                """,
                self.conn,
            )["saved_queries_version_id"]
        )

    def get_fields_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        fields = get_df_from_query(
            f"""
            SELECT *
            FROM saved_queries_version_fields 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        fields["new_name"] = fields["name"].apply(
            lambda x: self.config.fields_mapping.get(
                x.replace(self.config.old_table, self.config.target_table),
                x.replace(self.config.old_table, self.config.target_table),
            )
        )
        return UpdateParams(
            ids=fields["saved_queries_version_field_id"],
            new_values=fields["new_name"],
            table_name="saved_queries_version_fields",
            field_id_name="saved_queries_version_field_id",
            field_name="name",
            field_type="VARCHAR",
        )

    def get_calculations_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        def apply_mapping(s: str) -> str:
            s = s.replace(self.config.old_table, self.config.target_table)
            for k, v in self.config.calculations_mapping.items():
                s = s.replace(k, v)
            return s.replace("\\\\\\\\\\\\\\\\n", "")

        calculations = get_df_from_query(
            f"""
            SELECT *
            FROM saved_queries_version_table_calculations 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        calculations["new_calculations"] = calculations["calculation_raw_sql"].apply(apply_mapping)
        return UpdateParams(
            ids=calculations["saved_queries_version_table_calculation_id"],
            new_values=calculations["new_calculations"],
            table_name="saved_queries_version_table_calculations",
            field_id_name="saved_queries_version_table_calculation_id",
            field_name="calculation_raw_sql",
            field_type="VARCHAR",
        )

    def get_sorts_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        sorts = get_df_from_query(
            f"""
            SELECT *
            FROM saved_queries_version_sorts 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        sorts["new_name"] = sorts["field_name"].apply(
            lambda x: self.config.fields_mapping.get(
                x.replace(self.config.old_table, self.config.target_table),
                x.replace(self.config.old_table, self.config.target_table),
            )
        )
        return UpdateParams(
            ids=sorts["saved_queries_version_sort_id"],
            new_values=sorts["new_name"],
            table_name="saved_queries_version_sorts",
            field_id_name="saved_queries_version_sort_id",
            field_name="field_name",
            field_type="VARCHAR",
        )

    def get_charts_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        def apply_mapping(d: dict) -> str:
            s = json.dumps(d).replace(self.config.old_table, self.config.target_table)
            for k, v in self.config.fields_mapping.items():
                s = s.replace(k, v)
            return s

        charts = get_df_from_query(
            f"""
            SELECT saved_queries_version_id, chart_config
            FROM saved_queries_versions 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        charts["new_chart_config"] = charts["chart_config"].apply(apply_mapping)
        return UpdateParams(
            ids=charts["saved_queries_version_id"],
            new_values=charts["new_chart_config"],
            table_name="saved_queries_versions",
            field_id_name="saved_queries_version_id",
            field_name="chart_config",
            field_type="JSONB",
        )

    def get_filters_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        filters = get_df_from_query(
            f"""
            SELECT saved_queries_version_id, filters
            FROM saved_queries_versions 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        filters["new_filters"] = filters["filters"].apply(
            lambda x: json.dumps(x).replace(self.config.old_table, self.config.target_table)
        )
        return UpdateParams(
            ids=filters["saved_queries_version_id"],
            new_values=filters["new_filters"],
            table_name="saved_queries_versions",
            field_id_name="saved_queries_version_id",
            field_name="filters",
            field_type="JSONB",
        )

    def get_explore_names_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        explore_names = get_df_from_query(
            f"""
            SELECT saved_queries_version_id, explore_name
            FROM saved_queries_versions 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        explore_names["new_explore_name"] = explore_names["explore_name"].apply(
            lambda x: self.config.target_table if x == self.config.old_table else x
        )
        return UpdateParams(
            ids=explore_names["saved_queries_version_id"],
            new_values=explore_names["new_explore_name"],
            table_name="saved_queries_versions",
            field_id_name="saved_queries_version_id",
            field_name="explore_name",
            field_type="VARCHAR",
        )

    def get_pivot_dimensions_to_update(self, ids: tuple[int, ...]) -> UpdateParams:
        pivot_dimensions = get_df_from_query(
            f"""
            SELECT saved_queries_version_id, pivot_dimensions
            FROM saved_queries_versions 
            WHERE saved_queries_version_id {self.get_where_clause(ids)}
            """,
            self.conn,
        )
        pivot_dimensions["new_pivot_dimensions"] = pivot_dimensions["pivot_dimensions"].apply(
            lambda l: [
                self.config.fields_mapping.get(
                    x.replace(self.config.old_table, self.config.target_table),
                    x.replace(self.config.old_table, self.config.target_table),
                )
                for x in l
            ]
            if l is not None
            else None
        )
        pivot_dimensions.dropna(subset="new_pivot_dimensions", inplace=True)
        return UpdateParams(
            ids=pivot_dimensions["saved_queries_version_id"],
            new_values=pivot_dimensions["new_pivot_dimensions"],
            table_name="saved_queries_versions",
            field_id_name="saved_queries_version_id",
            field_name="pivot_dimensions",
            field_type="VARCHAR[]",
        )

    def overwrite_db(self) -> None:
        saved_queries_version_ids = self.get_saved_queries_version_ids()
        self.write_on_postgres(
            [
                self.get_fields_to_update(saved_queries_version_ids),
                self.get_calculations_to_update(saved_queries_version_ids),
                self.get_sorts_to_update(saved_queries_version_ids),
                self.get_charts_to_update(saved_queries_version_ids),
                self.get_filters_to_update(saved_queries_version_ids),
                self.get_explore_names_to_update(saved_queries_version_ids),
                self.get_pivot_dimensions_to_update(saved_queries_version_ids),
            ]
        )
