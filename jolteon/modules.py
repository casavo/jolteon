import json

import pandas as pd
import psycopg2

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


class Updater:
    def __init__(self, config: Config, conn: psycopg2.extensions.connection) -> None:
        self.config = config
        self.conn = conn

    @staticmethod
    def get_where_clause(ids: tuple[int, ...]):
        if len(ids) == 0:
            return "= -42"
        if len(ids) == 1:
            return f"= {ids[0]}"
        return f"IN {ids}"

    def write_on_postgres(
        self,
        ids: pd.Series,
        new_values: pd.Series,
        table_name: str,
        field_id_name: str,
        field_name: str,
        field_type: str,
    ) -> None:
        zipped_vals = zip(ids, new_values)
        tuple_to_str = str(tuple(zipped_vals))
        entries_to_update = tuple_to_str[1 : len(tuple_to_str) - 1].strip(",")
        if entries_to_update:
            with self.conn.cursor() as cur:
                update_sql_query = f"""
                    UPDATE {table_name} AS t
                    SET {field_name} = v.value::{field_type}
                    FROM (VALUES {entries_to_update}) AS v (id, value)
                    WHERE t.{field_id_name} = v.id
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

    def get_fields_to_update(self, ids: tuple[int, ...]) -> pd.DataFrame:
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
        return fields

    def get_calculations_to_update(self, ids: tuple[int, ...]) -> pd.DataFrame:
        def apply_mapping(s: str):
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
        return calculations

    def get_sorts_to_update(self, ids: tuple[int, ...]) -> pd.DataFrame:
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
        return sorts

    def get_charts_to_update(self, ids: tuple[int, ...]) -> pd.DataFrame:
        def apply_mapping(d: dict):
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
        return charts

    def get_filters_to_update(self, ids: tuple[int, ...]) -> pd.DataFrame:
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
        return filters

    def get_explore_names_to_update(self, ids: tuple[int, ...]) -> pd.DataFrame:
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
        return explore_names

    def overwrite_db(self) -> None:
        saved_queries_version_ids = self.get_saved_queries_version_ids()
        fields = self.get_fields_to_update(saved_queries_version_ids)
        self.write_on_postgres(
            fields["saved_queries_version_field_id"],
            fields["new_name"],
            "saved_queries_version_fields",
            "saved_queries_version_field_id",
            "name",
            "VARCHAR",
        )
        calculations = self.get_calculations_to_update(saved_queries_version_ids)
        self.write_on_postgres(
            calculations["saved_queries_version_table_calculation_id"],
            calculations["new_calculations"],
            "saved_queries_version_table_calculations",
            "saved_queries_version_table_calculation_id",
            "calculation_raw_sql",
            "VARCHAR",
        )
        sorts = self.get_sorts_to_update(saved_queries_version_ids)
        self.write_on_postgres(
            sorts["saved_queries_version_sort_id"],
            sorts["new_name"],
            "saved_queries_version_sorts",
            "saved_queries_version_sort_id",
            "field_name",
            "VARCHAR",
        )
        charts = self.get_charts_to_update(saved_queries_version_ids)
        self.write_on_postgres(
            charts["saved_queries_version_id"],
            charts["new_chart_config"],
            "saved_queries_versions",
            "saved_queries_version_id",
            "chart_config",
            "JSONB",
        )
        filters = self.get_filters_to_update(saved_queries_version_ids)
        self.write_on_postgres(
            filters["saved_queries_version_id"],
            filters["new_filters"],
            "saved_queries_versions",
            "saved_queries_version_id",
            "filters",
            "JSONB",
        )
        explore_names = self.get_explore_names_to_update(saved_queries_version_ids)
        self.write_on_postgres(
            explore_names["saved_queries_version_id"],
            explore_names["new_explore_name"],
            "saved_queries_versions",
            "saved_queries_version_id",
            "explore_name",
            "VARCHAR",
        )
