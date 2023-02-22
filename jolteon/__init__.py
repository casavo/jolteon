import warnings

from pydantic import BaseModel, validator

warnings.filterwarnings("ignore")


class Config(BaseModel):
    old_table: str
    new_table: str | None
    fields_raw_mapping: dict[str, str]
    query_ids: tuple[int, ...]

    @validator("fields_raw_mapping", pre=True)
    def handle_null_fields_raw_mapping(cls, v: dict[str, str] | None) -> dict[str, str]:
        return v or {}

    @validator("query_ids", pre=True)
    def handle_null_query_ids(cls, v: tuple[int, ...] | None) -> tuple[int, ...]:
        return v or ()

    @property
    def target_table(self) -> str:
        return self.new_table or self.old_table

    @property
    def fields_mapping(self) -> dict[str, str]:
        return {f"{self.target_table}_{k}": f"{self.target_table}_{v}" for k, v in self.fields_raw_mapping.items()}

    @property
    def calculations_mapping(self) -> dict[str, str]:
        return {f"{self.target_table}.{k}": f"{self.target_table}.{v}" for k, v in self.fields_raw_mapping.items()}
