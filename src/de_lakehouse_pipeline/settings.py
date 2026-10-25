"""Runtime settings, read from the environment and an optional ``.env`` file."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline configuration; nothing is read or created until it is instantiated."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)

    data_source: Literal["synthetic", "kaggle"] = Field(
        default="synthetic", validation_alias="DATA_SOURCE"
    )
    data_dir: Path = Field(default=Path("data"), validation_alias="LAKEHOUSE_DATA_DIR")
    duckdb_path: Path | None = Field(default=None, validation_alias="LAKEHOUSE_DUCKDB_PATH")
    synthetic_orders: int = Field(default=5000, ge=10, validation_alias="SYNTHETIC_ORDERS")
    synthetic_seed: int = Field(default=42, validation_alias="SYNTHETIC_SEED")
    max_reject_ratio: float = Field(default=0.01, ge=0, le=1, validation_alias="MAX_REJECT_RATIO")

    @property
    def source_dir(self) -> Path:
        """Directory holding the source CSVs."""
        return self.data_dir.resolve() / "source"

    @property
    def landing_dir(self) -> Path:
        """Root of the typed landing Parquet files."""
        return self.data_dir.resolve() / "landing"

    @property
    def rejects_dir(self) -> Path:
        """Root of the rejected-row Parquet files."""
        return self.data_dir.resolve() / "rejects"

    @property
    def ingest_log_dir(self) -> Path:
        """Directory of per-run ingest log files."""
        return self.data_dir.resolve() / "_meta" / "ingest_log"

    @property
    def warehouse_path(self) -> Path:
        """DuckDB file written by dbt (Python only reads it)."""
        if self.duckdb_path is not None:
            return self.duckdb_path.resolve()
        return self.data_dir.resolve() / "warehouse" / "lakehouse.duckdb"

    def ensure_dirs(self) -> None:
        """Create the directories Python writes into."""
        for path in (self.source_dir, self.landing_dir, self.rejects_dir, self.ingest_log_dir):
            path.mkdir(parents=True, exist_ok=True)
