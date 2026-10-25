"""Source adapters: produce the nine Olist-named CSVs in the source directory."""

from pathlib import Path

from de_lakehouse_pipeline.settings import Settings


def prepare_source(settings: Settings) -> dict[str, Path]:
    """Create the source CSVs from the configured data source.

    Args:
        settings: Pipeline settings (``data_source`` selects the adapter).

    Returns:
        Landing table name -> CSV path.
    """
    settings.ensure_dirs()
    if settings.data_source == "kaggle":
        from de_lakehouse_pipeline.sources.kaggle import fetch

        return fetch(settings.source_dir)
    from de_lakehouse_pipeline.sources.synthetic import generate

    return generate(
        settings.source_dir, orders=settings.synthetic_orders, seed=settings.synthetic_seed
    )
