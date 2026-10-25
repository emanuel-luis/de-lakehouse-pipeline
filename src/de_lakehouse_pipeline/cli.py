"""Command line entry point: ``lakehouse source|ingest|summary``. Arguments only, no logic."""

import argparse
import logging

from de_lakehouse_pipeline import __version__
from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.ingest import ingest
from de_lakehouse_pipeline.settings import Settings
from de_lakehouse_pipeline.sources import prepare_source
from de_lakehouse_pipeline.summary import summarize


class _RunIdDefault(logging.Filter):
    """Give records logged outside a run a placeholder ``run_id``."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = "-"
        return True


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakehouse")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    source = sub.add_parser("source", help="create the source CSVs (synthetic or Kaggle)")
    source.add_argument("--orders", type=int, help="synthetic order count (default: settings)")
    source.add_argument("--seed", type=int, help="synthetic seed (default: settings)")
    ing = sub.add_parser("ingest", help="source CSVs -> typed landing Parquet")
    ing.add_argument("--tables", nargs="+", choices=sorted(CONTRACTS), help="default: all")
    sub.add_parser("summary", help="last ingest run and mart row counts")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI.

    Args:
        argv: Arguments to parse; defaults to ``sys.argv[1:]``.

    Returns:
        Process exit code: 1 if any table failed to ingest, else 0.
    """
    args = _build_parser().parse_args(argv)
    handler = logging.StreamHandler()
    handler.addFilter(_RunIdDefault())
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(run_id)s] %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)

    if args.command == "source":
        overrides = {}
        if args.orders is not None:
            overrides["synthetic_orders"] = args.orders
        if args.seed is not None:
            overrides["synthetic_seed"] = args.seed
        paths = prepare_source(Settings(**overrides))
        print(f"wrote {len(paths)} source files")
        return 0
    settings = Settings()
    if args.command == "ingest":
        runs = ingest(settings, args.tables)
        return 1 if any(r.status == "failed" for r in runs) else 0
    print(summarize(settings))
    return 0
