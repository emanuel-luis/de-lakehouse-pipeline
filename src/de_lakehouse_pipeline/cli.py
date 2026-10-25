"""Command line entry point: ``lakehouse source|ingest|cdc|summary``. Arguments only, no logic."""

import argparse
import logging

from pydantic import ValidationError

from de_lakehouse_pipeline import __version__
from de_lakehouse_pipeline.cdc import simulate
from de_lakehouse_pipeline.contracts import CONTRACTS
from de_lakehouse_pipeline.ingest import ingest
from de_lakehouse_pipeline.settings import MIN_ORDERS, Settings
from de_lakehouse_pipeline.sources import prepare_source
from de_lakehouse_pipeline.sources.kaggle import KaggleUnavailableError
from de_lakehouse_pipeline.summary import summarize


class _RunIdDefault(logging.Filter):
    """Give records logged outside a run a placeholder ``run_id``."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = "-"
        return True


def _orders(value: str) -> int:
    orders = int(value)
    if orders < MIN_ORDERS:
        raise argparse.ArgumentTypeError(
            f"must be at least {MIN_ORDERS}: below that the planted dirt exceeds the reject ratio"
        )
    return orders


def _count(value: str) -> int:
    count = int(value)
    if count < 0:
        raise argparse.ArgumentTypeError("must be zero or more")
    return count


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakehouse")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    source = sub.add_parser("source", help="create the source CSVs (synthetic or Kaggle)")
    source.add_argument("--orders", type=_orders, help="synthetic order count (default: settings)")
    source.add_argument("--seed", type=int, help="synthetic seed (default: settings)")
    source.add_argument(
        "--if-missing", action="store_true", help="do nothing when all source CSVs already exist"
    )
    ing = sub.add_parser("ingest", help="source CSVs -> typed landing Parquet")
    ing.add_argument("--tables", nargs="+", choices=sorted(CONTRACTS), help="default: all")
    ing.add_argument("--mode", choices=("full", "incremental"), default="full")
    cdc = sub.add_parser("cdc", help="change data capture simulation")
    cdc_sub = cdc.add_subparsers(dest="cdc_command", required=True)
    sim = cdc_sub.add_parser("simulate", help="apply order changes and new orders to the source")
    sim.add_argument("--changes", type=_count, default=50, help="orders to move (default: 50)")
    sim.add_argument("--new-orders", type=_count, default=20, help="orders to add (default: 20)")
    sim.add_argument("--seed", type=int, help="seed (default: settings)")
    summ = sub.add_parser("summary", help="last ingest run and mart row counts")
    summ.add_argument("--watermarks", action="store_true", help="also list CDC watermarks")
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

    try:
        if args.command == "source":
            overrides = {}
            if args.orders is not None:
                overrides["synthetic_orders"] = args.orders
            if args.seed is not None:
                overrides["synthetic_seed"] = args.seed
            settings = Settings(**overrides)
            if args.if_missing and all(
                (settings.source_dir / c.source_file).is_file() for c in CONTRACTS.values()
            ):
                print("source CSVs already exist: nothing to do")
                return 0
            paths = prepare_source(settings)
            print(f"wrote {len(paths)} source files")
            return 0
        settings = Settings()
        if args.command == "ingest":
            runs = ingest(settings, args.tables, mode=args.mode)
            return 1 if any(r.status == "failed" for r in runs) else 0
        if args.command == "cdc":
            seed = settings.synthetic_seed if args.seed is None else args.seed
            result = simulate(
                settings.source_dir, changes=args.changes, new_orders=args.new_orders, seed=seed
            )
            moved = " ".join(f"{k}={v}" for k, v in sorted(result.transitions.items())) or "none"
            print(f"clock {result.clock}: moved {moved}; new orders {result.new_orders}")
            return 0
        print(summarize(settings, watermarks=args.watermarks))
        return 0
    except (KaggleUnavailableError, ValidationError, FileNotFoundError) as exc:
        logging.getLogger(__name__).error("%s: %s", type(exc).__name__, exc)
        return 1
