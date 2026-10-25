"""Allow ``python -m de_lakehouse_pipeline``."""

import sys

from de_lakehouse_pipeline.cli import main

sys.exit(main())
