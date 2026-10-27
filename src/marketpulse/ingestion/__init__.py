"""Historical market data ingestion from Yahoo Finance.

The connector and per-symbol service that land daily OHLCV bars as Parquet
in ``data/raw/``.

See docs/adr/0003-why-yahoo-finance-for-initial-ingestion.md for the source
choice and its operational limitations, and
docs/data-contracts/market_prices.md for the landed schema and known
limitations (notably, each run performs a full-refresh overwrite rather than
an incremental append).

Future connectors (FRED, a second equity source) each own their source's
quirks -- adjustment conventions, timestamp semantics, pagination -- behind
their own module, so a vendor changing its API breaks one connector rather
than the platform. No shared connector interface exists yet; one concrete
connector does not justify an abstraction built for a hypothetical second.
"""

from __future__ import annotations

from marketpulse.ingestion.service import SymbolIngestionResult, run_ingestion

__all__ = ["SymbolIngestionResult", "run_ingestion"]
