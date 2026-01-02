"""Fetch A-share daily data via akshare and store to CSV files."""
from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import akshare as ak
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "market_daily"


def fetch_symbols() -> pd.DataFrame:
    """Fetch A-share spot list."""
    return ak.stock_zh_a_spot_em()


def fetch_daily(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily history for a symbol."""
    return ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start,
        end_date=end,
        adjust="qfq",
    )


def save_symbol_data(symbol: str, data: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{symbol}.csv"
    data.to_csv(file_path, index=False)
    return file_path


def update_all_symbols(start: str, end: str, sleep: float) -> None:
    symbols_df = fetch_symbols()
    symbols = symbols_df["代码"].dropna().astype(str).tolist()

    for idx, symbol in enumerate(symbols, start=1):
        try:
            data = fetch_daily(symbol, start, end)
            if data.empty:
                continue
            save_symbol_data(symbol, data, DATA_DIR)
        except Exception as exc:  # pragma: no cover - basic logging
            print(f"[WARN] {symbol} failed: {exc}")
        if sleep > 0:
            time.sleep(sleep)
        if idx % 200 == 0:
            print(f"Processed {idx}/{len(symbols)} symbols")


def parse_args() -> argparse.Namespace:
    today = dt.date.today()
    default_start = (today - dt.timedelta(days=365)).strftime("%Y%m%d")
    default_end = today.strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="Fetch A-share daily data via akshare")
    parser.add_argument("--start", default=default_start, help="Start date YYYYMMDD")
    parser.add_argument("--end", default=default_end, help="End date YYYYMMDD")
    parser.add_argument("--sleep", type=float, default=0.5, help="Sleep seconds between symbols")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    update_all_symbols(args.start, args.end, args.sleep)


if __name__ == "__main__":
    main()
