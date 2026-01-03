"""Fetch A-share daily data via akshare and store to CSV files."""
from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import akshare as ak
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "market_daily"
SNAPSHOT_PATH = DATA_DIR / "symbols_snapshot.json"
CHANGELOG_PATH = DATA_DIR / "market_symbols_log.md"


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


def load_symbol_snapshot() -> set[str]:
    if not SNAPSHOT_PATH.exists():
        return set()
    return set(pd.read_json(SNAPSHOT_PATH)["symbols"].astype(str).tolist())


def save_symbol_snapshot(symbols: list[str]) -> None:
    snapshot = pd.DataFrame({"symbols": sorted(set(symbols))})
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_json(SNAPSHOT_PATH, orient="records", force_ascii=False, indent=2)


def log_symbol_changes(run_date: str, added: set[str], removed: set[str]) -> None:
    CHANGELOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"## {run_date}",
        "",
        f"- 新增股票: {', '.join(sorted(added)) if added else '无'}",
        f"- 退市股票: {', '.join(sorted(removed)) if removed else '无'}",
        "",
    ]
    with CHANGELOG_PATH.open("a", encoding="utf-8") as file_handle:
        file_handle.write("\n".join(lines))


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y%m%d").date()


def format_date(value: dt.date) -> str:
    return value.strftime("%Y%m%d")


def get_existing_last_date(file_path: Path) -> dt.date | None:
    if not file_path.exists():
        return None
    try:
        df = pd.read_csv(file_path)
    except Exception:
        return None
    if df.empty or "日期" not in df.columns:
        return None
    dates = pd.to_datetime(df["日期"], errors="coerce").dropna()
    if dates.empty:
        return None
    return dates.max().date()


def update_symbol_data(symbol: str, start: str, end: str) -> None:
    file_path = DATA_DIR / f"{symbol}.csv"
    last_date = get_existing_last_date(file_path)
    end_date = parse_date(end)
    if last_date and last_date >= end_date:
        return

    if last_date:
        start_date = max(parse_date(start), last_date + dt.timedelta(days=1))
        start = format_date(start_date)

    data = fetch_daily(symbol, start, end)
    if data.empty:
        return

    if file_path.exists():
        existing = pd.read_csv(file_path)
        merged = pd.concat([existing, data], ignore_index=True)
        if "日期" in merged.columns:
            merged["日期"] = pd.to_datetime(merged["日期"], errors="coerce")
            merged = merged.dropna(subset=["日期"]).sort_values("日期")
            merged["日期"] = merged["日期"].dt.strftime("%Y-%m-%d")
            merged = merged.drop_duplicates(subset=["日期"], keep="last")
        save_symbol_data(symbol, merged, DATA_DIR)
    else:
        save_symbol_data(symbol, data, DATA_DIR)


def update_all_symbols(start: str, end: str, sleep: float) -> None:
    symbols_df = fetch_symbols()
    symbols = symbols_df["代码"].dropna().astype(str).tolist()

    previous_symbols = load_symbol_snapshot()
    current_symbols = set(symbols)
    added = current_symbols - previous_symbols
    removed = previous_symbols - current_symbols
    log_symbol_changes(dt.date.today().strftime("%Y-%m-%d"), added, removed)
    save_symbol_snapshot(symbols)

    for idx, symbol in enumerate(symbols, start=1):
        try:
            update_symbol_data(symbol, start, end)
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
