"""Fetch A-share daily data via akshare and store to CSV files."""
from __future__ import annotations

import argparse
import datetime as dt
import random
import time
from pathlib import Path

import akshare as ak
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "market_daily"
SNAPSHOT_PATH = DATA_DIR / "symbols_snapshot.json"
CHANGELOG_PATH = DATA_DIR / "market_symbols_log.md"
ALT_SCREEN_ENTER = "\033[?1049h"
ALT_SCREEN_EXIT = "\033[?1049l"


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
        with file_path.open("r", encoding="utf-8") as handle:
            header = handle.readline().strip()
            if not header:
                return None
            columns = header.split(",")
            if "日期" not in columns:
                return None
            date_index = columns.index("日期")

            handle.seek(0, 2)
            position = handle.tell()
            if position <= len(header):
                return None
            buffer = ""
            while position > 0:
                position -= 1
                handle.seek(position)
                char = handle.read(1)
                if char == "\n" and buffer:
                    break
                buffer = char + buffer
            last_line = buffer.strip()
            if not last_line or last_line == header:
                return None
            values = last_line.split(",")
            if date_index >= len(values):
                return None
            last_date = pd.to_datetime(values[date_index], errors="coerce")
            if pd.isna(last_date):
                return None
            return last_date.date()
    except Exception:
        return None


def update_symbol_data(symbol: str, start: str, end: str) -> tuple[int, int]:
    file_path = DATA_DIR / f"{symbol}.csv"
    existing_rows = 0
    new_rows = 0
    last_date = get_existing_last_date(file_path)
    end_date = parse_date(end)
    if last_date and last_date >= end_date:
        return 0, 0

    if last_date:
        start_date = max(parse_date(start), last_date + dt.timedelta(days=1))
        start = format_date(start_date)

    data = fetch_daily(symbol, start, end)
    if data.empty:
        return 0, 0

    new_rows = len(data)

    if file_path.exists():
        existing = pd.read_csv(file_path)
        existing_rows = len(existing)
        merged = pd.concat([existing, data], ignore_index=True)
        if "日期" in merged.columns:
            merged["日期"] = pd.to_datetime(merged["日期"], errors="coerce")
            merged = merged.dropna(subset=["日期"]).sort_values("日期")
            merged["日期"] = merged["日期"].dt.strftime("%Y-%m-%d")
            merged = merged.drop_duplicates(subset=["日期"], keep="last")
        save_symbol_data(symbol, merged, DATA_DIR)
    else:
        save_symbol_data(symbol, data, DATA_DIR)
    return existing_rows, new_rows


def update_all_symbols(start: str, end: str, sleep: float) -> None:
    print(f"Data files will be saved to: {DATA_DIR.resolve()}")
    symbols_df = fetch_symbols()
    symbols = symbols_df["代码"].dropna().astype(str).tolist()

    previous_symbols = load_symbol_snapshot()
    current_symbols = set(symbols)
    added = current_symbols - previous_symbols
    removed = previous_symbols - current_symbols
    log_symbol_changes(dt.date.today().strftime("%Y-%m-%d"), added, removed)
    save_symbol_snapshot(symbols)

    total_existing = 0
    total_new = 0
    total_symbols = len(symbols)

    for idx, symbol in enumerate(symbols, start=1):
        try:
            existing_rows, new_rows = update_symbol_data(symbol, start, end)
            total_existing += existing_rows
            total_new += new_rows
        except Exception as exc:  # pragma: no cover - basic logging
            print(f"[WARN] {symbol} failed: {exc}")
        if sleep > 0:
            time.sleep(random.uniform(sleep * 0.5, sleep * 1.5))
        if idx % 10 == 0 or idx == total_symbols:
            percent = (idx / total_symbols) * 100 if total_symbols else 100
            print(
                "Progress:"
                f" {idx}/{total_symbols} symbols"
                f" ({percent:.1f}%) |"
                f" existing rows {total_existing} |"
                f" new rows {total_new}"
            )


def parse_args() -> argparse.Namespace:
    today = dt.date.today()
    default_start = (today - dt.timedelta(days=365 * 5)).strftime("%Y%m%d")
    default_end = today.strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="Fetch A-share daily data via akshare")
    parser.add_argument("--start", default=default_start, help="Start date YYYYMMDD")
    parser.add_argument("--end", default=default_end, help="End date YYYYMMDD")
    parser.add_argument("--sleep", type=float, default=0.5, help="Sleep seconds between symbols")
    return parser.parse_args()


def enter_alt_screen() -> None:
    print(ALT_SCREEN_ENTER, end="", flush=True)


def exit_alt_screen() -> None:
    print(ALT_SCREEN_EXIT, end="", flush=True)


def main() -> None:
    args = parse_args()
    enter_alt_screen()
    try:
        update_all_symbols(args.start, args.end, args.sleep)
    finally:
        exit_alt_screen()


if __name__ == "__main__":
    main()
