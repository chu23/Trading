"""Run strategy signal evaluation on stored market data."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "market_daily"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "outputs"


@dataclass
class Signal:
    symbol: str
    buy_price: float
    buy_qty: int
    take_profit: float
    stop_loss: float


def compute_signal(df: pd.DataFrame, symbol: str, capital_per_trade: float) -> Signal | None:
    """Example strategy placeholder using moving averages.

    Replace this logic with the Notion oil-tube strategy rules.
    """
    if df.empty or len(df) < 30:
        return None

    df = df.copy()
    df["close"] = pd.to_numeric(df["收盘"], errors="coerce")
    df = df.dropna(subset=["close"])
    if len(df) < 30:
        return None

    df["ma5"] = df["close"].rolling(5).mean()
    df["ma20"] = df["close"].rolling(20).mean()

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    crossover = prev["ma5"] <= prev["ma20"] and latest["ma5"] > latest["ma20"]
    if not crossover:
        return None

    buy_price = float(latest["close"])
    buy_qty = int(capital_per_trade // buy_price)
    if buy_qty <= 0:
        return None

    take_profit = round(buy_price * 1.1, 2)
    stop_loss = round(buy_price * 0.95, 2)

    return Signal(
        symbol=symbol,
        buy_price=buy_price,
        buy_qty=buy_qty,
        take_profit=take_profit,
        stop_loss=stop_loss,
    )


def load_symbol_data(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(file_path)


def run_signals(capital_per_trade: float) -> list[Signal]:
    signals: list[Signal] = []
    for file_path in sorted(DATA_DIR.glob("*.csv")):
        df = load_symbol_data(file_path)
        signal = compute_signal(df, file_path.stem, capital_per_trade)
        if signal:
            signals.append(signal)
    return signals


def save_signals(signals: list[Signal], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "signals.json"
    payload = [signal.__dict__ for signal in signals]
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate strategy signals")
    parser.add_argument("--capital", type=float, default=100000, help="Capital per trade")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signals = run_signals(args.capital)
    path = save_signals(signals, OUTPUT_DIR)
    print(f"Saved {len(signals)} signals to {path}")


if __name__ == "__main__":
    main()
