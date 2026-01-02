"""Backtest using signals from strategy_executor."""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "market_daily"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "outputs"


@dataclass
class TradeResult:
    symbol: str
    buy_price: float
    buy_qty: int
    take_profit: float
    stop_loss: float
    sell_price: float
    pnl: float
    return_pct: float


def load_signals(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def evaluate_trade(signal: dict, hold_days: int) -> TradeResult | None:
    symbol = signal["symbol"]
    file_path = DATA_DIR / f"{symbol}.csv"
    if not file_path.exists():
        return None

    df = pd.read_csv(file_path)
    if df.empty:
        return None

    df["close"] = pd.to_numeric(df["收盘"], errors="coerce")
    df = df.dropna(subset=["close"])
    if df.empty:
        return None

    buy_price = float(signal["buy_price"])
    buy_qty = int(signal["buy_qty"])
    take_profit = float(signal["take_profit"])
    stop_loss = float(signal["stop_loss"])

    buy_index = df.index[-(hold_days + 1)] if len(df) > hold_days else df.index[0]
    future_slice = df.loc[buy_index + 1 :].head(hold_days)

    if future_slice.empty:
        return None

    sell_price = float(future_slice.iloc[-1]["close"])
    for _, row in future_slice.iterrows():
        price = float(row["close"])
        if price >= take_profit:
            sell_price = take_profit
            break
        if price <= stop_loss:
            sell_price = stop_loss
            break

    pnl = (sell_price - buy_price) * buy_qty
    return_pct = (sell_price - buy_price) / buy_price

    return TradeResult(
        symbol=symbol,
        buy_price=buy_price,
        buy_qty=buy_qty,
        take_profit=take_profit,
        stop_loss=stop_loss,
        sell_price=sell_price,
        pnl=pnl,
        return_pct=return_pct,
    )


def summarize_results(results: list[TradeResult]) -> dict:
    if not results:
        return {
            "trades": 0,
            "total_pnl": 0.0,
            "win_rate": 0.0,
            "profit_loss_ratio": 0.0,
            "sharpe": 0.0,
        }

    pnls = np.array([r.pnl for r in results])
    returns = np.array([r.return_pct for r in results])
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]

    profit_loss_ratio = float(wins.mean() / abs(losses.mean())) if losses.size else float("inf")
    sharpe = float(returns.mean() / returns.std(ddof=1)) if returns.size > 1 else 0.0

    return {
        "trades": len(results),
        "total_pnl": float(pnls.sum()),
        "win_rate": float((pnls > 0).mean()),
        "profit_loss_ratio": profit_loss_ratio,
        "sharpe": sharpe,
    }


def save_report(results: list[TradeResult], summary: dict, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "backtest_report.csv"
    summary_path = output_dir / "backtest_summary.json"

    df = pd.DataFrame([r.__dict__ for r in results])
    df.to_csv(report_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    print(f"Saved report to {report_path}")
    print(f"Saved summary to {summary_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest with strategy signals")
    parser.add_argument("--signals", default=str(OUTPUT_DIR / "signals.json"))
    parser.add_argument("--hold-days", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signals = load_signals(Path(args.signals))
    results = []
    for signal in signals:
        result = evaluate_trade(signal, args.hold_days)
        if result:
            results.append(result)

    summary = summarize_results(results)
    save_report(results, summary, OUTPUT_DIR)


if __name__ == "__main__":
    main()
