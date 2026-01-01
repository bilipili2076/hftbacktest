"""
Fetch StandX perpetual markets and ticker statistics similar to ``1_ticker.py``.

Usage::

    STANDX_API_URL=https://api.standx.com python standx_ticker.py

The script merges public ticker stats with market metadata (tick/lot sizes and
launch time when available), keeps the top symbols by 24h quote volume, and
writes them to ``standx_tickers.json`` for downstream examples.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List

import requests

API_URL = os.environ.get("STANDX_API_URL", "https://api.standx.com").rstrip("/")
NUM_TICKERS = int(os.environ.get("STANDX_NUM_TICKERS", "50"))


def _fetch(path: str) -> Any:
    resp = requests.get(f"{API_URL}{path}")
    resp.raise_for_status()
    return resp.json()


def _index_by_symbol(markets: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for market in markets:
        symbol = market.get("symbol") or market.get("id")
        if isinstance(symbol, str):
            indexed[symbol] = market
    return indexed


def _extract_market_info(symbol: str, market: Dict[str, Any]) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    info["tick_size"] = market.get("tickSize") or market.get("priceIncrement")
    info["lot_size"] = market.get("lotSize") or market.get("sizeIncrement")
    info["min_qty"] = market.get("minQty") or market.get("minSize")

    onboard_ms = (
        market.get("onboardDate")
        or market.get("listedAt")
        or market.get("launchTime")
    )
    if onboard_ms:
        try:
            info["onboard_date"] = datetime.fromtimestamp(float(onboard_ms) / 1000).strftime(
                "%Y%m%d"
            )
        except (TypeError, ValueError):
            pass

    if market.get("baseAsset"):
        info["base_asset"] = market.get("baseAsset")
    if market.get("quoteAsset"):
        info["quote_asset"] = market.get("quoteAsset")

    if info.get("lot_size") is None and market.get("contractSize"):
        info["lot_size"] = market.get("contractSize")
    if info.get("min_qty") is None and market.get("minNotional"):
        info["min_qty"] = market.get("minNotional")

    if info.get("tick_size") is None and market.get("tick"):
        info["tick_size"] = market.get("tick")

    info = {k: v for k, v in info.items() if v is not None}
    if not info:
        return {"symbol": symbol}
    info["symbol"] = symbol
    return info


def _extract_ticker_info(ticker: Dict[str, Any]) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    info["weighted_avg_price"] = ticker.get("weightedAvgPrice") or ticker.get("markPrice")
    info["quote_volume"] = ticker.get("quoteVolume") or ticker.get("quoteVolume24h")
    info["last_price"] = ticker.get("lastPrice") or ticker.get("price")
    info["symbol"] = ticker.get("symbol") or ticker.get("id")

    if info["quote_volume"] is None and ticker.get("volumeUsd"):
        info["quote_volume"] = ticker.get("volumeUsd")

    return {k: v for k, v in info.items() if v is not None}


def _is_alt_symbol(symbol: str) -> bool:
    upper = symbol.upper()
    return not (upper.startswith("BTC") or upper.startswith("ETH"))


def main() -> None:
    tickers: List[Dict[str, Any]] = _fetch("/perps/public/tickers")
    markets: List[Dict[str, Any]] = _fetch("/perps/public/markets")

    markets_by_symbol = _index_by_symbol(markets)

    merged: Dict[str, Dict[str, Any]] = {}
    for ticker in tickers:
        info = _extract_ticker_info(ticker)
        symbol = info.get("symbol")
        if not symbol:
            continue
        merged[symbol] = info

    for symbol, market in markets_by_symbol.items():
        entry = merged.setdefault(symbol, {"symbol": symbol})
        entry.update({k: v for k, v in _extract_market_info(symbol, market).items() if k != "symbol"})

    sorted_tickers = sorted(
        merged.items(),
        key=lambda item: float(item[1].get("quote_volume", 0) or 0.0),
        reverse=True,
    )

    alts = [(symbol, info) for symbol, info in sorted_tickers if _is_alt_symbol(symbol)]
    top = dict(alts[:NUM_TICKERS])

    print(json.dumps(top, indent=2, ensure_ascii=False))
    with open("standx_tickers.json", "w", encoding="utf-8") as f:
        json.dump(top, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
