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
from typing import Any, Dict

import requests

API_URL = os.environ.get("STANDX_API_URL", "https://perps.standx.com").rstrip("/")
SYMBOLS = [
    s.strip()
    for s in os.environ.get("STANDX_SYMBOLS", "BTC-USD").split(",")
    if s.strip()
]


def _fetch(path: str) -> Any:
    resp = requests.get(f"{API_URL}{path}")
    resp.raise_for_status()
    return resp.json()


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


def main() -> None:
    merged: Dict[str, Dict[str, Any]] = {}

    for symbol in SYMBOLS:
        try:
            ticker = _fetch(f"/api/query_symbol_market?symbol={symbol}")
            info = _extract_ticker_info(ticker)
            merged[symbol] = info
        except requests.HTTPError as exc:  # pragma: no cover - informational
            print(f"failed to fetch market ticker for {symbol}: {exc}")
            continue

        try:
            market_info = _fetch(f"/api/query_symbol_info?symbol={symbol}")
            if isinstance(market_info, list) and market_info:
                merged[symbol].update(
                    {
                        k: v
                        for k, v in _extract_market_info(symbol, market_info[0]).items()
                        if k != "symbol"
                    }
                )
        except requests.HTTPError as exc:  # pragma: no cover - informational
            print(f"failed to fetch symbol info for {symbol}: {exc}")

    print(json.dumps(merged, indent=2, ensure_ascii=False))
    with open("standx_tickers.json", "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
