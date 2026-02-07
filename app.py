import datetime
import logging
import os
import threading
import time

import csv
import json
import requests as http_requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USERS = {
    "Nikhil": "ZETA",
    "Achu": "ASTS",
    "Vishal": "GLDM",
    "Siddu": "COIN",
    "Singh": "MU",
    "Satwik": "HOOD",
    "Adi": "AMZN",
    "Nari": "SOFI",
    "Saurya": "GME",
}

BENCHMARKS = ["SPY", "VT", "VTI"]
CRYPTO_ADJACENT = {"COIN", "HOOD"}

# TradingView needs exchange-qualified symbols
TV_SYMBOLS = {
    "ZETA": "NYSE:ZETA",
    "ASTS": "NASDAQ:ASTS",
    "GLDM": "AMEX:GLDM",
    "COIN": "NASDAQ:COIN",
    "MU": "NASDAQ:MU",
    "HOOD": "NASDAQ:HOOD",
    "AMZN": "NASDAQ:AMZN",
    "SOFI": "NASDAQ:SOFI",
    "GME": "NYSE:GME",
    "SPY": "AMEX:SPY",
    "VT": "AMEX:VT",
    "VTI": "AMEX:VTI",
}

STARTING_BALANCE = 1000.0
CACHE_TTL = 3600  # 1 hour

STOOQ_BASE = "https://stooq.com/q/d/l/"
CACHE_FILE = "/tmp/stock_cache.json"

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
_cache: dict = {"data": None, "ts": 0, "loading": False}
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def _fetch_tradingview_batch(tickers: list[str]) -> dict[str, dict]:
    """Fetch current close + YTD% for all tickers in ONE TradingView API call.

    Returns {ticker: {"close": float, "ytd_pct": float}} e.g. {"AAPL": {"close": 278, "ytd_pct": 2.15}}
    """
    tv_tickers = [TV_SYMBOLS[t] for t in tickers if t in TV_SYMBOLS]
    if not tv_tickers:
        return {}

    url = "https://scanner.tradingview.com/america/scan"
    payload = {
        "symbols": {"tickers": tv_tickers},
        "columns": ["close", "Perf.YTD"],
    }

    try:
        resp = http_requests.post(url, json=payload, timeout=20)
        data = resp.json()

        result: dict[str, dict] = {}
        for row in data.get("data", []):
            # row["s"] looks like "NYSE:ZETA"
            qualified = row["s"]
            ticker = qualified.split(":")[1]
            close_val, ytd_pct = row["d"]
            if close_val is not None and ytd_pct is not None:
                result[ticker] = {"close": float(close_val), "ytd_pct": float(ytd_pct)}
                logger.info(f"{ticker}: TradingView  close={close_val:.2f}  YTD={ytd_pct:.2f}%")

        logger.info(f"TradingView: fetched {len(result)}/{len(tv_tickers)} tickers")
        return result

    except Exception as e:
        logger.error(f"TradingView batch fetch failed: {e}")
        return {}


def _fetch_ticker_stooq(ticker: str, start_date: str, end_date: str) -> list[dict] | None:
    """Fetch daily candles from Stooq (no API key, used for chart time-series)."""
    symbol = f"{ticker}.US".lower()
    d1 = start_date.replace("-", "")
    d2 = end_date.replace("-", "")
    url = f"{STOOQ_BASE}?s={symbol}&d1={d1}&d2={d2}&i=d"

    try:
        resp = http_requests.get(url, timeout=20)
        if resp.status_code != 200 or not resp.text:
            logger.warning(f"{ticker}: Stooq returned no data")
            return None

        if "No data" in resp.text:
            logger.warning(f"{ticker}: Stooq returned 'No data'")
            return None

        reader = csv.DictReader(resp.text.splitlines())
        result = []
        for row in reader:
            date_str = row.get("Date")
            close_str = row.get("Close")
            if not date_str or not close_str or close_str in ("", "0", "0.0"):
                continue
            try:
                result.append({"date": date_str, "close": float(close_str)})
            except ValueError:
                continue

        if len(result) < 2:
            logger.warning(f"{ticker}: Stooq insufficient data ({len(result)} points)")
            return None

        logger.info(f"{ticker}: Stooq got {len(result)} daily points")
        return result

    except Exception as e:
        logger.error(f"{ticker}: Stooq fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Build full dataset
# ---------------------------------------------------------------------------

def _build_data() -> dict:
    """Fetch YTD data for all stocks and benchmarks, compute metrics.

    Strategy:
    - TradingView Scanner  →  accurate YTD % and current close (1 batch call)
    - Stooq                →  daily time-series for line charts
    - Chart series are scaled so the final point matches TradingView's YTD
    """
    year = datetime.date.today().year
    start_date = f"{year}-01-01"
    end_date = datetime.date.today().isoformat()

    all_tickers = list(USERS.values()) + BENCHMARKS
    seen = set()
    unique_tickers = []
    for t in all_tickers:
        if t not in seen:
            seen.add(t)
            unique_tickers.append(t)

    # --- 1) TradingView: get accurate YTD numbers in ONE call ----------------
    tv_data = _fetch_tradingview_batch(unique_tickers)

    # --- 2) Stooq: get daily time-series for each ticker ---------------------
    histories: dict[str, list[dict]] = {}
    ytd_returns: dict[str, float] = {}
    provider_parts: list[str] = []

    if tv_data:
        provider_parts.append("TradingView")

    for i, ticker in enumerate(unique_tickers):
        logger.info(f"Fetching chart data for {ticker} ({i+1}/{len(unique_tickers)})...")

        tv = tv_data.get(ticker)
        stooq_raw = _fetch_ticker_stooq(ticker, start_date, end_date)

        # Determine YTD return
        if tv:
            ytd_returns[ticker] = round(tv["ytd_pct"], 2)
        elif stooq_raw and len(stooq_raw) >= 2:
            first_c = stooq_raw[0]["close"]
            last_c = stooq_raw[-1]["close"]
            ytd_returns[ticker] = round(((last_c - first_c) / first_c) * 100, 2)
            if "Stooq" not in provider_parts:
                provider_parts.append("Stooq")
        else:
            ytd_returns[ticker] = 0.0

        # Build time-series for charts
        if stooq_raw and len(stooq_raw) >= 2:
            first_close = stooq_raw[0]["close"]
            stooq_final_return = ((stooq_raw[-1]["close"] - first_close) / first_close) * 100

            # Scale factor: normalize chart endpoint to TradingView's accurate YTD
            tv_ytd = tv["ytd_pct"] if tv else stooq_final_return
            if abs(stooq_final_return) > 0.01:
                scale = tv_ytd / stooq_final_return
            else:
                scale = 1.0

            series = []
            for pt in stooq_raw:
                raw_return = ((pt["close"] - first_close) / first_close) * 100
                series.append({"date": pt["date"], "value": round(raw_return * scale, 2)})
            histories[ticker] = series
        else:
            histories[ticker] = []

        # Small delay between Stooq requests
        time.sleep(0.3)

    logger.info(f"YTD returns: {ytd_returns}")

    # -- Per-user data -------------------------------------------------------
    users = []
    for name, ticker in USERS.items():
        ret = ytd_returns.get(ticker, 0.0)
        balance = round(STARTING_BALANCE * (1 + ret / 100), 2)
        users.append({
            "name": name,
            "ticker": ticker,
            "ytd_return": ret,
            "balance": balance,
            "crypto_adjacent": ticker in CRYPTO_ADJACENT,
        })

    users.sort(key=lambda u: u["ytd_return"], reverse=True)

    # -- Group averages ------------------------------------------------------
    all_returns = [u["ytd_return"] for u in users]
    filtered_returns = [u["ytd_return"] for u in users if not u["crypto_adjacent"]]

    group_avg = round(sum(all_returns) / len(all_returns), 2) if all_returns else 0
    filtered_avg = (
        round(sum(filtered_returns) / len(filtered_returns), 2)
        if filtered_returns
        else 0
    )

    benchmarks = []
    for ticker in BENCHMARKS:
        ret = ytd_returns.get(ticker, 0.0)
        balance = round(STARTING_BALANCE * (1 + ret / 100), 2)
        benchmarks.append({"ticker": ticker, "ytd_return": ret, "balance": balance})

    # -- Daily group-average time series ------------------------------------
    all_dates = set()
    for ticker in list(USERS.values()):
        for pt in histories.get(ticker, []):
            all_dates.add(pt["date"])
    sorted_dates = sorted(all_dates)

    group_avg_history = []
    filtered_avg_history = []
    for d in sorted_dates:
        vals_all = []
        vals_filtered = []
        for name, ticker in USERS.items():
            date_map = {pt["date"]: pt["value"] for pt in histories.get(ticker, [])}
            if d in date_map:
                vals_all.append(date_map[d])
                if ticker not in CRYPTO_ADJACENT:
                    vals_filtered.append(date_map[d])
        if vals_all:
            group_avg_history.append(
                {"date": d, "value": round(sum(vals_all) / len(vals_all), 2)}
            )
        if vals_filtered:
            filtered_avg_history.append(
                {"date": d, "value": round(sum(vals_filtered) / len(vals_filtered), 2)}
            )

    provider_label = " + ".join(provider_parts) if provider_parts else "Unknown"

    return {
        "users": users,
        "benchmarks": benchmarks,
        "group_avg": group_avg,
        "filtered_avg": filtered_avg,
        "group_avg_history": group_avg_history,
        "filtered_avg_history": filtered_avg_history,
        "histories": histories,
        "updated_at": datetime.datetime.now().strftime("%b %d, %Y %I:%M %p"),
        "data_provider": provider_label,
    }


# ---------------------------------------------------------------------------
# Disk cache helpers
# ---------------------------------------------------------------------------

def _save_cache_to_disk(data: dict) -> None:
    """Persist cache to disk for multi-worker consistency."""
    try:
        payload = {"_cache_ts": time.time(), "data": data}
        tmp_path = f"{CACHE_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp_path, CACHE_FILE)
        logger.info("Cache written to disk")
    except Exception as e:
        logger.error(f"Failed to save cache to disk: {e}")


def _load_cache_from_disk() -> tuple[dict | None, float]:
    """Load cache from disk if present."""
    try:
        if not os.path.exists(CACHE_FILE):
            return None, 0
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        data = payload.get("data")
        ts = float(payload.get("_cache_ts", 0))
        return data, ts
    except Exception as e:
        logger.error(f"Failed to load cache from disk: {e}")
        return None, 0


# ---------------------------------------------------------------------------
# Background refresh — fetches data without blocking web requests
# ---------------------------------------------------------------------------

def _refresh_cache():
    with _lock:
        if _cache["loading"]:
            return
        _cache["loading"] = True

    try:
        logger.info("Background refresh: starting...")
        data = _build_data()
        _cache["data"] = data
        _cache["ts"] = time.time()
        _save_cache_to_disk(data)
        logger.info("Background refresh: done!")
    except Exception as e:
        logger.error(f"Background refresh failed: {e}")
    finally:
        _cache["loading"] = False


def _schedule_refresh_loop():
    _refresh_cache()
    while True:
        time.sleep(CACHE_TTL)
        _refresh_cache()


def get_data() -> dict:
    # First try memory
    if _cache["data"] is not None:
        return _cache["data"]

    # Then try disk (useful if multiple workers are running)
    disk_data, disk_ts = _load_cache_from_disk()
    if disk_data is not None:
        _cache["data"] = disk_data
        _cache["ts"] = disk_ts
        return disk_data

    if not _cache["loading"]:
        threading.Thread(target=_refresh_cache, daemon=True).start()

    return {
        "users": [
            {
                "name": n, "ticker": t, "ytd_return": 0, "balance": 1000,
                "crypto_adjacent": t in CRYPTO_ADJACENT,
            }
            for n, t in USERS.items()
        ],
        "benchmarks": [{"ticker": t, "ytd_return": 0, "balance": 1000} for t in BENCHMARKS],
        "group_avg": 0, "filtered_avg": 0,
        "group_avg_history": [], "filtered_avg_history": [],
        "histories": {},
        "updated_at": "Loading data...",
        "_loading": True,
        "data_provider": "Loading",
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    return jsonify(get_data())


@app.route("/api/health")
def health():
    data = _cache["data"]
    if data is None:
        disk_data, _ = _load_cache_from_disk()
        if disk_data is not None:
            data = disk_data
            _cache["data"] = disk_data
            return jsonify({
                "status": "ok",
                "tickers_with_data": sum(1 for u in data["users"] if u["ytd_return"] != 0.0),
                "total_tickers": len(data["users"]),
                "updated_at": data["updated_at"],
                "loading": _cache["loading"],
                "data_provider": data.get("data_provider", "Unknown"),
            })
        return jsonify({"status": "loading", "loading": _cache["loading"]})
    has_data = any(u["ytd_return"] != 0.0 for u in data["users"])
    return jsonify({
        "status": "ok" if has_data else "no_data",
        "tickers_with_data": sum(1 for u in data["users"] if u["ytd_return"] != 0.0),
        "total_tickers": len(data["users"]),
        "updated_at": data["updated_at"],
        "loading": _cache["loading"],
        "data_provider": data.get("data_provider", "Unknown"),
    })


@app.route("/api/clear-cache")
def clear_cache():
    """Force a fresh data fetch (clears memory + disk cache)."""
    _cache["data"] = None
    _cache["ts"] = 0
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except Exception:
        pass
    if not _cache["loading"]:
        threading.Thread(target=_refresh_cache, daemon=True).start()
    return jsonify({"status": "cache_cleared", "refreshing": True})


# Start background refresh on import
_bg_thread = threading.Thread(target=_schedule_refresh_loop, daemon=True)
_bg_thread.start()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050, use_reloader=False)
