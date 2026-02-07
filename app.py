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

STARTING_BALANCE = 1000.0
CACHE_TTL = 3600  # 1 hour

TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
TWELVEDATA_BASE = "https://api.twelvedata.com"
TWELVEDATA_RATE_LIMIT = 8          # requests per minute on free tier
TWELVEDATA_RATE_WINDOW = 62        # seconds to wait after hitting the limit

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

def _fetch_ticker_twelvedata(ticker: str, start_date: str, end_date: str) -> list[dict] | None:
    """Fetch daily candles from Twelve Data (8 calls/min free tier)."""
    url = f"{TWELVEDATA_BASE}/time_series"
    params = {
        "symbol": ticker,
        "interval": "1day",
        "start_date": start_date,
        "end_date": end_date,
        "apikey": TWELVEDATA_API_KEY,
        "order": "ASC",               # oldest first
    }

    try:
        resp = http_requests.get(url, params=params, timeout=20)
        data = resp.json()

        if data.get("status") != "ok":
            msg = data.get("message", "unknown error")
            logger.warning(f"{ticker}: Twelve Data error — {msg}")
            return None

        values = data.get("values", [])
        if len(values) < 2:
            logger.warning(f"{ticker}: Twelve Data insufficient data ({len(values)} points)")
            return None

        result = []
        for v in values:
            result.append({"date": v["datetime"], "close": float(v["close"])})

        logger.info(f"{ticker}: Twelve Data got {len(result)} points")
        return result

    except Exception as e:
        logger.error(f"{ticker}: Twelve Data fetch failed: {e}")
        return None


def _fetch_ticker_stooq(ticker: str, start_date: str, end_date: str) -> list[dict] | None:
    """Fetch daily candles from Stooq (no API key required)."""
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

        logger.info(f"{ticker}: Stooq got {len(result)} data points")
        return result

    except Exception as e:
        logger.error(f"{ticker}: Stooq fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Build full dataset
# ---------------------------------------------------------------------------

def _build_data() -> dict:
    """Fetch YTD data for all stocks and benchmarks, compute metrics."""
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

    histories: dict[str, list[dict]] = {}
    ytd_returns: dict[str, float] = {}
    provider_used: dict[str, str] = {}

    twelvedata_calls = 0

    for i, ticker in enumerate(unique_tickers):
        logger.info(f"Fetching {ticker} ({i+1}/{len(unique_tickers)})...")

        raw = None

        # --- Try Twelve Data first (if key is set) --------------------------
        if TWELVEDATA_API_KEY:
            # Respect 8 calls/minute rate limit
            if twelvedata_calls > 0 and twelvedata_calls % TWELVEDATA_RATE_LIMIT == 0:
                logger.info(
                    f"Rate limit reached ({twelvedata_calls} calls). "
                    f"Pausing {TWELVEDATA_RATE_WINDOW}s..."
                )
                time.sleep(TWELVEDATA_RATE_WINDOW)

            raw = _fetch_ticker_twelvedata(ticker, start_date, end_date)
            twelvedata_calls += 1

            if raw:
                provider_used[ticker] = "Twelve Data"

        # --- Fallback to Stooq -----------------------------------------------
        if not raw:
            logger.info(f"{ticker}: falling back to Stooq...")
            raw = _fetch_ticker_stooq(ticker, start_date, end_date)
            if raw:
                provider_used[ticker] = "Stooq"

        # --- Process result ---------------------------------------------------
        if not raw or len(raw) < 2:
            histories[ticker] = []
            ytd_returns[ticker] = 0.0
            continue

        first_close = raw[0]["close"]
        last_close = raw[-1]["close"]
        ytd_pct = ((last_close - first_close) / first_close) * 100
        ytd_returns[ticker] = round(ytd_pct, 2)

        series = []
        for pt in raw:
            cum_return = ((pt["close"] - first_close) / first_close) * 100
            series.append({"date": pt["date"], "value": round(cum_return, 2)})
        histories[ticker] = series
        logger.info(f"{ticker}: YTD {ytd_returns[ticker]}%")

        # Short courtesy delay between requests
        time.sleep(0.3)

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

    # Provider label
    provider_label = "Unknown"
    if provider_used:
        providers = sorted(set(provider_used.values()))
        provider_label = " + ".join(providers)

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
                "api_key_set": bool(TWELVEDATA_API_KEY),
                "tickers_with_data": sum(1 for u in data["users"] if u["ytd_return"] != 0.0),
                "total_tickers": len(data["users"]),
                "updated_at": data["updated_at"],
                "loading": _cache["loading"],
                "data_provider": data.get("data_provider", "Unknown"),
            })
        return jsonify({
            "status": "loading",
            "loading": _cache["loading"],
            "api_key_set": bool(TWELVEDATA_API_KEY),
        })
    has_data = any(u["ytd_return"] != 0.0 for u in data["users"])
    return jsonify({
        "status": "ok" if has_data else "no_data",
        "api_key_set": bool(TWELVEDATA_API_KEY),
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
