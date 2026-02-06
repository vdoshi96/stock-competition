import datetime
import logging
import os
import threading
import time

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

RATE_LIMIT_PER_MIN = 8  # Twelve Data free tier: 8 requests per minute

# ---------------------------------------------------------------------------
# In-memory cache + background refresh state
# ---------------------------------------------------------------------------
_cache: dict = {"data": None, "ts": 0, "loading": False}
_lock = threading.Lock()


def _ytd_start() -> str:
    year = datetime.date.today().year
    return f"{year}-01-01"


def _fetch_ticker(ticker: str, start: str, end: str) -> list[dict] | None:
    """Fetch daily close prices for a single ticker from Twelve Data."""
    url = f"{TWELVEDATA_BASE}/time_series"
    params = {
        "symbol": ticker,
        "interval": "1day",
        "start_date": start,
        "end_date": end,
        "apikey": TWELVEDATA_API_KEY,
        "order": "ASC",
        "outputsize": 5000,
    }

    for attempt in range(1, 3):
        try:
            resp = http_requests.get(url, params=params, timeout=20)
            data = resp.json()

            if data.get("code") == 429:
                logger.warning(f"{ticker}: rate limited, waiting 62s (attempt {attempt})")
                time.sleep(62)
                continue

            if data.get("status") == "error":
                logger.error(f"Twelve Data error for {ticker}: {data.get('message')}")
                return None

            values = data.get("values")
            if not values:
                logger.warning(f"No values returned for {ticker}")
                return None

            result = []
            for v in values:
                try:
                    result.append({
                        "date": v["datetime"],
                        "close": float(v["close"]),
                    })
                except (KeyError, ValueError):
                    continue

            logger.info(f"{ticker}: got {len(result)} data points")
            return result

        except Exception as e:
            logger.error(f"Failed to fetch {ticker} (attempt {attempt}): {e}")
            if attempt < 2:
                time.sleep(10)

    return None


def _fetch_all_tickers(tickers: list[str], start: str, end: str) -> dict[str, list[dict]]:
    """Fetch all tickers while respecting the 8 req/min rate limit."""
    results: dict[str, list[dict]] = {}
    request_count = 0

    for i, ticker in enumerate(tickers):
        if request_count > 0 and request_count % RATE_LIMIT_PER_MIN == 0:
            logger.info(f"Rate limit pause: waiting 62s after {request_count} requests...")
            time.sleep(62)

        logger.info(f"Fetching {ticker} ({i+1}/{len(tickers)})...")
        raw = _fetch_ticker(ticker, start, end)
        results[ticker] = raw if raw else []
        request_count += 1

        if i < len(tickers) - 1:
            time.sleep(1)

    return results


def _build_data() -> dict:
    """Fetch YTD data for all user stocks and benchmarks, compute metrics."""
    today = datetime.date.today()
    start = _ytd_start()
    end = today.isoformat()

    all_tickers = list(USERS.values()) + BENCHMARKS
    seen = set()
    unique_tickers = []
    for t in all_tickers:
        if t not in seen:
            seen.add(t)
            unique_tickers.append(t)

    raw_data = _fetch_all_tickers(unique_tickers, start, end)

    histories: dict[str, list[dict]] = {}
    ytd_returns: dict[str, float] = {}

    for ticker in unique_tickers:
        raw = raw_data.get(ticker, [])

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

    # Per-user data
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

    # Group averages
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

    # Daily group-average time series
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
            group_avg_history.append({"date": d, "value": round(sum(vals_all) / len(vals_all), 2)})
        if vals_filtered:
            filtered_avg_history.append({"date": d, "value": round(sum(vals_filtered) / len(vals_filtered), 2)})

    return {
        "users": users,
        "benchmarks": benchmarks,
        "group_avg": group_avg,
        "filtered_avg": filtered_avg,
        "group_avg_history": group_avg_history,
        "filtered_avg_history": filtered_avg_history,
        "histories": histories,
        "updated_at": datetime.datetime.now().strftime("%b %d, %Y %I:%M %p"),
    }


# ---------------------------------------------------------------------------
# Background data refresh — runs in a separate thread so web requests
# never block on the slow, rate-limited API calls.
# ---------------------------------------------------------------------------

def _refresh_cache():
    """Fetch fresh data and update the cache. Called from a background thread."""
    with _lock:
        if _cache["loading"]:
            logger.info("Refresh already in progress, skipping")
            return
        _cache["loading"] = True

    try:
        logger.info("Background refresh: starting data fetch...")
        data = _build_data()
        _cache["data"] = data
        _cache["ts"] = time.time()
        logger.info("Background refresh: complete!")
    except Exception as e:
        logger.error(f"Background refresh failed: {e}")
    finally:
        _cache["loading"] = False


def _schedule_refresh_loop():
    """Continuously refresh data in the background every CACHE_TTL seconds."""
    # Initial fetch on startup
    _refresh_cache()

    while True:
        time.sleep(CACHE_TTL)
        _refresh_cache()


def get_data() -> dict:
    """Return cached data instantly. If no data yet, return a loading placeholder."""
    if _cache["data"] is not None:
        return _cache["data"]

    # No data yet — trigger a refresh if not already running
    if not _cache["loading"]:
        threading.Thread(target=_refresh_cache, daemon=True).start()

    # Return a loading placeholder
    return {
        "users": [{"name": n, "ticker": t, "ytd_return": 0, "balance": 1000, "crypto_adjacent": t in CRYPTO_ADJACENT} for n, t in USERS.items()],
        "benchmarks": [{"ticker": t, "ytd_return": 0, "balance": 1000} for t in BENCHMARKS],
        "group_avg": 0,
        "filtered_avg": 0,
        "group_avg_history": [],
        "filtered_avg_history": [],
        "histories": {},
        "updated_at": "Loading data...",
        "_loading": True,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    data = get_data()
    return jsonify(data)


@app.route("/api/health")
def health():
    data = _cache["data"]
    if data is None:
        return jsonify({"status": "loading", "loading": _cache["loading"], "api_key_set": bool(TWELVEDATA_API_KEY)})
    has_data = any(u["ytd_return"] != 0.0 for u in data["users"])
    return jsonify({
        "status": "ok" if has_data else "no_data",
        "api_key_set": bool(TWELVEDATA_API_KEY),
        "tickers_with_data": sum(1 for u in data["users"] if u["ytd_return"] != 0.0),
        "total_tickers": len(data["users"]),
        "updated_at": data["updated_at"],
        "loading": _cache["loading"],
    })


# ---------------------------------------------------------------------------
# Start background refresh thread on import (works with gunicorn)
# ---------------------------------------------------------------------------
_bg_thread = threading.Thread(target=_schedule_refresh_loop, daemon=True)
_bg_thread.start()

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050, use_reloader=False)
