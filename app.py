import datetime
import logging
import os
import threading
import time

import csv
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

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
FINNHUB_BASE = "https://finnhub.io/api/v1"
STOOQ_BASE = "https://stooq.com/q/d/l/"

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
_cache: dict = {"data": None, "ts": 0, "loading": False}
_lock = threading.Lock()


def _ytd_start_ts() -> int:
    """Unix timestamp for Jan 1 of the current year."""
    year = datetime.date.today().year
    return int(datetime.datetime(year, 1, 1).timestamp())


def _now_ts() -> int:
    """Current unix timestamp."""
    return int(time.time())


def _fetch_ticker_finnhub(ticker: str, from_ts: int, to_ts: int) -> list[dict] | None:
    """Fetch daily candles for a ticker from FinnHub (60 calls/min free)."""
    url = f"{FINNHUB_BASE}/stock/candle"
    params = {
        "symbol": ticker,
        "resolution": "D",
        "from": from_ts,
        "to": to_ts,
        "token": FINNHUB_API_KEY,
    }

    try:
        resp = http_requests.get(url, params=params, timeout=15)
        data = resp.json()

        if data.get("error"):
            logger.warning(f"{ticker}: FinnHub error: {data.get('error')}")
            return None

        if data.get("s") != "ok":
            logger.warning(f"{ticker}: no data (status={data.get('s')})")
            return None

        closes = data.get("c", [])
        timestamps = data.get("t", [])

        if not closes or not timestamps or len(closes) < 2:
            logger.warning(f"{ticker}: insufficient data ({len(closes)} points)")
            return None

        result = []
        for ts, close in zip(timestamps, closes):
            date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            result.append({"date": date_str, "close": float(close)})

        logger.info(f"{ticker}: got {len(result)} data points")
        return result

    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None


def _fetch_ticker_stooq(ticker: str, start_date: str, end_date: str) -> list[dict] | None:
    """Fetch daily candles for a ticker from Stooq (no API key required)."""
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


def _build_data() -> dict:
    """Fetch YTD data for all stocks and benchmarks, compute metrics."""
    from_ts = _ytd_start_ts()
    to_ts = _now_ts()
    start_date = datetime.date.fromtimestamp(from_ts).isoformat()
    end_date = datetime.date.fromtimestamp(to_ts).isoformat()

    all_tickers = list(USERS.values()) + BENCHMARKS
    seen = set()
    unique_tickers = []
    for t in all_tickers:
        if t not in seen:
            seen.add(t)
            unique_tickers.append(t)

    # Fetch all tickers — FinnHub allows 60/min so no pausing needed
    histories: dict[str, list[dict]] = {}
    ytd_returns: dict[str, float] = {}
    provider_used: dict[str, str] = {}

    for i, ticker in enumerate(unique_tickers):
        logger.info(f"Fetching {ticker} ({i+1}/{len(unique_tickers)})...")

        raw = None
        if FINNHUB_API_KEY:
            raw = _fetch_ticker_finnhub(ticker, from_ts, to_ts)
            if raw:
                provider_used[ticker] = "FinnHub"

        if not raw:
            raw = _fetch_ticker_stooq(ticker, start_date, end_date)
            if raw:
                provider_used[ticker] = "Stooq"

        if not raw or len(raw) < 2:
            histories[ticker] = []
            ytd_returns[ticker] = 0.0
            time.sleep(0.3)
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

        # Small courtesy delay (60/min limit is generous, this is just polite)
        time.sleep(0.3)

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

    provider_label = "Unknown"
    if provider_used:
        providers = sorted(set(provider_used.values()))
        provider_label = providers[0] if len(providers) == 1 else "Mixed (FinnHub + Stooq)"

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
    if _cache["data"] is not None:
        return _cache["data"]

    if not _cache["loading"]:
        threading.Thread(target=_refresh_cache, daemon=True).start()

    return {
        "users": [{"name": n, "ticker": t, "ytd_return": 0, "balance": 1000, "crypto_adjacent": t in CRYPTO_ADJACENT} for n, t in USERS.items()],
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
        return jsonify({"status": "loading", "loading": _cache["loading"], "api_key_set": bool(FINNHUB_API_KEY)})
    has_data = any(u["ytd_return"] != 0.0 for u in data["users"])
    return jsonify({
        "status": "ok" if has_data else "no_data",
        "api_key_set": bool(FINNHUB_API_KEY),
        "tickers_with_data": sum(1 for u in data["users"] if u["ytd_return"] != 0.0),
        "total_tickers": len(data["users"]),
        "updated_at": data["updated_at"],
        "loading": _cache["loading"],
    })


# Start background refresh on import
_bg_thread = threading.Thread(target=_schedule_refresh_loop, daemon=True)
_bg_thread.start()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050, use_reloader=False)
