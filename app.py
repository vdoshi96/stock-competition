import datetime
import logging
import time
from flask import Flask, jsonify, render_template

import yfinance as yf

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
CACHE_TTL = 900  # 15 minutes in seconds

# ---------------------------------------------------------------------------
# Simple in-memory cache
# ---------------------------------------------------------------------------
_cache: dict = {"data": None, "ts": 0}


def _ytd_start() -> str:
    """Return the first trading day of the current year as 'YYYY-MM-DD'."""
    year = datetime.date.today().year
    return f"{year}-01-01"


def _download_with_retry(tickers: list[str], start: str, end: str, max_retries: int = 3):
    """Batch-download daily close prices for all tickers with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}: downloading {len(tickers)} tickers...")
            df = yf.download(
                tickers,
                start=start,
                end=end,
                auto_adjust=True,
                threads=False,       # sequential to avoid rate-limit
                progress=False,
            )
            if df is not None and not df.empty:
                logger.info(f"Download succeeded on attempt {attempt}, shape={df.shape}")
                return df
            logger.warning(f"Attempt {attempt}: empty dataframe returned")
        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {e}")

        if attempt < max_retries:
            wait = attempt * 5
            logger.info(f"Waiting {wait}s before retry...")
            time.sleep(wait)

    logger.error("All download attempts failed")
    return None


def _build_data() -> dict:
    """Fetch YTD data for all user stocks and benchmarks, compute metrics."""
    today = datetime.date.today()
    start = _ytd_start()
    end = (today + datetime.timedelta(days=1)).isoformat()

    all_tickers = list(USERS.values()) + BENCHMARKS
    # De-duplicate while preserving order
    seen = set()
    unique_tickers = []
    for t in all_tickers:
        if t not in seen:
            seen.add(t)
            unique_tickers.append(t)

    # Batch download all tickers at once -----------------------------------
    raw_df = _download_with_retry(unique_tickers, start, end)

    histories: dict[str, list[dict]] = {}
    ytd_returns: dict[str, float] = {}

    if raw_df is not None and not raw_df.empty:
        for ticker in unique_tickers:
            try:
                # yf.download returns MultiIndex columns: ('Close', 'TICKER')
                # for multi-ticker downloads, or single-level for single ticker
                if len(unique_tickers) == 1:
                    close_series = raw_df["Close"].dropna()
                else:
                    if ("Close", ticker) not in raw_df.columns:
                        # Try alternate: sometimes column is just ticker under Close
                        if "Close" in raw_df.columns and hasattr(raw_df["Close"], ticker):
                            close_series = raw_df["Close"][ticker].dropna()
                        else:
                            logger.warning(f"No Close data for {ticker}")
                            histories[ticker] = []
                            ytd_returns[ticker] = 0.0
                            continue
                    else:
                        close_series = raw_df[("Close", ticker)].dropna()

                if close_series.empty or len(close_series) < 2:
                    logger.warning(f"Insufficient data for {ticker}: {len(close_series)} rows")
                    histories[ticker] = []
                    ytd_returns[ticker] = 0.0
                    continue

                first_close = float(close_series.iloc[0])
                last_close = float(close_series.iloc[-1])
                ytd_pct = ((last_close - first_close) / first_close) * 100
                ytd_returns[ticker] = round(ytd_pct, 2)

                # Build time-series of cumulative % return for the chart
                series = []
                for dt_idx, val in close_series.items():
                    date_str = dt_idx.strftime("%Y-%m-%d")
                    cum_return = ((float(val) - first_close) / first_close) * 100
                    series.append({"date": date_str, "value": round(cum_return, 2)})
                histories[ticker] = series
                logger.info(f"{ticker}: YTD {ytd_returns[ticker]}%, {len(series)} data points")

            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                histories[ticker] = []
                ytd_returns[ticker] = 0.0
    else:
        logger.error("No data downloaded at all — all tickers will show 0%")
        for ticker in unique_tickers:
            histories[ticker] = []
            ytd_returns[ticker] = 0.0

    # Build per-user data ------------------------------------------------
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

    # Sort by YTD return descending (leaderboard)
    users.sort(key=lambda u: u["ytd_return"], reverse=True)

    # Group averages ------------------------------------------------------
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
        benchmarks.append({
            "ticker": ticker,
            "ytd_return": ret,
            "balance": balance,
        })

    # Build daily group-average time series --------------------------------
    all_dates = set()
    user_tickers = list(USERS.values())
    for ticker in user_tickers:
        for pt in histories.get(ticker, []):
            all_dates.add(pt["date"])
    sorted_dates = sorted(all_dates)

    group_avg_history = []
    filtered_avg_history = []
    for d in sorted_dates:
        vals_all = []
        vals_filtered = []
        for name, ticker in USERS.items():
            series = histories.get(ticker, [])
            date_map = {pt["date"]: pt["value"] for pt in series}
            if d in date_map:
                vals_all.append(date_map[d])
                if ticker not in CRYPTO_ADJACENT:
                    vals_filtered.append(date_map[d])
        if vals_all:
            group_avg_history.append({
                "date": d,
                "value": round(sum(vals_all) / len(vals_all), 2),
            })
        if vals_filtered:
            filtered_avg_history.append({
                "date": d,
                "value": round(sum(vals_filtered) / len(vals_filtered), 2),
            })

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


def get_data() -> dict:
    """Return cached data, refreshing if stale."""
    now = time.time()
    if _cache["data"] is None or (now - _cache["ts"]) > CACHE_TTL:
        _cache["data"] = _build_data()
        _cache["ts"] = now
    return _cache["data"]


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
    """Health check endpoint — also useful for debugging on Render."""
    data = get_data()
    has_data = any(u["ytd_return"] != 0.0 for u in data["users"])
    return jsonify({
        "status": "ok" if has_data else "no_data",
        "tickers_with_data": sum(1 for u in data["users"] if u["ytd_return"] != 0.0),
        "total_tickers": len(data["users"]),
        "updated_at": data["updated_at"],
    })


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050)
