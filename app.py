import os
import time
import logging
from typing import Any, Dict, Optional

import requests
from flask import Flask, request, jsonify

# ------------------------------------------------------------------------------
# Flask app & logging
# ------------------------------------------------------------------------------

app = Flask(__name__)

logger = app.logger
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------

def env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip() not in ("0", "false", "False", "")


def env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


DRY_RUN = env_bool("DRY_RUN", True)  # 1 = simulate only, 0 = real orders
MAX_POSITION_USD = env_float("MAX_POSITION_USD", 1000.0)
POSITION_DOLLARS = env_float("POSITION_DOLLARS", 100.0)
RISK_PER_TRADE = env_float("RISK_PER_TRADE", 50.0)

QUESTRADE_ACCOUNT_NUMBER = os.environ.get("QUESTRADE_ACCOUNT_NUMBER", "").strip()
QUESTRADE_REFRESH_TOKEN = os.environ.get("QUESTRADE_REFRESH_TOKEN", "").strip()

# practice flag is here mostly for clarity; Questrade decides account from token
QUESTRADE_PRACTICE = env_bool("QUESTRADE_PRACTICE", True)

if not QUESRADE_ACCOUNT_NUMBER := QUESRADE_ACCOUNT_NUMBER:
    logger.warning("QUESTRADE_ACCOUNT_NUMBER not set – order placement will fail")

if not QUESTRADE_REFRESH_TOKEN:
    logger.warning("QUESTRADE_REFRESH_TOKEN not set – Questrade API will fail")

# ------------------------------------------------------------------------------
# Questrade OAuth + API helpers
# ------------------------------------------------------------------------------

QT_ACCESS_TOKEN: Optional[str] = None
QT_API_SERVER: Optional[str] = None
QT_TOKEN_EXPIRES_AT: float = 0.0  # epoch seconds


def refresh_qt_token() -> None:
    """
    Use the long-lived refresh token to obtain a short-lived access token
    and the base API server URL.
    """
    global QT_ACCESS_TOKEN, QT_API_SERVER, QT_TOKEN_EXPIRES_AT

    if not QUESTRADE_REFRESH_TOKEN:
        raise RuntimeError("Missing QUESRADE_REFRESH_TOKEN")

    url = (
        "https://login.questrade.com/oauth2/token"
        f"?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    )

    logger.info("Refreshing Questrade token…")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    QT_ACCESS_TOKEN = data["access_token"]
    QT_API_SERVER = data["api_server"].rstrip("/")
    expires_in = data.get("expires_in", 1800)
    QT_TOKEN_EXPIRES_AT = time.time() + float(expires_in) * 0.9  # renew a bit early

    logger.info(
        "Questrade token refreshed. api_server=%s, expires_in=%s",
        QT_API_SERVER,
        expires_in,
    )


def ensure_qt_token() -> None:
    if QT_ACCESS_TOKEN is None or QT_API_SERVER is None or time.time() >= QT_TOKEN_EXPIRES_AT:
        refresh_qt_token()


def qt_request(method: str, path: str, **kwargs) -> Dict[str, Any]:
    """
    Helper to call Questrade API with auto token refresh.
    path: "/v1/..." (no server)
    """
    ensure_qt_token()
    assert QT_API_SERVER is not None
    assert QT_ACCESS_TOKEN is not None

    url = QT_API_SERVER + path
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {QT_ACCESS_TOKEN}"
    headers.setdefault("Content-Type", "application/json")

    logger.info("Questrade %s %s", method, path)
    resp = requests.request(method, url, headers=headers, timeout=10, **kwargs)

    # If token expired unexpectedly, refresh once and retry
    if resp.status_code == 401:
        logger.warning("401 from Questrade, refreshing token and retrying…")
        refresh_qt_token()
        headers["Authorization"] = f"Bearer {QT_ACCESS_TOKEN}"
        resp = requests.request(method, url, headers=headers, timeout=10, **kwargs)

    resp.raise_for_status()
    if resp.text:
        try:
            return resp.json()
        except ValueError:
            logger.warning("Non-JSON response from Questrade: %s", resp.text)
            return {}
    return {}


# ------------------------------------------------------------------------------
# Questrade trading helpers
# ------------------------------------------------------------------------------

_symbol_id_cache: Dict[str, int] = {}


def get_symbol_id(symbol: str) -> int:
    """
    Look up the Questrade symbolId for a ticker, with caching.
    """
    sym = symbol.upper()
    if sym in _symbol_id_cache:
        return _symbol_id_cache[sym]

    data = qt_request("GET", f"/v1/symbols?names={sym}")
    syms = data.get("symbols") or []
    if not syms:
        raise RuntimeError(f"Symbol not found in Questrade: {sym}")

    symbol_id = int(syms[0]["symbolId"])
    _symbol_id_cache[sym] = symbol_id
    return symbol_id


def get_last_price(symbol_id: int) -> float:
    data = qt_request("GET", f"/v1/markets/quotes/{symbol_id}")
    quotes = data.get("quotes") or []
    if not quotes:
        raise RuntimeError(f"No quote for symbolId={symbol_id}")
    q = quotes[0]
    last = q.get("lastTradePrice") or q.get("lastTradePriceTrHrs") or q.get("bidPrice")
    return float(last)


def get_total_exposure_usd() -> float:
    """
    Approximate current exposure in USD based on positions.
    Only used to enforce MAX_POSITION_USD.
    """
    if not QUESTRADE_ACCOUNT_NUMBER:
        return 0.0

    data = qt_request("GET", f"/v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/positions")
    positions = data.get("positions") or []
    total = 0.0
    for p in positions:
        try:
            total += float(p.get("currentMarketValue", 0.0))
        except (TypeError, ValueError):
            continue
    return total


def place_qt_order(symbol: str, side: str, event: str) -> Dict[str, Any]:
    """
    Place a MARKET order on Questrade based on TradingView event.
    - event: 'ENTRY' / 'BUY' / 'OPEN' / 'LONG'  -> Buy
    - event: 'EXIT'  / 'SELL' / 'CLOSE'        -> Sell
    - side:  'long' (only long supported for now)
    """
    if not QUESTRADE_ACCOUNT_NUMBER:
        msg = "QUESTRADE_ACCOUNT_NUMBER not set"
        logger.error(msg)
        return {"ok": False, "error": msg}

    # TradingView sometimes sends "NASDAQ:AMZN" → strip prefix
    if ":" in symbol:
        symbol = symbol.split(":", 1)[1]

    symbol = symbol.upper()
    event_clean = (event or "").strip().upper()
    side_clean = (side or "").strip().lower()

    # Map multiple spellings to entry/exit
    is_entry = event_clean in ("ENTRY", "BUY", "OPEN", "LONG")
    is_exit = event_clean in ("EXIT", "SELL", "CLOSE")

    if is_entry and side_clean == "long":
        action = "Buy"
    elif is_exit and side_clean == "long":
        action = "Sell"
    else:
        msg = f"Unsupported event/side combination: event={event_clean}, side={side_clean}"
        logger.warning(msg)
        return {"ok": False, "error": msg}

    # Risk / exposure check
    try:
        current_exposure = get_total_exposure_usd()
    except Exception as e:
        logger.warning("Could not fetch exposure, continuing anyway: %s", e)
        current_exposure = 0.0

    if is_entry and current_exposure + POSITION_DOLLARS > MAX_POSITION_USD:
        msg = (
            f"Exposure limit reached: current={current_exposure:.2f}, "
            f"would_add={POSITION_DOLLARS:.2f}, max={MAX_POSITION_USD:.2f}"
        )
        logger.warning(msg)
        return {"ok": False, "error": msg, "exposure": current_exposure}

    symbol_id = get_symbol_id(symbol)
    last_price = get_last_price(symbol_id)

    qty = max(1, int(POSITION_DOLLARS // last_price))
    logger.info(
        "Computed quantity: symbol=%s symbolId=%s last=%.4f pos$=%.2f qty=%s",
        symbol,
        symbol_id,
        last_price,
        POSITION_DOLLARS,
        qty,
    )

    order = {
        "symbolId": symbol_id,
        "quantity": qty,
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderType": "Market",
        "timeInForce": "Day",
        "action": action,
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
    }

    if DRY_RUN:
        logger.info("[DRY RUN] Would send order: %s", order)
        return {"ok": True, "dry_run": True, "order": order}

    payload = {"accountNumber": QUESTRADE_ACCOUNT_NUMBER, "orders": [order]}
    res = qt_request(
        "POST",
        f"/v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders",
        json=payload,
    )
    logger.info("Order response: %s", res)
    return {"ok": True, "response": res, "order": order}


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health() -> tuple[str, int]:
    logger.info("HEALTH CHECK HIT")
    return "OK", 200


@app.route("/tv", methods=["GET", "POST"])
def tv() -> tuple[Any, int]:
    # Log EVERYTHING that hits this route
    raw_body = request.get_data(as_text=True)
    logger.info(
        "TV HIT: method=%s path=%s body='%s'",
        request.method,
        request.path,
        raw_body,
    )

    # If someone opens it in a browser (GET), just tell them to use POST
    if request.method != "POST":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Use POST with JSON body",
                }
            ),
            405,
        )

    # Try to parse JSON
    data = request.get_json(silent=True) or {}
    logger.info("TV PARSED JSON: %s", data)

    symbol = str(data.get("symbol", "")).strip().upper()
    event = str(data.get("event", "")).strip()
    side = str(data.get("side", "")).strip().lower()
    risk_stop_pct = data.get("risk_stop_pct")

    if not symbol or not event:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Missing symbol or event",
                    "received": data,
                }
            ),
            400,
        )

    # Call trading logic
    try:
        result = place_qt_order(symbol=symbol, side=side, event=event)
        return jsonify({"ok": True, "result": result, "risk_stop_pct": risk_stop_pct}), 200
    except requests.HTTPError as e:
        logger.exception("HTTP error from Questrade")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Questrade HTTP error",
                    "status_code": e.response.status_code if e.response else None,
                    "response_text": e.response.text if e.response else None,
                }
            ),
            500,
        )
    except Exception as e:
        logger.exception("Unhandled error placing Questrade order")
        return jsonify({"ok": False, "error": str(e)}), 500


# ------------------------------------------------------------------------------
# Entry point for local dev
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
