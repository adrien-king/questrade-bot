import os
import logging
import requests
from flask import Flask, request, jsonify

# ------------------------------------------------------------------------------
# Flask + logging setup
# ------------------------------------------------------------------------------

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = app.logger

# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------

QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")  # "1" = practice, "0" = live

POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))  # not used for size yet, but logged
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")  # optional, just in case

# Basic validation
if not QUESTRADE_REFRESH_TOKEN:
    raise ValueError("Missing Questrade refresh token (QUESTRADE_REFRESH_TOKEN).")

if not QUESTRADE_ACCOUNT_NUMBER:
    raise ValueError("Missing Questrade account number (QUESTRADE_ACCOUNT_NUMBER).")

log.info(
    "Config loaded: PRACTICE=%s, DRY_RUN=%s, POSITION_DOLLARS=%.2f, RISK_PER_TRADE=%.2f",
    QUESTRADE_PRACTICE,
    DRY_RUN,
    POSITION_DOLLARS,
    RISK_PER_TRADE,
)

# ------------------------------------------------------------------------------
# Helpers: base URL / token refresh / symbol lookup / last price / place order
# ------------------------------------------------------------------------------


def _login_base_url() -> str:
    """
    Questrade OAuth login host is fixed.
    """
    return "https://login.questrade.com"


def qt_refresh_access_token():
    """
    Use the long-lived refresh token (from Questrade web UI) to get:
      - short-lived access_token
      - api_server base URL
    """
    token_prefix = (QUESTRADE_REFRESH_TOKEN or "")[:5]
    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"

    log.info(
        "qt_refresh_access_token: refreshing token (practice=%s, token_prefix=%s...)",
        QUESTRADE_PRACTICE,
        token_prefix,
    )

    r = requests.get(url)
    log.info(
        "qt_refresh_access_token: refresh token response status=%s body=%s",
        r.status_code,
        r.text[:400],
    )

    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: {r.text}")

    data = r.json()
    access_token = data["access_token"]
    api_server = data["api_server"]  # e.g. "https://api01.iq.questrade.com/"

    log.info("qt_refresh_access_token: got api_server=%s", api_server)
    return access_token, api_server


def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    """
    Resolve a ticker like 'PLRZ' to a Questrade symbolId.
    """
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    log.info("qt_get_symbol_id: GET %s", url)

    r = requests.get(url, headers=qt_headers(access_token))
    log.info(
        "qt_get_symbol_id: status=%s body=%s",
        r.status_code,
        r.text[:400],
    )

    if r.status_code != 200:
        raise Exception(f"Failed to lookup symbolId for {symbol}: {r.text}")

    data = r.json()
    symbols = data.get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol}")

    # Prefer exact match if present
    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = (exact[0] if exact else symbols[0])
    symbol_id = chosen["symbolId"]

    log.info(
        "qt_get_symbol_id: symbol=%s symbolId=%s (chosen=%s)",
        symbol,
        symbol_id,
        chosen.get("symbol"),
    )
    return symbol_id


def get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    """
    Fetch last trade price for the given symbol.
    """
    url = f"{api_server}v1/markets/quotes/{symbol}"
    log.info("get_last_price: GET %s", url)

    r = requests.get(url, headers=qt_headers(access_token))
    log.info(
        "get_last_price: status=%s body=%s",
        r.status_code,
        r.text[:400],
    )

    if r.status_code != 200:
        raise Exception(f"Failed to get last price for {symbol}: {r.text}")

    data = r.json()
    price = float(data["quotes"][0]["lastTradePrice"])
    log.info("get_last_price: symbol=%s last_price=%.4f", symbol, price)
    return price


def qt_place_order(symbol: str, side: str, risk_stop_pct: float):
    """
    Place a market order via Questrade.

    side:
        "long" -> buy
        "sell" -> sell

    Returns: Questrade JSON response.
    """
    # Refresh token + get api_server
    access_token, api_server = qt_refresh_access_token()

    # Lookup symbolId
    symbol_id = qt_get_symbol_id(access_token, api_server, symbol)

    # Get last price (for position sizing / logging)
    last_price = get_last_price(access_token, api_server, symbol)

    # Position sizing: fixed notional
    shares = max(1, int(POSITION_DOLLARS / last_price))
    log.info(
        "qt_place_order: symbol=%s side=%s last_price=%.4f shares=%s risk_stop_pct=%s",
        symbol,
        side,
        last_price,
        shares,
        risk_stop_pct,
    )

    # Optional: simple max position cap
    if MAX_POSITION_USD > 0 and POSITION_DOLLARS > MAX_POSITION_USD:
        raise Exception(
            f"Requested position {POSITION_DOLLARS} exceeds MAX_POSITION_USD={MAX_POSITION_USD}"
        )

    leg_side = "Buy" if side == "long" else "Sell"

    order_body = {
        "accountNumber": QUESTRADE_ACCOUNT_NUMBER,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderLegs": [
            {
                "symbolId": symbol_id,
                "legSide": leg_side,
                "quantity": shares,
            }
        ],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    log.info("qt_place_order: POST %s body=%s", url, order_body)

    r = requests.post(url, headers=qt_headers(access_token), json=order_body)
    log.info(
        "qt_place_order: response status=%s body=%s",
        r.status_code,
        r.text[:500],
    )

    if r.status_code >= 300:
        raise Exception(f"Order rejected: {r.status_code} {r.text}")

    return r.json()


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------


@app.route("/health", methods=["GET"])
def health():
    """
    Simple health check endpoint.
    """
    log.info("HEALTH CHECK HIT")
    return "OK", 200


@app.route("/tv", methods=["GET", "POST"])
def tv():
    """
    TradingView webhook endpoint.

    Expects JSON like:
    {
        "symbol": "PLRZ",
        "event": "BUY" | "SELL" | "ENTRY" | "EXIT",
        "side": "long",
        "risk_stop_pct": 2.0
    }
    """
    # Log everything that hits this route
    raw_body = request.get_data(as_text=True)
    log.info("TV Webhook raw body: %s", raw_body)

    if request.method != "POST":
        log.info(
            "TV HIT with non-POST method=%s path=%s", request.method, request.path
        )
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    # Try to parse JSON
    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("TV JSON parse error")
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    log.info("TV Webhook parsed JSON: %s", data)

    symbol = str(data.get("symbol", "")).upper()
    event = str(data.get("event", "")).upper()
    side = str(data.get("side", "")).lower()
    risk_stop_pct = float(data.get("risk_stop_pct", RISK_PER_TRADE))

    log.info(
        "TV Webhook parsed -> symbol=%s, event=%s, side=%s, risk_stop_pct=%s, DRY_RUN=%s",
        symbol,
        event,
        side,
        risk_stop_pct,
        DRY_RUN,
    )

    if not symbol:
        log.warning("TV Webhook missing symbol")
        return jsonify({"ok": False, "error": "Missing symbol", "received": data}), 400

    # ------------------------------------------------------------------
    # EVENT / SIDE MAPPING
    # ------------------------------------------------------------------
    # We accept both the "BUY/SELL" and "ENTRY/EXIT" styles.
    action = None  # "BUY" or "SELL"

    if side == "long" and event in ("BUY", "ENTRY"):
        action = "BUY"
    elif side == "long" and event in ("SELL", "EXIT"):
        action = "SELL"
    else:
        log.warning("TV Webhook: unsupported combo event=%s side=%s", event, side)
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Unsupported event/side combo",
                    "event": event,
                    "side": side,
                }
            ),
            400,
        )

    # ------------------------------------------------------------------
    # DRY RUN HANDLING
    # ------------------------------------------------------------------
    if DRY_RUN:
        log.info(
            "DRY_RUN=True -> would place %s for %s with risk_stop_pct=%s",
            action,
            symbol,
            risk_stop_pct,
        )
        return (
            jsonify(
                {
                    "ok": True,
                    "dry_run": True,
                    "symbol": symbol,
                    "event": event,
                    "side": side,
                    "mapped_action": action,
                    "risk_stop_pct": risk_stop_pct,
                }
            ),
            200,
        )

    # ------------------------------------------------------------------
    # LIVE ORDER
    # ------------------------------------------------------------------
    try:
        qt_side = "long" if action == "BUY" else "sell"
        log.info(
            "LIVE ORDER: %s %s (qt_side=%s, risk_stop_pct=%s)",
            action,
            symbol,
            qt_side,
            risk_stop_pct,
        )

        result = qt_place_order(symbol, qt_side, risk_stop_pct)
        log.info("Order result: %s", result)

        return (
            jsonify(
                {
                    "ok": True,
                    "symbol": symbol,
                    "event": event,
                    "side": side,
                    "mapped_action": action,
                    "risk_stop_pct": risk_stop_pct,
                    "broker_result": result,
                }
            ),
            200,
        )

    except Exception as e:
        log.exception("ERROR: app-exception on /tv while placing order")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "order_failed",
                    "detail": str(e),
                }
            ),
            500,
        )


# ------------------------------------------------------------------------------
# Entry point for local dev (Render uses gunicorn via Procfile)
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
