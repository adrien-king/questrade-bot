import os
import time
import json
import uuid
import logging
from typing import Dict, Any, Optional, Tuple

import requests
from flask import Flask, request, jsonify

# ------------------------------------------------------------------------------
# Flask + logging
# ------------------------------------------------------------------------------
app = Flask(__name__)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = app.logger
log.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")  # Questrade refresh token (NOT TradingView token)
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")  # "1" practice, "0" live

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Position sizing
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))  # dollars at risk per trade
USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "0") == "1"

# Cooldowns (seconds)
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "0") or "0")
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "0") or "0")

# Optional: Google Sheets logger (Apps Script "web app" URL)
GOOGLE_SHEETS_WEBHOOK_URL = os.getenv("GOOGLE_SHEETS_WEBHOOK_URL")  # optional

# Optional auth for your /tv endpoint
TV_WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET")  # optional; if set, require header X-Webhook-Secret

# ------------------------------------------------------------------------------
# In-memory state (Render instances can restart; this is best-effort)
# ------------------------------------------------------------------------------
_last_global_trade_ts = 0.0
_last_symbol_trade_ts: Dict[str, float] = {}

# For DRY_RUN profit simulation: store last "entry" per symbol
_dry_positions: Dict[str, Dict[str, Any]] = {}
# example: _dry_positions["AMCI"] = {"side": "long", "shares": 10, "entry_price": 7.12, "ts": ...}

# ------------------------------------------------------------------------------
# Startup info
# ------------------------------------------------------------------------------
log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f "
    "RISK_PER_TRADE=%.2f MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s LOG_LEVEL=%s",
    QUESTRADE_PRACTICE,
    DRY_RUN,
    USE_RISK_SIZING,
    POSITION_DOLLARS,
    RISK_PER_TRADE,
    MAX_POSITION_USD,
    GLOBAL_COOLDOWN_SEC,
    SYMBOL_COOLDOWN_SEC,
    LOG_LEVEL,
)

# Validate Questrade config only if we might place live orders
if not DRY_RUN:
    if not QUESTRADE_REFRESH_TOKEN:
        raise ValueError("Missing Questrade refresh token (QUESTRADE_REFRESH_TOKEN).")
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise ValueError("Missing Questrade account number (QUESTRADE_ACCOUNT_NUMBER).")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _now() -> float:
    return time.time()


def _json_safe(obj: Any, maxlen: int = 2000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    return s if len(s) <= maxlen else s[:maxlen] + "...(truncated)"


def _mask(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "*" * (len(s) - keep)


def _login_base_url() -> str:
    return "https://login.questrade.com"


def qt_refresh_access_token() -> Tuple[str, str]:
    """
    Refresh token -> access_token + api_server
    """
    assert QUESTRADE_REFRESH_TOKEN, "QUESTRADE_REFRESH_TOKEN missing"
    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"

    log.info("QT refresh: GET %s token_prefix=%s...", _login_base_url(), _mask(QUESTRADE_REFRESH_TOKEN, 6))
    r = requests.get(url, timeout=20)

    # Log small snippet only (avoid dumping secrets)
    body_snip = (r.text or "")[:500]
    log.info("QT refresh response: status=%s body=%s", r.status_code, body_snip)

    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={body_snip}")

    data = r.json()
    access_token = data["access_token"]
    api_server = data["api_server"]  # e.g. "https://api01.iq.questrade.com/"
    return access_token, api_server


def qt_headers(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    if r.status_code != 200:
        raise Exception(f"Failed symbol search: {symbol} status={r.status_code} body={(r.text or '')[:500]}")
    data = r.json()
    symbols = data.get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol}")
    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])


def qt_get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    url = f"{api_server}v1/markets/quotes/{symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    if r.status_code != 200:
        raise Exception(f"Failed quote: {symbol} status={r.status_code} body={(r.text or '')[:500]}")
    data = r.json()
    return float(data["quotes"][0]["lastTradePrice"])


def _compute_shares(
    price: float,
    risk_stop_pct: float,
) -> int:
    """
    If USE_RISK_SIZING=1:
      shares = floor(RISK_PER_TRADE / (price * risk_stop_pct/100))
    Else:
      shares = floor(POSITION_DOLLARS / price)

    Always min 1 share.
    """
    if price <= 0:
        return 1

    if USE_RISK_SIZING:
        stop_dollars_per_share = price * (risk_stop_pct / 100.0)
        if stop_dollars_per_share <= 0:
            return max(1, int(POSITION_DOLLARS / price))
        shares = int(RISK_PER_TRADE / stop_dollars_per_share)
        return max(1, shares)

    return max(1, int(POSITION_DOLLARS / price))


def _stop_price_for_long(entry_price: float, risk_stop_pct: float) -> float:
    return round(entry_price * (1.0 - risk_stop_pct / 100.0), 4)


def _check_cooldowns(symbol: str) -> Optional[str]:
    global _last_global_trade_ts
    now = _now()

    if GLOBAL_COOLDOWN_SEC > 0:
        if now - _last_global_trade_ts < GLOBAL_COOLDOWN_SEC:
            return f"Global cooldown active ({GLOBAL_COOLDOWN_SEC}s)."

    if SYMBOL_COOLDOWN_SEC > 0:
        last = _last_symbol_trade_ts.get(symbol, 0.0)
        if now - last < SYMBOL_COOLDOWN_SEC:
            return f"Symbol cooldown active for {symbol} ({SYMBOL_COOLDOWN_SEC}s)."

    return None


def _mark_trade(symbol: str):
    global _last_global_trade_ts
    now = _now()
    _last_global_trade_ts = now
    _last_symbol_trade_ts[symbol] = now


def _log_to_google_sheets(payload: Dict[str, Any]):
    """
    Optional: send a JSON payload to your Apps Script Web App endpoint.
    """
    if not GOOGLE_SHEETS_WEBHOOK_URL:
        return
    try:
        requests.post(GOOGLE_SHEETS_WEBHOOK_URL, json=payload, timeout=10)
    except Exception:
        log.exception("Google Sheets logging failed (non-fatal)")


def qt_place_market_order(symbol: str, is_buy: bool, shares: int) -> Dict[str, Any]:
    """
    Live: place a Market Day order.
    """
    access_token, api_server = qt_refresh_access_token()
    symbol_id = qt_get_symbol_id(access_token, api_server, symbol)

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
                "legSide": "Buy" if is_buy else "Sell",
                "quantity": shares,
            }
        ],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    r = requests.post(url, headers=qt_headers(access_token), json=order_body, timeout=20)

    if r.status_code >= 300:
        raise Exception(f"Order rejected: status={r.status_code} body={(r.text or '')[:800]}")

    return r.json()


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/tv", methods=["POST", "GET"])
def tv():
    req_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())[:8]

    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    # Optional shared secret
    if TV_WEBHOOK_SECRET:
        got = request.headers.get("X-Webhook-Secret", "")
        if got != TV_WEBHOOK_SECRET:
            log.warning("[%s] Unauthorized webhook (bad secret)", req_id)
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    raw_body = request.get_data(as_text=True) or ""
    log.info("[%s] /tv raw body: %s", req_id, raw_body[:2000])

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("[%s] JSON parse error", req_id)
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    # Expected payload (recommended: include price!)
    # {
    #   "symbol":"AMCI",
    #   "event":"BUY"|"SELL"|"ENTRY"|"EXIT",
    #   "side":"long",
    #   "risk_stop_pct": 2.0,
    #   "price": 7.12   <-- strongly recommended for DRY_RUN profit calc
    # }
    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", 2.0))

    # Optional: price from TradingView alert placeholders
    price = data.get("price", None)
    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None

    log.info(
        "[%s] parsed: symbol=%s event=%s side=%s risk_stop_pct=%s price=%s DRY_RUN=%s",
        req_id, symbol, event, side, risk_stop_pct, price, DRY_RUN
    )

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol", "received": data}), 400

    # Map event+side -> action BUY/SELL for long-only
    action = None
    if side == "long" and event in ("BUY", "ENTRY"):
        action = "BUY"
    elif side == "long" and event in ("SELL", "EXIT"):
        action = "SELL"
    else:
        return jsonify({"ok": False, "error": "Unsupported event/side combo", "event": event, "side": side}), 400

    # Cooldowns
    cd = _check_cooldowns(symbol)
    if cd:
        log.warning("[%s] cooldown blocked: %s", req_id, cd)
        return jsonify({"ok": False, "error": "cooldown", "detail": cd}), 429

    # --------------------------------------------------------------------------
    # DRY RUN: RETURN BEFORE ANY QUESTRADE CALLS
    # --------------------------------------------------------------------------
    if DRY_RUN:
        # Simulate shares using either risk sizing or fixed notional.
        # For simulation we need a price. If not provided, we can't compute shares/profit.
        sim_shares = None
        sim_stop_price = None

        if price is not None and price > 0:
            sim_shares = _compute_shares(price, risk_stop_pct)
            sim_stop_price = _stop_price_for_long(price, risk_stop_pct)

        # Profit simulation: if we have a stored entry and this is an EXIT, compute PnL
        sim_pnl = None
        sim_entry = _dry_positions.get(symbol)

        if action == "BUY":
            if price is not None and sim_shares is not None:
                _dry_positions[symbol] = {
                    "side": "long",
                    "shares": sim_shares,
                    "entry_price": price,
                    "ts": _now(),
                }
        elif action == "SELL":
            if sim_entry and price is not None:
                # Long PnL = (exit - entry) * shares
                sim_pnl = round((price - float(sim_entry["entry_price"])) * int(sim_entry["shares"]), 4)
                # clear position on exit
                _dry_positions.pop(symbol, None)

        _mark_trade(symbol)

        resp = {
            "ok": True,
            "dry_run": True,
            "symbol": symbol,
            "event": event,
            "side": side,
            "mapped_action": action,
            "risk_stop_pct": risk_stop_pct,
            "use_risk_sizing": USE_RISK_SIZING,
            "risk_per_trade": RISK_PER_TRADE,
            "position_dollars": POSITION_DOLLARS,
            "price": price,
            "sim_shares": sim_shares,
            "sim_stop_price": sim_stop_price,
            "sim_position_value": (round(sim_shares * price, 4) if (sim_shares and price) else None),
            "sim_pnl": sim_pnl,
            "note": (
                "For best dry-run sizing & profit simulation, include price in your TradingView alert JSON "
                "(e.g., using placeholders like {{close}})."
            ),
            "received": data,
        }

        log.info("[%s] DRY_RUN response: %s", req_id, _json_safe(resp))
        _log_to_google_sheets({"req_id": req_id, "ts": int(_now()), "type": "dry_run", **resp})

        return jsonify(resp), 200

    # --------------------------------------------------------------------------
    # LIVE ORDER FLOW (Questrade calls happen only here)
    # --------------------------------------------------------------------------
    try:
        if not QUESTRADE_REFRESH_TOKEN or not QUESTRADE_ACCOUNT_NUMBER:
            return jsonify({"ok": False, "error": "missing_broker_config"}), 500

        # For live sizing, we can use either webhook price OR quote from Questrade
        use_price = price
        qt_access_token, qt_api_server = qt_refresh_access_token()

        if use_price is None:
            use_price = qt_get_last_price(qt_access_token, qt_api_server, symbol)

        shares = _compute_shares(float(use_price), risk_stop_pct)

        # safety cap
        notional = float(use_price) * shares
        if MAX_POSITION_USD > 0 and notional > MAX_POSITION_USD:
            raise Exception(f"Notional ${notional:.2f} exceeds MAX_POSITION_USD=${MAX_POSITION_USD:.2f}")

        stop_price = _stop_price_for_long(float(use_price), risk_stop_pct)

        log.info(
            "[%s] LIVE: action=%s symbol=%s price=%.4f shares=%s notional=%.2f stop=%.4f",
            req_id, action, symbol, float(use_price), shares, notional, stop_price
        )

        # Place market order
        result = qt_place_market_order(symbol, is_buy=(action == "BUY"), shares=shares)

        _mark_trade(symbol)

        resp = {
            "ok": True,
            "dry_run": False,
            "symbol": symbol,
            "event": event,
            "side": side,
            "mapped_action": action,
            "risk_stop_pct": risk_stop_pct,
            "use_risk_sizing": USE_RISK_SIZING,
            "risk_per_trade": RISK_PER_TRADE,
            "position_dollars": POSITION_DOLLARS,
            "price_used": float(use_price),
            "shares": shares,
            "notional": round(notional, 4),
            "computed_stop_price": stop_price,
            "broker_result": result,
        }

        _log_to_google_sheets({"req_id": req_id, "ts": int(_now()), "type": "live", **resp})
        return jsonify(resp), 200

    except Exception as e:
        log.exception("[%s] LIVE error", req_id)
        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# ------------------------------------------------------------------------------
# Local entrypoint (Render uses gunicorn)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
