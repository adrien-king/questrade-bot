# app.py
import os
import time
import json
import math
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Flask, request, jsonify

# Google Sheets (service account)
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ------------------------------------------------------------------------------
# Flask + logging setup
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = app.logger

# ------------------------------------------------------------------------------
# ENV / CONFIG
# ------------------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")

# practice flag is informational in this simple bot (refresh token determines env)
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")  # "1"=practice, "0"=live

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# sizing
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")

USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "1") == "1"
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))  # $ risk per trade

# cooldowns
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "0") or "0")
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "0") or "0")

# Google Sheets logging switch + settings
SHEETS = os.getenv("SHEETS", "off").lower()  # "on" / "off"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID") or os.getenv("GSHEET_ID")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")

# ✅ requested: use GOOGLE_CREDS_PATH env var (with Render Secret File fallback)
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "/etc/secrets/google_creds.json")

# ------------------------------------------------------------------------------
# Basic validation (don't hard-crash if DRY_RUN; let /tv still work)
# ------------------------------------------------------------------------------
if not QUESTRADE_ACCOUNT_NUMBER:
    log.warning("Missing QUESTRADE_ACCOUNT_NUMBER env var.")
if not QUESTRADE_REFRESH_TOKEN:
    log.warning("Missing QUESTRADE_REFRESH_TOKEN env var.")

log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f "
    "RISK_PER_TRADE=%.2f MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s "
    "SHEETS=%s SHEET_ID=%s SHEET_TAB=%s GOOGLE_CREDS_PATH=%s",
    QUESTRADE_PRACTICE,
    DRY_RUN,
    USE_RISK_SIZING,
    POSITION_DOLLARS,
    RISK_PER_TRADE,
    MAX_POSITION_USD,
    GLOBAL_COOLDOWN_SEC,
    SYMBOL_COOLDOWN_SEC,
    SHEETS,
    (GOOGLE_SHEET_ID[:6] + "..." if GOOGLE_SHEET_ID else None),
    GOOGLE_SHEET_TAB,
    GOOGLE_CREDS_PATH,
)

# ------------------------------------------------------------------------------
# In-memory state (Render free tier restarts can wipe this; OK for now)
# ------------------------------------------------------------------------------
_last_global_action_ts = 0.0
_last_symbol_action_ts: Dict[str, float] = {}

# Simulated positions for DRY_RUN P&L (per-symbol)
# { "AMCI": {"side":"long", "shares": 10, "avg_price": 13.25, "stop_price": 12.98, "opened_ts": ...} }
_sim_positions: Dict[str, Dict[str, Any]] = {}

# ------------------------------------------------------------------------------
# Helpers: Questrade
# ------------------------------------------------------------------------------
def _login_base_url() -> str:
    return "https://login.questrade.com"


def qt_refresh_access_token() -> Tuple[str, str]:
    """
    Use refresh token to get (access_token, api_server).
    """
    if not QUESTRADE_REFRESH_TOKEN:
        raise Exception("Missing QUESTRADE_REFRESH_TOKEN")

    url = (
        f"{_login_base_url()}/oauth2/token"
        f"?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    )
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={r.text}")

    data = r.json()
    return data["access_token"], data["api_server"]  # e.g. https://api01.iq.questrade.com/


def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    if r.status_code != 200:
        raise Exception(f"Symbol lookup failed: {symbol} status={r.status_code} body={r.text}")

    symbols = (r.json() or {}).get("symbols", [])
    if not symbols:
        raise Exception(f"No symbol results for {symbol}")

    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])


def qt_get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    # Questrade quotes endpoint typically expects numeric IDs; some allow string.
    # We'll try with symbol string first, then fall back to symbolId if needed.
    url = f"{api_server}v1/markets/quotes/{symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    if r.status_code != 200:
        # fallback: use symbolId
        sym_id = qt_get_symbol_id(access_token, api_server, symbol)
        url2 = f"{api_server}v1/markets/quotes/{sym_id}"
        r2 = requests.get(url2, headers=qt_headers(access_token), timeout=20)
        if r2.status_code != 200:
            raise Exception(
                f"Quote failed: {symbol} status={r2.status_code} body={r2.text}"
            )
        data = r2.json()
    else:
        data = r.json()

    quotes = (data or {}).get("quotes", [])
    if not quotes:
        raise Exception(f"No quote data for {symbol}")
    return float(quotes[0].get("lastTradePrice") or 0.0)


def qt_place_market_order(symbol: str, action: str, shares: int) -> Dict[str, Any]:
    """
    action: "BUY" or "SELL"
    """
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise Exception("Missing QUESTRADE_ACCOUNT_NUMBER")

    access_token, api_server = qt_refresh_access_token()
    symbol_id = qt_get_symbol_id(access_token, api_server, symbol)

    leg_side = "Buy" if action == "BUY" else "Sell"
    order_body = {
        "accountNumber": QUESTRADE_ACCOUNT_NUMBER,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderLegs": [{"symbolId": symbol_id, "legSide": leg_side, "quantity": shares}],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    r = requests.post(url, headers=qt_headers(access_token), json=order_body, timeout=20)
    if r.status_code >= 300:
        raise Exception(f"Order rejected: status={r.status_code} body={r.text}")
    return r.json()


# ------------------------------------------------------------------------------
# Helpers: sizing + cooldown
# ------------------------------------------------------------------------------
def _now_ts() -> float:
    return time.time()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cooldown_blocked(symbol: str) -> Optional[str]:
    global _last_global_action_ts, _last_symbol_action_ts

    now = _now_ts()

    if GLOBAL_COOLDOWN_SEC > 0 and (now - _last_global_action_ts) < GLOBAL_COOLDOWN_SEC:
        remain = GLOBAL_COOLDOWN_SEC - (now - _last_global_action_ts)
        return f"Global cooldown active ({remain:.0f}s)."

    if SYMBOL_COOLDOWN_SEC > 0:
        last = _last_symbol_action_ts.get(symbol, 0.0)
        if (now - last) < SYMBOL_COOLDOWN_SEC:
            remain = SYMBOL_COOLDOWN_SEC - (now - last)
            return f"Symbol cooldown active for {symbol} ({remain:.0f}s)."

    return None


def _cooldown_touch(symbol: str) -> None:
    global _last_global_action_ts, _last_symbol_action_ts
    now = _now_ts()
    _last_global_action_ts = now
    _last_symbol_action_ts[symbol] = now


def compute_shares(
    price: float,
    risk_stop_pct: float,
) -> Tuple[int, float, float, str]:
    """
    Returns: (shares, stop_price, position_value, note)
    - If USE_RISK_SIZING: shares = floor(RISK_PER_TRADE / (price - stop_price))
    - Else: shares = floor(POSITION_DOLLARS / price)
    Always >= 1
    """
    if price <= 0:
        return 1, 0.0, 0.0, "Invalid price; defaulted to 1 share."

    stop_price = price * (1.0 - (risk_stop_pct / 100.0)) if risk_stop_pct > 0 else 0.0
    stop_dist = max(0.0000001, price - stop_price) if stop_price > 0 else 0.0

    if USE_RISK_SIZING and risk_stop_pct > 0 and stop_dist > 0:
        raw = RISK_PER_TRADE / stop_dist
        shares = max(1, int(math.floor(raw)))
        note = "Risk-sizing enabled (RISK_PER_TRADE / stop distance)."
    else:
        shares = max(1, int(math.floor(POSITION_DOLLARS / price)))
        note = "Fixed notional sizing (POSITION_DOLLARS / price)."

    position_value = float(shares) * float(price)

    if MAX_POSITION_USD > 0 and position_value > MAX_POSITION_USD:
        # clamp down
        shares = max(1, int(math.floor(MAX_POSITION_USD / price)))
        position_value = float(shares) * float(price)
        note += " Clamped by MAX_POSITION_USD."

    return shares, stop_price, position_value, note


# ------------------------------------------------------------------------------
# Helpers: Google Sheets logging
# ------------------------------------------------------------------------------
_sheets_service = None


def sheets_enabled() -> bool:
    return (
        SHEETS == "on"
        and bool(GOOGLE_SHEET_ID)
        and bool(GOOGLE_SHEET_TAB)
        and bool(GOOGLE_CREDS_PATH)
    )


def get_sheets_service():
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service

    if not sheets_enabled():
        raise Exception("Sheets logging not enabled or missing config.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDS_PATH, scopes=scopes
    )
    _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _sheets_service


def append_sheet_row(row: Dict[str, Any]) -> None:
    """
    Appends one row to the sheet/tab.
    Your header row should already exist in row 1.
    """
    if not sheets_enabled():
        return

    # Column order must match your sheet headers:
    headers = [
        "timestamp",
        "symbol",
        "event",
        "side",
        "price",
        "shares",
        "position_value",
        "stop_price",
        "risk_usd",
        "status",
        "note",
    ]

    values = [[str(row.get(h, "")) for h in headers]]

    svc = get_sheets_service()
    body = {"values": values}
    range_name = f"{GOOGLE_SHEET_TAB}!A:K"
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=range_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def root():
    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "ok": True,
            "dry_run": DRY_RUN,
            "practice": QUESTRADE_PRACTICE,
            "sheets_enabled": sheets_enabled(),
            "sheet_id_set": bool(GOOGLE_SHEET_ID),
            "sheet_tab": GOOGLE_SHEET_TAB,
        }
    ), 200


@app.route("/tv", methods=["POST", "GET"])
def tv():
    """
    TradingView webhook endpoint.

    Example JSON (ENTRY):
      {"symbol":"AMCI","event":"BUY","side":"long","risk_stop_pct":2.0,"price":13.19}

    Example JSON (EXIT):
      {"symbol":"AMCI","event":"SELL","side":"long","risk_stop_pct":2.0,"price":13.55}

    Notes:
    - Include "price" for best dry-run profit simulation.
      In TradingView alert message you can do: "price":"{{close}}"
    """
    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    raw_body = request.get_data(as_text=True)
    log.info("TV raw body: %s", raw_body[:2000])

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("Bad JSON")
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", 0) or 0)
    price_in = data.get("price", None)

    # price is optional, but helps a LOT for dry-run P&L
    try:
        price = float(price_in) if price_in is not None else 0.0
    except Exception:
        price = 0.0

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol", "received": data}), 400

    # Map events to BUY/SELL
    action = None
    if side == "long" and event in ("BUY", "ENTRY"):
        action = "BUY"
    elif side == "long" and event in ("SELL", "EXIT"):
        action = "SELL"
    else:
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

    # Cooldowns
    blocked = _cooldown_blocked(symbol)
    if blocked:
        row = {
            "timestamp": _iso_now(),
            "symbol": symbol,
            "event": event,
            "side": side,
            "price": price if price else "",
            "shares": "",
            "position_value": "",
            "stop_price": "",
            "risk_usd": "",
            "status": "cooldown",
            "note": blocked,
        }
        try:
            append_sheet_row(row)
        except Exception:
            log.exception("Sheets append failed (cooldown row)")
        return jsonify({"ok": False, "error": "cooldown", "detail": blocked}), 429

    # If no price provided, we can still function:
    # - DRY_RUN: compute shares via POSITION_DOLLARS but shares might be wrong; warn
    # - LIVE: fetch quote from Questrade for sizing
    note_extra = ""
    if price <= 0:
        note_extra = " For best dry-run sizing and profit simulation, include price in alert JSON (e.g. {{close}})."

    # DRY RUN path
    if DRY_RUN:
        # if no price provided, try to fetch a quote (best effort)
        fetched_price = None
        if price <= 0:
            try:
                access_token, api_server = qt_refresh_access_token()
                fetched_price = qt_get_last_price(access_token, api_server, symbol)
                price = fetched_price
            except Exception as e:
                log.warning("DRY_RUN quote fetch failed: %s", str(e))

        shares, stop_price, position_value, sizing_note = compute_shares(
            price=price if price > 0 else 1.0,
            risk_stop_pct=risk_stop_pct if risk_stop_pct > 0 else 0.0,
        )

        # Simulated P&L
        sim_pnl = None
        status = "dry_run"

        if action == "BUY":
            _sim_positions[symbol] = {
                "side": "long",
                "shares": shares,
                "avg_price": price,
                "stop_price": stop_price,
                "opened_ts": _iso_now(),
            }
        else:  # SELL / EXIT
            pos = _sim_positions.get(symbol)
            if pos and pos.get("side") == "long":
                entry = float(pos.get("avg_price", 0))
                sh = int(pos.get("shares", 0))
                if entry > 0 and sh > 0 and price > 0:
                    sim_pnl = (price - entry) * sh
                    status = "dry_run_exit"
                _sim_positions.pop(symbol, None)
            else:
                status = "dry_run_exit_no_position"

        _cooldown_touch(symbol)

        row = {
            "timestamp": _iso_now(),
            "symbol": symbol,
            "event": event,
            "side": side,
            "price": round(price, 6) if price > 0 else "",
            "shares": shares,
            "position_value": round(position_value, 2),
            "stop_price": round(stop_price, 6) if stop_price > 0 else "",
            "risk_usd": round(RISK_PER_TRADE, 2) if USE_RISK_SIZING else "",
            "status": status,
            "note": sizing_note + (f" sim_pnl={sim_pnl:.2f}" if sim_pnl is not None else "") + note_extra,
        }

        try:
            append_sheet_row(row)
        except Exception:
            log.exception("Sheets append failed (dry run)")

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
                    "use_risk_sizing": USE_RISK_SIZING,
                    "risk_per_trade": RISK_PER_TRADE,
                    "position_dollars": POSITION_DOLLARS,
                    "price": price if price > 0 else None,
                    "sim_shares": shares,
                    "sim_stop_price": stop_price if stop_price > 0 else None,
                    "sim_position_value": position_value,
                    "sim_pnl": sim_pnl,
                    "note": sizing_note + note_extra,
                    "received": data,
                }
            ),
            200,
        )

    # LIVE path
    try:
        # Always fetch quote from Questrade for LIVE sizing
        access_token, api_server = qt_refresh_access_token()
        last_price = qt_get_last_price(access_token, api_server, symbol)

        shares, stop_price, position_value, sizing_note = compute_shares(
            price=last_price,
            risk_stop_pct=risk_stop_pct,
        )

        if MAX_POSITION_USD > 0 and position_value > MAX_POSITION_USD:
            raise Exception("Computed position exceeds MAX_POSITION_USD after clamp (unexpected).")

        broker_result = qt_place_market_order(symbol, action, shares)

        _cooldown_touch(symbol)

        row = {
            "timestamp": _iso_now(),
            "symbol": symbol,
            "event": event,
            "side": side,
            "price": round(last_price, 6),
            "shares": shares,
            "position_value": round(position_value, 2),
            "stop_price": round(stop_price, 6) if stop_price > 0 else "",
            "risk_usd": round(RISK_PER_TRADE, 2) if USE_RISK_SIZING else "",
            "status": "live_sent",
            "note": sizing_note + " (Stop-loss not auto-placed in this version.)",
        }

        try:
            append_sheet_row(row)
        except Exception:
            log.exception("Sheets append failed (live)")

        return jsonify(
            {
                "ok": True,
                "dry_run": False,
                "symbol": symbol,
                "event": event,
                "side": side,
                "mapped_action": action,
                "risk_stop_pct": risk_stop_pct,
                "price": last_price,
                "shares": shares,
                "position_value": position_value,
                "stop_price": stop_price,
                "broker_result": broker_result,
                "note": sizing_note,
            }
        ), 200

    except Exception as e:
        log.exception("Exception on /tv (live)")
        row = {
            "timestamp": _iso_now(),
            "symbol": symbol,
            "event": event,
            "side": side,
            "price": price if price else "",
            "shares": "",
            "position_value": "",
            "stop_price": "",
            "risk_usd": "",
            "status": "error",
            "note": str(e),
        }
        try:
            append_sheet_row(row)
        except Exception:
            log.exception("Sheets append failed (error row)")

        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# ------------------------------------------------------------------------------
# Local dev entry point (Render uses gunicorn via Procfile)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
