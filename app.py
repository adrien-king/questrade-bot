import os
import time
import json
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

# ------------------------------------------------------------------------------
# Flask + logging setup
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = app.logger

# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
PRACTICE = os.getenv("PRACTICE", "1")  # informational; refresh token determines practice/live

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Position sizing controls
USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "true").lower() in ("1", "true", "yes", "on")
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))  # used if USE_RISK_SIZING = false
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))        # used if USE_RISK_SIZING = true
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")

# Cooldowns
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "0") or "0")
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "0") or "0")

# Google Sheets logging
SHEETS_ON = os.getenv("SHEETS", "off").lower() in ("1", "true", "yes", "on")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID") or os.getenv("GSHEET_ID")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")
POSITIONS_TAB = os.getenv("POSITIONS_TAB", "Positions")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "/etc/secrets/google_creds.json")

# ------------------------------------------------------------------------------
# Basic validation (only enforce what is truly required for this config)
# ------------------------------------------------------------------------------
if not DRY_RUN:
    if not QUESTRADE_REFRESH_TOKEN:
        raise ValueError("Missing Questrade refresh token (QUESTRADE_REFRESH_TOKEN).")
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise ValueError("Missing Questrade account number (QUESTRADE_ACCOUNT_NUMBER).")

if SHEETS_ON:
    if not GOOGLE_SHEET_ID:
        raise ValueError("SHEETS=on but missing GOOGLE_SHEET_ID (or GSHEET_ID).")
    if not os.path.exists(GOOGLE_CREDS_PATH):
        raise ValueError(f"SHEETS=on but GOOGLE_CREDS_PATH file not found: {GOOGLE_CREDS_PATH}")

log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f "
    "RISK_PER_TRADE=%.2f MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s "
    "SHEETS_ON=%s SHEET_ID=%s SHEET_TAB=%s POSITIONS_TAB=%s GOOGLE_CREDS_PATH=%s",
    PRACTICE, DRY_RUN, USE_RISK_SIZING, POSITION_DOLLARS, RISK_PER_TRADE,
    MAX_POSITION_USD, GLOBAL_COOLDOWN_SEC, SYMBOL_COOLDOWN_SEC,
    SHEETS_ON, (GOOGLE_SHEET_ID or "")[:12] + "..." if GOOGLE_SHEET_ID else None,
    GOOGLE_SHEET_TAB, POSITIONS_TAB, GOOGLE_CREDS_PATH
)

# ------------------------------------------------------------------------------
# Small utilities
# ------------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def _login_base_url() -> str:
    return "https://login.questrade.com"

# ------------------------------------------------------------------------------
# Google Sheets helpers (only used if SHEETS_ON)
# ------------------------------------------------------------------------------
_gs_client = None
_gs_book = None
_positions_cache = {"ts": 0, "data": {}}  # simple cache to reduce read calls


def gs_init():
    global _gs_client, _gs_book
    if not SHEETS_ON:
        return None
    if _gs_book is not None:
        return _gs_book

    # Import inside function to avoid import issues if SHEETS is off
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=scopes)
    _gs_client = gspread.authorize(creds)
    _gs_book = _gs_client.open_by_key(GOOGLE_SHEET_ID)
    return _gs_book


def gs_worksheet(name: str):
    book = gs_init()
    if book is None:
        return None
    try:
        return book.worksheet(name)
    except Exception:
        # Create if missing
        ws = book.add_worksheet(title=name, rows=2000, cols=30)
        return ws


def ensure_headers(ws, headers):
    # If sheet is empty, add headers; if first row differs, don’t overwrite (safe)
    values = ws.get_all_values()
    if not values:
        ws.append_row(headers)
        return

    first = values[0]
    # If it's shorter or blank-ish, write headers
    if len(first) < len(headers) or all((c.strip() == "" for c in first)):
        ws.update("A1", [headers])


def append_log_row(row_dict: dict):
    """
    Append a log row into GOOGLE_SHEET_TAB.
    Auto header creation.
    """
    if not SHEETS_ON:
        return

    ws = gs_worksheet(GOOGLE_SHEET_TAB)
    headers = [
        "timestamp", "symbol", "event", "side", "price", "shares",
        "position_value", "stop_price", "risk_usd", "status", "note", "pnl_usd"
    ]
    ensure_headers(ws, headers)

    # Ensure stable order
    row = [
        row_dict.get("timestamp", ""),
        row_dict.get("symbol", ""),
        row_dict.get("event", ""),
        row_dict.get("side", ""),
        row_dict.get("price", ""),
        row_dict.get("shares", ""),
        row_dict.get("position_value", ""),
        row_dict.get("stop_price", ""),
        row_dict.get("risk_usd", ""),
        row_dict.get("status", ""),
        row_dict.get("note", ""),
        row_dict.get("pnl_usd", ""),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")


def positions_read_all(force=False) -> dict:
    """
    Reads Positions tab into {SYMBOL: {state, entry_price, shares, entry_ts, ...}}.
    Caches for ~3 seconds to reduce API calls.
    """
    if not SHEETS_ON:
        return {}

    if not force and (time.time() - _positions_cache["ts"] < 3):
        return _positions_cache["data"]

    ws = gs_worksheet(POSITIONS_TAB)
    headers = [
        "symbol", "state", "entry_price", "shares", "entry_ts",
        "entry_value", "last_event", "last_ts"
    ]
    ensure_headers(ws, headers)

    rows = ws.get_all_values()
    data = {}
    if len(rows) >= 2:
        head = rows[0]
        idx = {h: head.index(h) for h in headers if h in head}
        for r in rows[1:]:
            if not r or len(r) < 1:
                continue
            sym = (r[idx["symbol"]] if "symbol" in idx and idx["symbol"] < len(r) else "").strip().upper()
            if not sym:
                continue

            def _get(col, default=""):
                i = idx.get(col, None)
                return r[i].strip() if i is not None and i < len(r) else default

            # parse numbers where possible
            def _f(x):
                try:
                    return float(x)
                except Exception:
                    return None

            def _i(x):
                try:
                    return int(float(x))
                except Exception:
                    return None

            data[sym] = {
                "symbol": sym,
                "state": _get("state", "FLAT") or "FLAT",
                "entry_price": _f(_get("entry_price", "")),
                "shares": _i(_get("shares", "")),
                "entry_ts": _get("entry_ts", ""),
                "entry_value": _f(_get("entry_value", "")),
                "last_event": _get("last_event", ""),
                "last_ts": _get("last_ts", ""),
            }

    _positions_cache["ts"] = time.time()
    _positions_cache["data"] = data
    return data


def pos_get(symbol: str) -> dict:
    return positions_read_all().get(symbol.upper(), {"symbol": symbol.upper(), "state": "FLAT"})


def pos_upsert(symbol: str, state: str, entry_price=None, shares=None, entry_value=None, last_event=None):
    """
    Upsert single symbol row in Positions.
    """
    if not SHEETS_ON:
        return

    symbol = symbol.upper()
    ws = gs_worksheet(POSITIONS_TAB)
    headers = [
        "symbol", "state", "entry_price", "shares", "entry_ts",
        "entry_value", "last_event", "last_ts"
    ]
    ensure_headers(ws, headers)

    # Find row by symbol in col A
    colA = ws.col_values(1)  # includes header at index 0
    row_idx = None
    for i, v in enumerate(colA[1:], start=2):
        if v.strip().upper() == symbol:
            row_idx = i
            break

    ts = now_iso()
    entry_ts = ts if state == "LONG" else ""
    payload = [
        symbol,
        state,
        "" if entry_price is None else entry_price,
        "" if shares is None else shares,
        entry_ts if state == "LONG" else "",
        "" if entry_value is None else entry_value,
        "" if last_event is None else last_event,
        ts,
    ]

    if row_idx is None:
        ws.append_row(payload, value_input_option="USER_ENTERED")
    else:
        ws.update(f"A{row_idx}:H{row_idx}", [payload])

    # invalidate cache
    _positions_cache["ts"] = 0
    _positions_cache["data"] = {}


# ------------------------------------------------------------------------------
# Questrade helpers
# ------------------------------------------------------------------------------
def qt_refresh_access_token():
    """
    Uses refresh token -> gets access_token + api_server
    """
    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    token_prefix = (QUESTRADE_REFRESH_TOKEN or "")[:6]
    log.info("QT: refreshing token token_prefix=%s...", token_prefix)

    r = requests.get(url, timeout=30)
    log.info("QT: token refresh status=%s body=%s", r.status_code, r.text[:400])

    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={r.text}")

    j = r.json()
    return j["access_token"], j["api_server"]


def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Symbol lookup failed for {symbol}: {r.status_code} {r.text}")

    data = r.json()
    syms = data.get("symbols", [])
    if not syms:
        raise Exception(f"No symbol found for {symbol}")

    exact = [s for s in syms if s.get("symbol", "").upper() == symbol.upper()]
    chosen = exact[0] if exact else syms[0]
    return int(chosen["symbolId"])


def qt_last_price(access_token: str, api_server: str, symbol: str) -> float:
    url = f"{api_server}v1/markets/quotes/{symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=30)
    if r.status_code != 200:
        raise Exception(f"Quote failed for {symbol}: {r.status_code} {r.text}")
    data = r.json()
    return float(data["quotes"][0]["lastTradePrice"])


def qt_place_market_order(symbol: str, side: str, shares: int):
    """
    side: "BUY" or "SELL"
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
        "orderLegs": [{
            "symbolId": symbol_id,
            "legSide": "Buy" if side == "BUY" else "Sell",
            "quantity": int(shares),
        }],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    r = requests.post(url, headers=qt_headers(access_token), json=order_body, timeout=30)
    if r.status_code >= 300:
        raise Exception(f"Order rejected: {r.status_code} {r.text}")
    return r.json()


# ------------------------------------------------------------------------------
# Sizing + sim helpers
# ------------------------------------------------------------------------------
def calc_shares_and_risk(price: float, risk_stop_pct: float):
    """
    Returns:
      shares, stop_price, risk_usd, position_value, note
    """
    if price <= 0:
        raise Exception("Invalid price for sizing")

    stop_price = price * (1 - (risk_stop_pct / 100.0))
    stop_dist = price - stop_price  # $ per share risk

    if USE_RISK_SIZING:
        if stop_dist <= 0:
            raise Exception("Invalid stop distance (risk_stop_pct too small?)")

        desired_risk = max(0.0, RISK_PER_TRADE)
        shares = int(desired_risk / stop_dist) if desired_risk > 0 else 1
        shares = max(1, shares)

        position_value = shares * price

        # clamp by MAX_POSITION_USD if set
        if MAX_POSITION_USD > 0 and position_value > MAX_POSITION_USD:
            shares = max(1, int(MAX_POSITION_USD / price))
            position_value = shares * price

        risk_usd = shares * stop_dist

        note = "Risk-sizing enabled (RISK_PER_TRADE / stop distance)."
        if MAX_POSITION_USD > 0:
            note += " Clamped by MAX_POSITION_USD."

        return shares, stop_price, round(risk_usd, 2), round(position_value, 2), note

    # fixed notional sizing
    desired = POSITION_DOLLARS
    if MAX_POSITION_USD > 0:
        desired = min(desired, MAX_POSITION_USD)

    shares = max(1, int(desired / price))
    position_value = shares * price
    risk_usd = shares * stop_dist
    note = "Fixed-notional sizing (POSITION_DOLLARS)."
    if MAX_POSITION_USD > 0:
        note += " Clamped by MAX_POSITION_USD."

    return shares, stop_price, round(risk_usd, 2), round(position_value, 2), note


# ------------------------------------------------------------------------------
# Simple in-memory cooldowns (secondary protection)
# ------------------------------------------------------------------------------
_last_global_ts = 0.0
_last_symbol_ts = {}  # symbol -> ts


def cooldown_check(symbol: str) -> str:
    """
    Returns "" if ok, else reason string.
    """
    global _last_global_ts
    symbol = symbol.upper()
    now = time.time()

    if GLOBAL_COOLDOWN_SEC > 0 and (now - _last_global_ts) < GLOBAL_COOLDOWN_SEC:
        return f"Global cooldown active ({GLOBAL_COOLDOWN_SEC}s)."

    if SYMBOL_COOLDOWN_SEC > 0:
        last = _last_symbol_ts.get(symbol, 0.0)
        if (now - last) < SYMBOL_COOLDOWN_SEC:
            return f"Symbol cooldown active for {symbol} ({SYMBOL_COOLDOWN_SEC}s)."

    return ""


def cooldown_mark(symbol: str):
    global _last_global_ts
    symbol = symbol.upper()
    now = time.time()
    _last_global_ts = now
    _last_symbol_ts[symbol] = now


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def root():
    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/tv", methods=["POST"])
def tv():
    """
    TradingView webhook endpoint.

    Example JSON:
    {
      "symbol": "AMCI",
      "event": "BUY" | "SELL" | "ENTRY" | "EXIT",
      "side": "long",
      "risk_stop_pct": 2.0,
      "price": 13.29
    }

    Notes:
    - If price is not provided, we will fetch last price from Questrade when not DRY_RUN.
      In DRY_RUN we prefer you include "price" so simulation is accurate.
    """
    raw = request.get_data(as_text=True)
    log.info("TV raw body: %s", raw[:2000])

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("Bad JSON")
        return jsonify({"ok": False, "error": "bad_json", "detail": str(e)}), 400

    # normalize
    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "long")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", 2.0))
    price_in = data.get("price", None)
    price = float(price_in) if price_in not in (None, "") else None

    if not symbol:
        return jsonify({"ok": False, "error": "missing_symbol"}), 400

    # Map to ENTRY/EXIT (your app logic)
    if side != "long":
        return jsonify({"ok": False, "error": "only_long_supported"}), 400

    if event in ("BUY", "ENTRY"):
        mapped = "ENTRY"
    elif event in ("SELL", "EXIT"):
        mapped = "EXIT"
    else:
        return jsonify({"ok": False, "error": "unsupported_event", "event": event}), 400

    # ------------------------------------------------------------------
    # ---- HARD STATE BLOCK (no multiple entry / exit) ----
    # ------------------------------------------------------------------
    current = pos_get(symbol) if SHEETS_ON else None
    state = (current.get("state") if current else "FLAT") or "FLAT"

    if mapped == "ENTRY" and state == "LONG":
        log.info("ENTRY ignored: already LONG for %s", symbol)
        if SHEETS_ON:
            append_log_row({
                "timestamp": now_iso(),
                "symbol": symbol,
                "event": event,
                "side": side,
                "price": price if price is not None else "",
                "shares": "",
                "position_value": "",
                "stop_price": "",
                "risk_usd": "",
                "status": "ignored",
                "note": "ENTRY ignored: already LONG",
                "pnl_usd": ""
            })
        return jsonify({
            "ok": True,
            "ignored": True,
            "reason": "Already in position (LONG)"
        }), 200

    if mapped == "EXIT" and state != "LONG":
        log.info("EXIT ignored: no open position for %s", symbol)
        if SHEETS_ON:
            append_log_row({
                "timestamp": now_iso(),
                "symbol": symbol,
                "event": event,
                "side": side,
                "price": price if price is not None else "",
                "shares": "",
                "position_value": "",
                "stop_price": "",
                "risk_usd": "",
                "status": "ignored",
                "note": "EXIT ignored: no open position",
                "pnl_usd": ""
            })
        return jsonify({
            "ok": True,
            "ignored": True,
            "reason": "No open position to exit"
        }), 200

    # ------------------------------------------------------------------
    # Cooldowns (secondary protection)
    # ------------------------------------------------------------------
    cd_reason = cooldown_check(symbol)
    if cd_reason:
        log.warning("Cooldown blocked: %s", cd_reason)
        if SHEETS_ON:
            append_log_row({
                "timestamp": now_iso(),
                "symbol": symbol,
                "event": event,
                "side": side,
                "price": price if price is not None else "",
                "shares": "",
                "position_value": "",
                "stop_price": "",
                "risk_usd": "",
                "status": "cooldown",
                "note": cd_reason,
                "pnl_usd": ""
            })
        return jsonify({"ok": True, "blocked": True, "reason": cd_reason}), 200

    # ------------------------------------------------------------------
    # Determine price if missing
    # ------------------------------------------------------------------
    if price is None:
        if DRY_RUN:
            # In DRY_RUN we don't want to refresh tokens unless you really want it
            return jsonify({
                "ok": False,
                "error": "missing_price_in_dry_run",
                "detail": "Include 'price' in your TradingView JSON for accurate dry-run simulation."
            }), 400

        # live mode: fetch last price from QT
        access_token, api_server = qt_refresh_access_token()
        price = qt_last_price(access_token, api_server, symbol)

    # ------------------------------------------------------------------
    # Compute sizing
    # ------------------------------------------------------------------
    shares, stop_price, risk_usd, position_value, sizing_note = calc_shares_and_risk(price, risk_stop_pct)

    # ------------------------------------------------------------------
    # Execute (DRY_RUN vs LIVE)
    # ------------------------------------------------------------------
    if DRY_RUN:
        pnl_usd = ""
        status = "dry_run"

        if mapped == "ENTRY":
            # record position state
            if SHEETS_ON:
                pos_upsert(
                    symbol=symbol,
                    state="LONG",
                    entry_price=price,
                    shares=shares,
                    entry_value=position_value,
                    last_event="ENTRY"
                )

        if mapped == "EXIT":
            # compute realized P&L using stored entry
            entry_price = None
            entry_shares = None
            if SHEETS_ON:
                cur = pos_get(symbol)
                entry_price = cur.get("entry_price")
                entry_shares = cur.get("shares")

            if entry_price is not None and entry_shares:
                pnl = (price - float(entry_price)) * int(entry_shares)
                pnl_usd = round(pnl, 2)
                sizing_note = f"entry_price={entry_price} exit_price={price} shares={entry_shares}"
            else:
                pnl_usd = ""
                sizing_note = "No stored entry found to compute pnl."

            if SHEETS_ON:
                pos_upsert(
                    symbol=symbol,
                    state="FLAT",
                    entry_price="",
                    shares="",
                    entry_value="",
                    last_event="EXIT"
                )

        if SHEETS_ON:
            append_log_row({
                "timestamp": now_iso(),
                "symbol": symbol,
                "event": event,
                "side": side,
                "price": price,
                "shares": shares,
                "position_value": position_value,
                "stop_price": round(stop_price, 4),
                "risk_usd": risk_usd,
                "status": status,
                "note": sizing_note,
                "pnl_usd": pnl_usd
            })

        cooldown_mark(symbol)

        return jsonify({
            "ok": True,
            "dry_run": True,
            "symbol": symbol,
            "event": event,
            "mapped": mapped,
            "side": side,
            "price": price,
            "shares": shares,
            "position_value": position_value,
            "stop_price": round(stop_price, 4),
            "risk_usd": risk_usd,
            "pnl_usd": pnl_usd,
            "note": sizing_note
        }), 200

    # LIVE
    try:
        order_side = "BUY" if mapped == "ENTRY" else "SELL"

        broker = qt_place_market_order(symbol, order_side, shares)

        if SHEETS_ON:
            # update position state
            if mapped == "ENTRY":
                pos_upsert(symbol, "LONG", entry_price=price, shares=shares, entry_value=position_value, last_event="ENTRY")
            else:
                pos_upsert(symbol, "FLAT", entry_price="", shares="", entry_value="", last_event="EXIT")

            append_log_row({
                "timestamp": now_iso(),
                "symbol": symbol,
                "event": event,
                "side": side,
                "price": price,
                "shares": shares,
                "position_value": position_value,
                "stop_price": round(stop_price, 4),
                "risk_usd": risk_usd,
                "status": "live",
                "note": sizing_note,
                "pnl_usd": ""
            })

        cooldown_mark(symbol)

        return jsonify({
            "ok": True,
            "dry_run": False,
            "symbol": symbol,
            "event": event,
            "mapped": mapped,
            "order_side": order_side,
            "price": price,
            "shares": shares,
            "position_value": position_value,
            "stop_price": round(stop_price, 4),
            "risk_usd": risk_usd,
            "broker_result": broker
        }), 200

    except Exception as e:
        log.exception("LIVE order failed")
        if SHEETS_ON:
            append_log_row({
                "timestamp": now_iso(),
                "symbol": symbol,
                "event": event,
                "side": side,
                "price": price,
                "shares": shares,
                "position_value": position_value,
                "stop_price": round(stop_price, 4),
                "risk_usd": risk_usd,
                "status": "error",
                "note": str(e),
                "pnl_usd": ""
            })
        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# ------------------------------------------------------------------------------
# Entry point for local dev
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
