import os
import time
import uuid
import json
import math
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify

# Google Sheets (service account)
from google.oauth2 import service_account
from googleapiclient.discovery import build


# =============================================================================
# Flask + logging
# =============================================================================
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = app.logger


# =============================================================================
# Small helpers
# =============================================================================
def env_truthy(*keys: str, default: str = "0") -> bool:
    """
    Accepts values: 1/true/on/yes/y
    Checks multiple env var keys in order.
    """
    val = None
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            val = v
            break
    if val is None:
        val = default
    val = str(val).strip().lower()
    return val in ("1", "true", "on", "yes", "y")

def env_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


# =============================================================================
# ENV / CONFIG
# =============================================================================

# --- Questrade ---
QUESTRADE_REFRESH_TOKEN = env_first("QUESTRADE_REFRESH_TOKEN").strip()
QUESTRADE_ACCOUNT_NUMBER = env_first("QUESTRADE_ACCOUNT_NUMBER").strip()
PRACTICE = env_first("QUESTRADE_PRACTICE", default="1").strip()  # "1" practice, "0" live

# --- Bot behavior ---
DRY_RUN = env_truthy("DRY_RUN", default="1")
USE_RISK_SIZING = env_truthy("USE_RISK_SIZING", default="0")
POSITION_DOLLARS = float(env_first("POSITION_DOLLARS", default="1000") or "1000")
RISK_PER_TRADE = float(env_first("RISK_PER_TRADE", default="50") or "50")
MAX_POSITION_USD = float(env_first("MAX_POSITION_USD", default="0") or "0")

GLOBAL_COOLDOWN_SEC = int(env_first("GLOBAL_COOLDOWN_SEC", default="0") or "0")
SYMBOL_COOLDOWN_SEC = int(env_first("SYMBOL_COOLDOWN_SEC", default="0") or "0")

# --- Sheets ---
# IMPORTANT FIX:
# Accept BOTH:
#   SHEETS_ON=1
#   SHEETS=on   (what your screenshot shows)
SHEETS_ON = env_truthy("SHEETS_ON", "SHEETS", "SHEETS_ENABLED", default="0")

# Accept multiple possible sheet id var names (people often rename these)
GOOGLE_SHEET_ID = env_first("GOOGLE_SHEET_ID", "GSHEET_ID", "GOOGLE_SHEETS_ID", default="").strip()

GOOGLE_SHEET_TAB = env_first("GOOGLE_SHEET_TAB", default="Sheet1").strip()  # raw log tab
POSITIONS_TAB = env_first("POSITIONS_TAB", default="Positions").strip()
PNL_TAB = env_first("PNL_TAB", default="PnL").strip()
DAILY_TAB = env_first("DAILY_TAB", default="Daily").strip()
DASHBOARD_TAB = env_first("DASHBOARD_TAB", default="Dashboard").strip()

# Path to Render secret file (recommended) e.g. /etc/secrets/google_creds.json
GOOGLE_CREDS_PATH = env_first("GOOGLE_CREDS_PATH", default="/etc/secrets/google_creds.json").strip()

# Instead of crashing the whole service at import time, we will disable sheets and log warnings.
if SHEETS_ON and not GOOGLE_SHEET_ID:
    log.warning("SHEETS is ON but GOOGLE_SHEET_ID is missing. Disabling Sheets logging.")
    SHEETS_ON = False

if SHEETS_ON and not os.path.exists(GOOGLE_CREDS_PATH):
    log.warning("SHEETS is ON but GOOGLE_CREDS_PATH not found at %s. Disabling Sheets logging.", GOOGLE_CREDS_PATH)
    SHEETS_ON = False

log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f RISK_PER_TRADE=%.2f "
    "MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s SHEETS_ON=%s SHEET_ID=%s SHEET_TAB=%s "
    "GOOGLE_CREDS_PATH=%s",
    PRACTICE, DRY_RUN, USE_RISK_SIZING, POSITION_DOLLARS, RISK_PER_TRADE, MAX_POSITION_USD,
    GLOBAL_COOLDOWN_SEC, SYMBOL_COOLDOWN_SEC,
    SHEETS_ON, (GOOGLE_SHEET_ID[:6] + "...") if GOOGLE_SHEET_ID else "", GOOGLE_SHEET_TAB,
    GOOGLE_CREDS_PATH
)


# =============================================================================
# Questrade helpers
# =============================================================================
def _login_base_url() -> str:
    return "https://login.questrade.com"

def qt_refresh_access_token() -> Tuple[str, str]:
    """
    Uses refresh token to get access_token + api_server.
    NOTE: In DRY_RUN we do NOT call Questrade.
    """
    if not QUESTRADE_REFRESH_TOKEN:
        raise Exception("Missing QUESTRADE_REFRESH_TOKEN")

    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    log.info("qt_refresh_access_token: GET %s", url.split("refresh_token=")[0] + "refresh_token=***")

    r = requests.get(url, timeout=20)
    log.info("qt_refresh_access_token: status=%s body=%s", r.status_code, r.text[:300])

    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={r.text[:300]}")

    data = r.json()
    return data["access_token"], data["api_server"]

def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}

def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    if r.status_code != 200:
        raise Exception(f"Failed symbol lookup for {symbol}: {r.text[:300]}")
    symbols = (r.json() or {}).get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol}")
    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])

def qt_place_market_order(symbol: str, action: str, shares: int) -> dict:
    """
    action: "BUY" or "SELL"
    """
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise Exception("Missing QUESTRADE_ACCOUNT_NUMBER")

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
            "legSide": "Buy" if action == "BUY" else "Sell",
            "quantity": int(shares),
        }],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    log.info("qt_place_market_order: POST %s body=%s", url, {"...": "redacted", "orderLegs": order_body["orderLegs"]})

    r = requests.post(url, headers=qt_headers(access_token), json=order_body, timeout=20)
    log.info("qt_place_market_order: status=%s body=%s", r.status_code, r.text[:500])

    if r.status_code >= 300:
        raise Exception(f"Order rejected: {r.status_code} {r.text[:500]}")
    return r.json()


# =============================================================================
# Position sizing / stop
# =============================================================================
def calc_stop_price(price: float, risk_stop_pct: float) -> float:
    return round(price * (1.0 - (risk_stop_pct / 100.0)), 4)

def calc_shares(price: float, risk_stop_pct: float) -> Tuple[int, float, float, str]:
    """
    Returns (shares, position_value, risk_usd, note)
    """
    if price <= 0:
        return 0, 0.0, 0.0, "invalid_price"

    note = ""

    if USE_RISK_SIZING:
        stop_dist = price * (risk_stop_pct / 100.0)
        if stop_dist <= 0:
            return 0, 0.0, 0.0, "invalid_stop_dist"

        shares = int(math.floor(RISK_PER_TRADE / stop_dist))
        shares = max(1, shares)
        position_value = shares * price
        risk_usd = shares * stop_dist
        note = "Risk-sizing enabled (RISK_PER_TRADE / stop distance)."
    else:
        shares = int(math.floor(POSITION_DOLLARS / price))
        shares = max(1, shares)
        position_value = shares * price
        risk_usd = RISK_PER_TRADE
        note = "Fixed notional sizing (POSITION_DOLLARS / price)."

    if MAX_POSITION_USD > 0 and position_value > MAX_POSITION_USD:
        clamp_shares = int(math.floor(MAX_POSITION_USD / price))
        clamp_shares = max(1, clamp_shares)
        shares = clamp_shares
        position_value = shares * price

        if USE_RISK_SIZING:
            stop_dist = price * (risk_stop_pct / 100.0)
            risk_usd = shares * stop_dist

        note += f" Clamped by MAX_POSITION_USD={MAX_POSITION_USD:.2f}."

    return int(shares), round(position_value, 2), round(risk_usd, 2), note


# =============================================================================
# Cooldowns (in-memory)
# =============================================================================
_last_global_ts = 0.0
_last_symbol_ts: Dict[str, float] = {}

def cooldown_block(symbol: str) -> Optional[str]:
    global _last_global_ts
    now = time.time()

    if GLOBAL_COOLDOWN_SEC > 0:
        if now - _last_global_ts < GLOBAL_COOLDOWN_SEC:
            return f"Global cooldown active ({GLOBAL_COOLDOWN_SEC}s)."
    if SYMBOL_COOLDOWN_SEC > 0:
        last = _last_symbol_ts.get(symbol, 0.0)
        if now - last < SYMBOL_COOLDOWN_SEC:
            return f"Symbol cooldown active for {symbol} ({SYMBOL_COOLDOWN_SEC}s)."
    return None

def cooldown_mark(symbol: str):
    global _last_global_ts
    now = time.time()
    _last_global_ts = now
    _last_symbol_ts[symbol] = now


# =============================================================================
# Google Sheets helpers
# =============================================================================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_svc = None

def sheets_service():
    global _svc
    if _svc is not None:
        return _svc
    creds = service_account.Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=SCOPES)
    _svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _svc

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def ensure_tabs_exist(tab_names: List[str]):
    if not SHEETS_ON:
        return
    svc = sheets_service()
    meta = svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}

    requests_batch = []
    for name in tab_names:
        if name not in existing:
            requests_batch.append({"addSheet": {"properties": {"title": name}}})

    if requests_batch:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={"requests": requests_batch}
        ).execute()
        log.info("Created missing tabs: %s", [r["addSheet"]["properties"]["title"] for r in requests_batch])

def get_header(tab: str) -> List[str]:
    svc = sheets_service()
    rng = f"{tab}!1:1"
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    vals = (res.get("values") or [])
    return vals[0] if vals else []

def set_header_if_missing(tab: str, header: List[str]):
    if not SHEETS_ON:
        return
    svc = sheets_service()
    existing = get_header(tab)
    if existing and len(existing) > 0:
        return
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!1:1",
        valueInputOption="RAW",
        body={"values": [header]}
    ).execute()
    log.info("Header created on tab=%s", tab)

def append_row(tab: str, row: List[Any]):
    if not SHEETS_ON:
        return
    svc = sheets_service()
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()

def read_table(tab: str) -> Tuple[List[str], List[List[str]]]:
    svc = sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A:Z"
    ).execute()
    values = res.get("values") or []
    if not values:
        return [], []
    header = values[0]
    rows = values[1:]
    return header, rows

def update_row(tab: str, row_index_1based: int, values: List[Any]):
    svc = sheets_service()
    rng = f"{tab}!A{row_index_1based}:Z{row_index_1based}"
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": [values]}
    ).execute()


# =============================================================================
# Sheet schemas
# =============================================================================
RAW_HEADER = [
    "timestamp", "symbol", "event", "mapped", "side",
    "price", "shares", "position_value",
    "stop_price", "risk_usd",
    "status", "note", "request_id"
]

POSITIONS_HEADER = [
    "symbol", "state",
    "entry_time", "entry_price",
    "shares", "position_value",
    "stop_price", "risk_usd",
    "last_event", "last_update",
    "trade_id", "notes"
]

PNL_HEADER = [
    "trade_id", "date", "symbol",
    "entry_time", "exit_time",
    "entry_price", "exit_price",
    "shares", "position_value",
    "gross_pnl", "pnl_per_share",
    "return_pct", "notes"
]

DAILY_HEADER = [
    "date", "trades", "gross_pnl",
    "wins", "losses", "win_rate",
    "avg_pnl", "avg_win", "avg_loss"
]

def init_sheets():
    if not SHEETS_ON:
        return
    ensure_tabs_exist([GOOGLE_SHEET_TAB, POSITIONS_TAB, PNL_TAB, DAILY_TAB, DASHBOARD_TAB])
    set_header_if_missing(GOOGLE_SHEET_TAB, RAW_HEADER)
    set_header_if_missing(POSITIONS_TAB, POSITIONS_HEADER)
    set_header_if_missing(PNL_TAB, PNL_HEADER)
    set_header_if_missing(DAILY_TAB, DAILY_HEADER)

def dash_write_layout():
    if not SHEETS_ON:
        return
    svc = sheets_service()
    rows = [
        ["Performance Dashboard"],
        [""],
        ["Metric", "Value"],
        ["All-time Net P&L", f"=IFERROR(SUM({PNL_TAB}!J:J),0)"],
        ["All-time Trades", f"=IFERROR(COUNTA({PNL_TAB}!A:A)-1,0)"],
        ["All-time Win Rate", f"=IFERROR(COUNTIF({PNL_TAB}!J:J,\">0\")/(COUNTA({PNL_TAB}!J:J)-1),0)"],
        ["All-time Avg Trade", f"=IFERROR(AVERAGE({PNL_TAB}!J:J),0)"],
        ["Today Net P&L", f"=IFERROR(SUMIF({PNL_TAB}!B:B, TEXT(TODAY(),\"yyyy-mm-dd\"), {PNL_TAB}!J:J),0)"],
        ["Today Trades", f"=IFERROR(COUNTIF({PNL_TAB}!B:B, TEXT(TODAY(),\"yyyy-mm-dd\")),0)"],
    ]
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{DASHBOARD_TAB}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows}
    ).execute()


# =============================================================================
# Positions tab helpers
# =============================================================================
def pos_get(symbol: str) -> Optional[Dict[str, Any]]:
    if not SHEETS_ON:
        return None
    header, rows = read_table(POSITIONS_TAB)
    if not header:
        return None
    idx = {name: i for i, name in enumerate(header)}
    for i, r in enumerate(rows):
        sym = (r[idx.get("symbol", 0)] if idx.get("symbol", 0) < len(r) else "").strip().upper()
        if sym == symbol.upper():
            out = {}
            for k, j in idx.items():
                out[k] = r[j] if j < len(r) else ""
            out["_row_index_1based"] = i + 2
            return out
    return None

def pos_set(symbol: str, state: str, entry_time: str, entry_price: float, shares: int,
            position_value: float, stop_price: float, risk_usd: float,
            last_event: str, trade_id: str, notes: str):
    if not SHEETS_ON:
        return
    existing = pos_get(symbol)
    row = [
        symbol.upper(),
        state,
        entry_time,
        "" if entry_price is None else entry_price,
        shares,
        position_value,
        stop_price,
        risk_usd,
        last_event,
        now_iso(),
        trade_id,
        notes,
    ]
    if existing and existing.get("_row_index_1based"):
        update_row(POSITIONS_TAB, int(existing["_row_index_1based"]), row)
    else:
        append_row(POSITIONS_TAB, row)

def pos_flat(symbol: str, last_event: str, notes: str):
    if not SHEETS_ON:
        return
    existing = pos_get(symbol)
    trade_id = (existing.get("trade_id") if existing else "") or ""
    row = [
        symbol.upper(),
        "FLAT",
        "", "", "", "", "", "",
        last_event,
        now_iso(),
        trade_id,
        notes,
    ]
    if existing and existing.get("_row_index_1based"):
        update_row(POSITIONS_TAB, int(existing["_row_index_1based"]), row)
    else:
        append_row(POSITIONS_TAB, row)


# =============================================================================
# PnL + Daily recompute
# =============================================================================
def append_pnl_row(trade_id: str, symbol: str, entry_time: str, exit_time: str,
                   entry_price: float, exit_price: float, shares: int, position_value: float,
                   notes: str):
    gross_pnl = round((exit_price - entry_price) * shares, 2)
    pnl_per_share = round((exit_price - entry_price), 4)
    ret_pct = round(((exit_price - entry_price) / entry_price) * 100.0, 4) if entry_price > 0 else 0.0
    date_str = datetime.now(timezone.utc).date().isoformat()

    append_row(PNL_TAB, [
        trade_id, date_str, symbol.upper(),
        entry_time, exit_time,
        entry_price, exit_price,
        shares, round(position_value, 2),
        gross_pnl, pnl_per_share,
        ret_pct, notes
    ])

def recompute_daily_from_pnl():
    if not SHEETS_ON:
        return
    header, rows = read_table(PNL_TAB)
    if not header:
        return
    idx = {name: i for i, name in enumerate(header)}
    by_date: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        if not r or len(r) < 3:
            continue
        date_str = r[idx.get("date", 1)] if idx.get("date", 1) < len(r) else ""
        pnl_str = r[idx.get("gross_pnl", 9)] if idx.get("gross_pnl", 9) < len(r) else "0"
        try:
            pnl = float(pnl_str)
        except:
            pnl = 0.0

        d = by_date.setdefault(date_str, {"trades": 0, "gross_pnl": 0.0, "wins": 0, "losses": 0, "sum_win": 0.0, "sum_loss": 0.0})
        d["trades"] += 1
        d["gross_pnl"] += pnl
        if pnl > 0:
            d["wins"] += 1
            d["sum_win"] += pnl
        elif pnl < 0:
            d["losses"] += 1
            d["sum_loss"] += pnl

    out = [DAILY_HEADER]
    for date_str in sorted(by_date.keys()):
        d = by_date[date_str]
        trades = d["trades"]
        gross = round(d["gross_pnl"], 2)
        wins = d["wins"]
        losses = d["losses"]
        win_rate = round((wins / trades) if trades else 0.0, 4)
        avg_pnl = round((gross / trades) if trades else 0.0, 2)
        avg_win = round((d["sum_win"] / wins) if wins else 0.0, 2)
        avg_loss = round((d["sum_loss"] / losses) if losses else 0.0, 2)
        out.append([date_str, trades, gross, wins, losses, win_rate, avg_pnl, avg_win, avg_loss])

    svc = sheets_service()
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{DAILY_TAB}!A1",
        valueInputOption="RAW",
        body={"values": out}
    ).execute()


# =============================================================================
# Boot Sheets (once)
# =============================================================================
if SHEETS_ON:
    try:
        init_sheets()
        dash_write_layout()
    except Exception as e:
        log.exception("Sheets init failed; disabling Sheets. Error=%s", str(e))
        SHEETS_ON = False


# =============================================================================
# Routes
# =============================================================================
@app.route("/", methods=["GET"])
def root():
    return "OK", 200

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/tv", methods=["POST", "GET"])
def tv():
    req_id = str(uuid.uuid4())[:8]

    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    raw_body = request.get_data(as_text=True)
    log.info("[%s] /tv raw body: %s", req_id, raw_body[:800])

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("[%s] JSON parse error", req_id)
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "long")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", 2.0) or 2.0)

    price = data.get("price", None)
    try:
        price = float(price) if price is not None else None
    except:
        price = None

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol"}), 400
    if side != "long":
        return jsonify({"ok": False, "error": "Only long side supported"}), 400

    # Map events
    if event in ("BUY", "ENTRY"):
        mapped = "ENTRY"
    elif event in ("SELL", "EXIT"):
        mapped = "EXIT"
    else:
        return jsonify({"ok": False, "error": "Unsupported event", "event": event}), 400

    # Cooldown check
    cd = cooldown_block(symbol)
    if cd:
        log.info("[%s] cooldown blocked: %s", req_id, cd)
        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            price if price is not None else "",
            "", "", "", "",
            "cooldown", cd, req_id
        ])
        return jsonify({"ok": True, "status": "cooldown", "reason": cd}), 200

    # ---- HARD STATE BLOCK (no multiple entry / exit) ----
    current = pos_get(symbol) if SHEETS_ON else None
    state = (current.get("state") if current else "FLAT") or "FLAT"

    if mapped == "ENTRY" and state == "LONG":
        log.info("[%s] ENTRY ignored: already LONG for %s", req_id, symbol)
        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            price if price is not None else "",
            "", "", "", "",
            "ignored", "Already in position (LONG)", req_id
        ])
        return jsonify({"ok": True, "ignored": True, "reason": "Already in position (LONG)"}), 200

    if mapped == "EXIT" and state != "LONG":
        log.info("[%s] EXIT ignored: no open position for %s (state=%s)", req_id, symbol, state)
        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            price if price is not None else "",
            "", "", "", "",
            "ignored", "No open position to exit", req_id
        ])
        return jsonify({"ok": True, "ignored": True, "reason": "No open position to exit"}), 200

    if price is None:
        note = "Missing price. Include price in TradingView webhook JSON (e.g. using {{close}})."
        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            "", "", "", "", "",
            "error", note, req_id
        ])
        return jsonify({"ok": False, "error": "missing_price", "detail": note}), 400

    shares, position_value, risk_usd, sizing_note = calc_shares(price, risk_stop_pct)
    stop_price = calc_stop_price(price, risk_stop_pct)

    cooldown_mark(symbol)

    # DRY_RUN flow
    if DRY_RUN:
        if mapped == "ENTRY":
            trade_id = f"{symbol}-{int(time.time())}"

            pos_set(
                symbol=symbol,
                state="LONG",
                entry_time=now_iso(),
                entry_price=price,
                shares=shares,
                position_value=position_value,
                stop_price=stop_price,
                risk_usd=risk_usd,
                last_event="ENTRY",
                trade_id=trade_id,
                notes=sizing_note
            )

            append_row(GOOGLE_SHEET_TAB, [
                now_iso(), symbol, event, mapped, side,
                price, shares, position_value,
                stop_price, risk_usd,
                "dry_run", sizing_note, req_id
            ])

            return jsonify({
                "ok": True, "dry_run": True, "mapped": "ENTRY", "symbol": symbol,
                "price": price, "shares": shares, "position_value": position_value,
                "stop_price": stop_price, "risk_usd": risk_usd, "note": sizing_note,
                "request_id": req_id
            }), 200

        # EXIT in DRY_RUN
        if not current:
            return jsonify({"ok": True, "ignored": True, "reason": "No position record"}), 200

        try:
            entry_price = float(current.get("entry_price") or 0)
            entry_time = current.get("entry_time") or ""
            entry_shares = int(float(current.get("shares") or 0))
            trade_id = current.get("trade_id") or f"{symbol}-{int(time.time())}"
        except:
            entry_price = 0.0
            entry_time = ""
            entry_shares = 0
            trade_id = current.get("trade_id") or f"{symbol}-{int(time.time())}"

        gross_pnl = round((price - entry_price) * entry_shares, 2) if entry_shares else 0.0
        notes = f"dry_run_exit_pnl={gross_pnl}"

        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            price, entry_shares, round(entry_shares * price, 2),
            "", "",
            "dry_run_exit", notes, req_id
        ])

        append_pnl_row(
            trade_id=trade_id,
            symbol=symbol,
            entry_time=entry_time,
            exit_time=now_iso(),
            entry_price=entry_price,
            exit_price=price,
            shares=entry_shares,
            position_value=entry_shares * entry_price,
            notes="dry_run"
        )

        pos_flat(symbol, last_event="EXIT", notes="closed in dry_run")
        recompute_daily_from_pnl()
        dash_write_layout()

        return jsonify({
            "ok": True, "dry_run": True, "mapped": "EXIT", "symbol": symbol,
            "entry_price": entry_price, "exit_price": price, "shares": entry_shares,
            "gross_pnl": gross_pnl, "request_id": req_id
        }), 200

    # LIVE mode
    try:
        if mapped == "ENTRY":
            broker = qt_place_market_order(symbol, "BUY", shares)

            trade_id = f"{symbol}-{int(time.time())}"
            pos_set(
                symbol=symbol,
                state="LONG",
                entry_time=now_iso(),
                entry_price=price,
                shares=shares,
                position_value=position_value,
                stop_price=stop_price,
                risk_usd=risk_usd,
                last_event="ENTRY",
                trade_id=trade_id,
                notes="LIVE: " + sizing_note
            )

            append_row(GOOGLE_SHEET_TAB, [
                now_iso(), symbol, event, mapped, side,
                price, shares, position_value,
                stop_price, risk_usd,
                "live_entry", "Order sent", req_id
            ])

            return jsonify({"ok": True, "live": True, "mapped": "ENTRY", "broker": broker}), 200

        # EXIT => SELL all shares recorded in Positions
        current = pos_get(symbol) if SHEETS_ON else None
        entry_shares = shares
        entry_price = None
        entry_time = ""
        trade_id = f"{symbol}-{int(time.time())}"

        if current:
            try:
                entry_shares = int(float(current.get("shares") or shares))
                entry_price = float(current.get("entry_price") or 0)
                entry_time = current.get("entry_time") or ""
                trade_id = current.get("trade_id") or trade_id
            except:
                pass

        broker = qt_place_market_order(symbol, "SELL", entry_shares)

        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            price, entry_shares, round(entry_shares * price, 2),
            "", "",
            "live_exit", "Order sent", req_id
        ])

        if entry_price is not None and entry_shares:
            append_pnl_row(
                trade_id=trade_id,
                symbol=symbol,
                entry_time=entry_time,
                exit_time=now_iso(),
                entry_price=float(entry_price),
                exit_price=float(price),
                shares=int(entry_shares),
                position_value=float(entry_price) * int(entry_shares),
                notes="live"
            )
            recompute_daily_from_pnl()
            dash_write_layout()

        pos_flat(symbol, last_event="EXIT", notes="closed live")

        return jsonify({"ok": True, "live": True, "mapped": "EXIT", "broker": broker}), 200

    except Exception as e:
        log.exception("[%s] live order failed", req_id)
        append_row(GOOGLE_SHEET_TAB, [
            now_iso(), symbol, event, mapped, side,
            price if price is not None else "",
            shares, position_value,
            stop_price, risk_usd,
            "error", str(e)[:200], req_id
        ])
        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# =============================================================================
# Local dev entry point (Render uses gunicorn)
# =============================================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
