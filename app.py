import os
from flask import Flask, request, jsonify

app = Flask(__name__)

# ---------------------------------------------------------
# Simple health check
# ---------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    app.logger.info("HEALTH CHECK HIT")
    return "OK", 200

# ---------------------------------------------------------
# TV webhook echo endpoint (no Questrade yet)
# ---------------------------------------------------------
@app.route("/tv", methods=["GET", "POST"])
def tv():
    # Log EVERYTHING that hits this route
    raw_body = request.get_data(as_text=True)
    app.logger.info("TV HIT: method=%s path=%s body=%s", request.method, request.path, raw_body)

    # If someone opens it in a browser (GET), just tell them to use POST
    if request.method == "GET":
        return "Use POST with JSON body", 405

    # Try to parse JSON
    data = request.get_json(silent=True) or {}
    app.logger.info("TV PARSED JSON: %s", data)

    symbol = str(data.get("symbol", "")).upper()
    event = str(data.get("event", "")).upper()
    side = str(data.get("side", "")).lower()
    risk_stop_pct = data.get("risk_stop_pct")

    if not symbol or not event:
        return jsonify({
            "ok": False,
            "error": "Missing symbol or event",
            "received": data
        }), 400

    # Just echo back what we got
    return jsonify({
        "ok": True,
        "msg": "Webhook received",
        "symbol": symbol,
        "event": event,
        "side": side,
        "risk_stop_pct": risk_stop_pct,
        "raw": data
    }), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
