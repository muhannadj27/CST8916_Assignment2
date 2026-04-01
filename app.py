# AI Assistance Disclosure: This file was written with the assistance of Claude (Anthropic).
# The author understands and can explain all code in this file.
#
# CST8916 – Assignment 2: Real-time Stream Analytics Pipeline
#
# Changes from Week 10 lab:
#   - /track now enriches events with deviceType, browser, and os parsed from User-Agent
#   - New background thread reads analytics output from a second Event Hub (analytics-output)
#   - New /api/analytics endpoint serves aggregated Stream Analytics results to the dashboard
#
# Routes:
#   GET  /              → serves the demo store (client.html)
#   POST /track         → enriches + publishes click event to Event Hubs
#   GET  /dashboard     → serves the analytics dashboard (dashboard.html)
#   GET  /api/events    → returns recent raw events as JSON
#   GET  /api/analytics → returns latest Stream Analytics results as JSON
#   GET  /health        → health check for Azure App Service

import os
import json
import re
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – set these as environment variables / Azure App Settings
#
#   EVENT_HUB_CONNECTION_STR   – connection string for the clickstream Event Hub namespace
#   EVENT_HUB_NAME             – name of the input hub  (default: "clickstream")
#   ANALYTICS_HUB_NAME         – name of the output hub (default: "analytics-output")
#                                Stream Analytics writes its results here
# ---------------------------------------------------------------------------
CONNECTION_STR      = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME      = os.environ.get("EVENT_HUB_NAME", "clickstream")
ANALYTICS_HUB_NAME  = os.environ.get("ANALYTICS_HUB_NAME", "analytics-output")

# In-memory buffers
_event_buffer     = []   # raw click events (max 50)
_analytics_buffer = {    # latest Stream Analytics results
    "device_breakdown": [],   # [{deviceType, event_count, window_end}, ...]
    "spikes":           [],   # [{window_start, window_end, event_count, is_spike}, ...]
}
_buffer_lock    = threading.Lock()
_analytics_lock = threading.Lock()
MAX_BUFFER = 50


# ---------------------------------------------------------------------------
# Part 1 – User-Agent parser
# Detects deviceType, browser, and os from the HTTP User-Agent header.
# We use a lightweight regex approach so we have zero extra dependencies.
# ---------------------------------------------------------------------------

def parse_user_agent(ua: str) -> dict:
    """
    Parse a User-Agent string and return deviceType, browser, and os.

    deviceType: "mobile" | "tablet" | "desktop"
    browser:    "Chrome" | "Firefox" | "Safari" | "Edge" | "Opera" | "Unknown"
    os:         "Android" | "iOS" | "Windows" | "macOS" | "Linux" | "Unknown"
    """
    ua = ua or ""

    # --- deviceType ---
    if re.search(r"tablet|ipad|playbook|silk", ua, re.I):
        device_type = "tablet"
    elif re.search(r"mobile|iphone|ipod|android.*mobile|blackberry|windows phone", ua, re.I):
        device_type = "mobile"
    else:
        device_type = "desktop"

    # --- browser (order matters: Edge and Opera contain "Chrome") ---
    if re.search(r"Edg/", ua):
        browser = "Edge"
    elif re.search(r"OPR/|Opera", ua):
        browser = "Opera"
    elif re.search(r"Firefox/", ua):
        browser = "Firefox"
    elif re.search(r"Chrome/", ua):
        browser = "Chrome"
    elif re.search(r"Safari/", ua) and "Chrome" not in ua:
        browser = "Safari"
    else:
        browser = "Unknown"

    # --- os ---
    if re.search(r"Android", ua):
        os_name = "Android"
    elif re.search(r"iPhone|iPad|iPod", ua):
        os_name = "iOS"
    elif re.search(r"Windows NT", ua):
        os_name = "Windows"
    elif re.search(r"Mac OS X", ua):
        os_name = "macOS"
    elif re.search(r"Linux", ua):
        os_name = "Linux"
    else:
        os_name = "Unknown"

    return {"deviceType": device_type, "browser": browser, "os": os_name}


# ---------------------------------------------------------------------------
# Helper – publish one event dict to Azure Event Hubs
# ---------------------------------------------------------------------------

def send_to_event_hubs(event_dict: dict):
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR not set – skipping publish")
        return
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    with producer:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(batch)


# ---------------------------------------------------------------------------
# Background thread 1 – raw event consumer (same as lab, unchanged)
# ---------------------------------------------------------------------------

def _on_raw_event(partition_context, event):
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}
    with _buffer_lock:
        _event_buffer.append(data)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)
    partition_context.update_checkpoint(event)


def start_raw_consumer():
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR not set – raw consumer not started")
        return
    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
    )
    def run():
        with consumer:
            consumer.receive(on_event=_on_raw_event, starting_position="-1")
    threading.Thread(target=run, daemon=True).start()
    app.logger.info("Raw event consumer thread started")


# ---------------------------------------------------------------------------
# Background thread 2 – analytics output consumer
#
# Stream Analytics writes its query results to the "analytics-output" Event Hub.
# Each message is a JSON object that matches one of two shapes:
#
#   Device breakdown (Q1):
#   { "query": "device_breakdown", "deviceType": "mobile",
#     "event_count": 42, "window_end": "2026-03-30T12:05:00Z" }
#
#   Spike detection (Q2):
#   { "query": "spike_detection", "window_start": "...", "window_end": "...",
#     "event_count": 87, "is_spike": true }
#
# We keep the latest window of each type in _analytics_buffer so the dashboard
# can fetch them via GET /api/analytics.
# ---------------------------------------------------------------------------

def _on_analytics_event(partition_context, event):
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        app.logger.warning("Non-JSON analytics event: %s", body)
        partition_context.update_checkpoint(event)
        return

    query_type = data.get("query", "")

    with _analytics_lock:
        if query_type == "device_breakdown":
            # Replace the row for this deviceType with the latest window value
            existing = _analytics_buffer["device_breakdown"]
            updated = [r for r in existing if r.get("deviceType") != data.get("deviceType")]
            updated.append(data)
            _analytics_buffer["device_breakdown"] = updated

        elif query_type == "spike_detection":
            # Keep only the last 20 spike windows
            _analytics_buffer["spikes"].append(data)
            if len(_analytics_buffer["spikes"]) > 20:
                _analytics_buffer["spikes"].pop(0)

    partition_context.update_checkpoint(event)


def start_analytics_consumer():
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR not set – analytics consumer not started")
        return
    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=ANALYTICS_HUB_NAME,
    )
    def run():
        with consumer:
            consumer.receive(on_event=_on_analytics_event, starting_position="-1")
    threading.Thread(target=run, daemon=True).start()
    app.logger.info("Analytics consumer thread started")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event, enrich it with device/browser/os, publish to Event Hubs.

    Expected JSON body:
    {
        "event_type": "page_view" | "product_click" | "add_to_cart" | "purchase",
        "page":       "/products/shoes",
        "product_id": "p_shoe_42",
        "user_id":    "u_1234"
    }
    """
    if not request.json:
        abort(400)

    # Parse User-Agent for Part 1 enrichment
    ua_string = request.headers.get("User-Agent", "")
    ua_info   = parse_user_agent(ua_string)

    event = {
        # Original fields
        "event_type": request.json.get("event_type", "unknown"),
        "page":       request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id":    request.json.get("user_id", "anonymous"),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        # Part 1 – enriched fields
        "deviceType": ua_info["deviceType"],
        "browser":    ua_info["browser"],
        "os":         ua_info["os"],
    }

    send_to_event_hubs(event)

    with _buffer_lock:
        _event_buffer.append(event)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    return jsonify({"status": "ok", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """Return buffered raw events + summary. Polled by the dashboard every 2s."""
    try:
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    summary = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        summary[et] = summary.get(et, 0) + 1

    return jsonify({"events": recent, "summary": summary, "total": len(recent)}), 200


@app.route("/api/analytics", methods=["GET"])
def get_analytics():
    """
    Return the latest Stream Analytics results.
    Polled by the dashboard every 5 seconds.

    Response shape:
    {
      "device_breakdown": [
        {"deviceType": "desktop", "event_count": 42, "window_end": "..."},
        {"deviceType": "mobile",  "event_count": 18, "window_end": "..."},
        {"deviceType": "tablet",  "event_count":  5, "window_end": "..."}
      ],
      "spikes": [
        {"window_start": "...", "window_end": "...", "event_count": 87, "is_spike": true},
        ...
      ]
    }
    """
    with _analytics_lock:
        data = {
            "device_breakdown": list(_analytics_buffer["device_breakdown"]),
            "spikes":           list(_analytics_buffer["spikes"]),
        }
    return jsonify(data), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    start_raw_consumer()
    start_analytics_consumer()
    app.run(debug=False, host="0.0.0.0", port=8000)
