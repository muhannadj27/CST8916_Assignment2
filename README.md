# CST8916 – Assignment 2: Real-time Stream Analytics Pipeline

> **AI Assistance Disclosure:** This project was developed with the assistance of Claude (Anthropic).
> All code has been reviewed and understood by the author.

## Demo Video

[Watch the demo on YouTube](https://youtu.be/t-ED5jEDGZY)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Azure App Service                               │
│                                                                          │
│   Browser                                                                │
│   ┌──────────┐  POST /track   ┌──────────────────────────────────────┐  │
│   │  Demo    │ ─────────────► │           app.py (Flask)             │  │
│   │  Store   │                │                                      │  │
│   │client.html│               │  parse_user_agent(User-Agent header) │  │
│   └──────────┘                │  → adds deviceType, browser, os      │  │
│                               └─────────────┬────────────────────────┘  │
│                                             │ send_to_event_hubs()       │
└─────────────────────────────────────────────┼────────────────────────────┘
                                              │
                    ┌─────────────────────────▼──────────────────────────┐
                    │              Azure Event Hubs Namespace             │
                    │                                                     │
                    │   Hub: "clickstream"  (raw enriched events)        │
                    │   Hub: "analytics-output" (ASA results)            │
                    └────────────┬──────────────────────┬────────────────┘
                                 │ input                │ output
                    ┌────────────▼──────────────────────▼────────────────┐
                    │           Azure Stream Analytics Job                │
                    │                                                     │
                    │  Q1 – Device Breakdown  (TumblingWindow 1 min)     │
                    │       GROUP BY deviceType → event_count             │
                    │                                                     │
                    │  Q2 – Spike Detection   (TumblingWindow 1 min)     │
                    │       COUNT(*) > 10 threshold → is_spike flag       │
                    └─────────────────────────────────────────────────────┘
                                              │
                    ┌─────────────────────────▼──────────────────────────┐
                    │                  app.py (Flask)                     │
                    │                                                     │
                    │  start_analytics_consumer() reads analytics-output  │
                    │  stores results in _analytics_buffer (in-memory)   │
                    │  GET /api/analytics serves buffer as JSON           │
                    └─────────────────────────┬──────────────────────────┘
                                              │ poll every 5s
                    ┌─────────────────────────▼──────────────────────────┐
                    │           dashboard.html (browser)                  │
                    │                                                     │
                    │  • Bar chart: device type breakdown (Chart.js)     │
                    │  • Spike timeline: per-minute event counts          │
                    │  • Raw event table with deviceType, browser, os    │
                    └─────────────────────────────────────────────────────┘
```

---

## Design Decisions

### Part 1 – Event Payload Enrichment

**Approach:** Server-side User-Agent parsing in `app.py` using Python's built-in `re` module.

When the browser sends a `POST /track` request, the server reads the `User-Agent` HTTP header and runs regex patterns to extract:
- `deviceType` — `"desktop"`, `"mobile"`, or `"tablet"`
- `browser` — `"Chrome"`, `"Firefox"`, `"Safari"`, `"Edge"`, `"Opera"`, or `"Unknown"`
- `os` — `"Windows"`, `"macOS"`, `"Linux"`, `"Android"`, `"iOS"`, or `"Unknown"`

**Why server-side?** The browser cannot reliably report its own device type. Doing it on the server keeps the client code simple and ensures every event has these fields regardless of which page generated it.

**Why no external library?** A simple regex approach avoids adding a dependency (`ua-parser2` etc.) and is sufficient for the three fields required by the assignment.

### Part 2 – Connecting Stream Analytics to the Dashboard

**Approach:** Stream Analytics → second Event Hub (`analytics-output`) → Flask background thread → in-memory buffer → `/api/analytics` HTTP endpoint → dashboard polls every 5 seconds.

**Why a second Event Hub?** Stream Analytics supports Event Hub as a native output sink with no extra configuration. The alternative (Azure SQL or Blob Storage) would require additional Azure resources, extra SDK dependencies, and more complex polling logic.

**Why poll instead of WebSocket push?** The existing lab dashboard already uses HTTP polling for raw events (`/api/events`). Reusing the same pattern for analytics keeps the architecture consistent and simple to understand and debug.

**Why in-memory buffer?** For a course assignment with a short demo window, an in-memory dictionary is sufficient. In a production system this would be replaced with a database query.

### Stream Analytics Query Design

**Window type:** Tumbling window (non-overlapping, fixed-size). Chosen because:
- Device breakdown needs a clean "count per minute" with no double-counting
- Spike detection needs a discrete yes/no per window — overlapping windows would produce noisy results

**Window size:** 1 minute — short enough to see changes quickly during a live demo, large enough to accumulate meaningful counts.

**Spike threshold:** 10 events/minute — chosen to be easily triggerable during a demo by clicking around the store.

---

## Azure Resources Required

| Resource | Purpose |
|---|---|
| Azure Event Hubs Namespace | Hosts both event hubs |
| Event Hub: `clickstream` | Receives enriched click events from the store |
| Event Hub: `analytics-output` | Receives Stream Analytics query results |
| Azure Stream Analytics Job | Runs Q1 and Q2 queries |
| Azure App Service (Linux, Python 3.11) | Hosts the Flask app |

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/muhannadj27/CST8916_Assignment2.git
cd CST8916_Assignment2
```

### 2. Create Azure Event Hubs

1. In the Azure Portal, create an **Event Hubs Namespace**
2. Inside it, create two Event Hubs:
   - `clickstream` (for raw events)
   - `analytics-output` (for Stream Analytics output)
3. Go to **Shared access policies → RootManageSharedAccessKey** and copy the connection string

### 3. Create Azure Stream Analytics Job

1. Create a new Stream Analytics Job in Azure Portal
2. Add input: Event Hub `clickstream` → alias `clickstream-input`
3. Add output: Event Hub `analytics-output` → alias `analytics-output`
4. Paste the queries from `stream-analytics/queries.saql` into the query editor
5. Start the job

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in your connection string
```

For Azure App Service, set these under **Settings → Configuration → Application settings**:
- `EVENT_HUB_CONNECTION_STR` — your namespace connection string
- `EVENT_HUB_NAME` — `clickstream`
- `ANALYTICS_HUB_NAME` — `analytics-output`

### 5. Run locally

```bash
pip install -r requirements.txt
python app.py
```

Visit `http://localhost:8000` for the store and `http://localhost:8000/dashboard` for the dashboard.

### 6. Deploy to Azure App Service

The repo includes a GitHub Actions workflow (`.github/workflows/`) for automatic deployment. Push to `main` to trigger a deploy.

Alternatively, deploy via Azure CLI:
```bash
az webapp up --name YOUR_APP_NAME --resource-group YOUR_RG --runtime "PYTHON:3.11"
```

---

## File Structure

```
CST8916_Assignment2/
├── app.py                        # Flask app (producer + consumer + analytics)
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variable template
├── templates/
│   ├── client.html               # Demo store (unchanged from lab)
│   └── dashboard.html            # Analytics dashboard (upgraded)
└── stream-analytics/
    └── queries.saql              # Stream Analytics SAQL queries (Q1 + Q2)
```
