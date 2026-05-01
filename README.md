# Data Enrichment Pipeline

This repository contains my implementation of the Senior Python Developer take-home task.

The application accepts a URL, runs a three-step enrichment pipeline in the background, and streams results back to the browser with Server-Sent Events.

## Task Summary

Build a FastAPI service that:

- accepts a URL from a browser
- runs `scrape -> parse -> score` sequentially
- returns from `POST /enrich` immediately
- streams incremental results as each step completes
- emits a terminal `done` event every time
- keeps the job running even if the client disconnects

## What I Implemented

- `GET /` serves a plain HTML page with a URL input, submit button, and live event log
- `POST /enrich` validates input, creates a `job_id`, queues the work, and returns `202 Accepted`
- `GET /stream/{job_id}` streams SSE events in real time
- `GET /jobs/{job_id}` returns the aggregate job state so results remain retrievable after disconnects
- the pipeline runs in background worker tasks using `asyncio.Queue`
- events are replayed for late subscribers from in-memory job history
- step failures emit structured error events and still end with `done`

## Stack

- Python 3.11+
- FastAPI
- Pydantic v2
- `asyncio.Queue` for in-process job dispatch
- per-job subscriber queues for in-memory pub/sub
- `httpx` for HTTP fetching
- `beautifulsoup4` for HTML parsing
- `langdetect` for simple language detection
- `uvicorn` as the ASGI server

## Why In-Memory Queue + Pub/Sub

I chose an in-process queue and pub/sub design for this take-home because it keeps the implementation small, easy to review, and fast to run locally.

Benefits:

- no external infrastructure is required
- background execution is still explicit and non-blocking
- SSE replay for reconnecting clients is straightforward
- the full solution stays readable in a single file

Trade-offs:

- job state is lost on process restart
- this is intended for a single-process deployment
- horizontal scaling would require shared state and an external broker

For a production version, I would move job dispatch and event fan-out to Redis and store job results in durable persistence.

## Project Files

- `main.py` - FastAPI app, models, pipeline logic, in-memory queue/pub-sub, and HTML client
- `requirements.txt` - Python dependencies
- `README.md` - setup, architecture, and trade-offs

## How To Run

```bash
python3.11 -m pip install --user -r requirements.txt
python3.11 -m uvicorn main:app --reload
```

If `uvicorn` is already on your `PATH`, this also works:

```bash
uvicorn main:app --reload
```

Then open:

- `http://127.0.0.1:8000/`

## API

### Endpoints Overview

- `GET /` - opens the minimal browser UI
- `POST /enrich` - creates a new enrichment job and returns a `job_id`
- `GET /stream/{job_id}` - streams live SSE events for that job
- `GET /jobs/{job_id}` - returns the stored aggregate job result

### `GET /`

Serves a minimal HTML page with no frontend framework or build step.

Example:

```bash
curl http://127.0.0.1:8000/
```

### `POST /enrich`

Creates a job and returns immediately.

Example:

```bash
curl -X POST http://127.0.0.1:8000/enrich \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com"}'
```

Request:

```json
{
  "url": "https://example.com"
}
```

Response:

```json
{
  "job_id": "8f5f4f9e-0ee1-4b2f-b1ed-1fbb2f9c2b69",
  "status": "queued"
}
```

### `GET /stream/{job_id}`

Streams Server-Sent Events for the job.

Example:

```bash
curl -N http://127.0.0.1:8000/stream/<job_id>
```

SSE event names:

- `step`
- `error`
- `done`

Event envelope:

```json
{
  "job_id": "8f5f4f9e-0ee1-4b2f-b1ed-1fbb2f9c2b69",
  "sequence": 1,
  "type": "step",
  "step": "scrape",
  "created_at": "2026-05-01T12:00:00Z",
  "data": {
    "step": "scrape",
    "final_url": "https://example.com/",
    "http_status": 200,
    "page_title": "Example Domain",
    "text_content": "Example Domain ..."
  }
}
```

Error example:

```json
{
  "job_id": "8f5f4f9e-0ee1-4b2f-b1ed-1fbb2f9c2b69",
  "sequence": 2,
  "type": "error",
  "step": "parse",
  "created_at": "2026-05-01T12:00:01Z",
  "data": {
    "step": "parse",
    "error_type": "ValueError",
    "message": "Parse step skipped because scrape did not complete successfully."
  }
}
```

### `GET /jobs/{job_id}`

Returns the aggregate job status, including stored step results, errors, and event history.

Example:

```bash
curl http://127.0.0.1:8000/jobs/<job_id>
```

## Pipeline Behavior

The three steps run sequentially inside a background worker:

1. `scrape`
2. `parse`
3. `score`

Each successful step publishes a `step` event immediately.

### Scrape

Fetches the submitted URL and extracts:

- final URL
- HTTP status
- page title
- normalized plain text content

### Parse

Extracts:

- word count
- language
- meta description
- outbound links capped at 10

### Score

Produces:

- a `0-100` quality score
- a one-line rationale based on simple content heuristics

## Typed Models

The core flow uses typed Pydantic models for:

- `EnrichRequest`
- `JobCreated`
- `ScrapeResult`
- `ParseResult`
- `ScoreResult`
- `StepError`
- `StreamEvent`
- `JobStatus`

No raw dictionaries are used in the request handling, pipeline execution, or SSE event construction.

## Error Handling

The app treats step failures as part of the job state instead of process-level failures.

Behavior:

- a failing step emits a structured `error` event
- the application process stays alive
- later steps still run if their prerequisites are available
- skipped dependent steps emit their own structured errors
- a terminal `done` event is always emitted

## Concurrency And Streaming Notes

- `POST /enrich` returns immediately after queueing the job
- multiple worker tasks consume the queue, so multiple jobs can run concurrently
- SSE subscribers receive live events as they are published
- late subscribers receive replayed events from stored job history
- disconnected subscribers are cleaned up without stopping the underlying job

## Manual Verification

I performed a basic local verification pass:

- started the app with `uvicorn`
- loaded `GET /`
- submitted a job through `POST /enrich`
- confirmed the browser opened the SSE stream endpoint
- confirmed the app returned `202 Accepted` and served the stream endpoint successfully

## Time-Pressure Trade-Offs

To keep the solution focused for the take-home:

- I kept everything in `main.py` instead of splitting into multiple modules
- job persistence is in memory only
- scoring uses lightweight heuristics
- there is no retry layer, database, or authentication

## Future Improvements

- split the code into `models.py`, `pipeline.py`, and `main.py`
- move queueing/pub-sub to Redis
- persist job state in a database
- add retries and better fetch classification for transient failures
- add automated tests for SSE behavior and step failure paths
