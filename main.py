import asyncio
import contextlib
import os
import re
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from enum import Enum
from typing import Literal
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, StreamingResponse
from langdetect import LangDetectException, detect
from pydantic import BaseModel, Field, HttpUrl


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"


class EnrichRequest(BaseModel):
    url: HttpUrl


class JobCreated(BaseModel):
    job_id: str
    status: JobState


class ScrapeResult(BaseModel):
    step: Literal["scrape"] = "scrape"
    final_url: str
    http_status: int
    page_title: str | None
    text_content: str
    raw_html: str = Field(default="", exclude=True, repr=False)


class ParseResult(BaseModel):
    step: Literal["parse"] = "parse"
    word_count: int
    language: str | None
    meta_description: str | None
    outbound_links: list[str] = Field(default_factory=list)


class ScoreResult(BaseModel):
    step: Literal["score"] = "score"
    quality_score: int = Field(ge=0, le=100)
    rationale: str


class StepError(BaseModel):
    step: Literal["scrape", "parse", "score"]
    error_type: str
    message: str


class DonePayload(BaseModel):
    status: JobState


EventPayload = ScrapeResult | ParseResult | ScoreResult | StepError | DonePayload


class StreamEvent(BaseModel):
    job_id: str
    sequence: int
    type: Literal["step", "error", "done"]
    step: Literal["scrape", "parse", "score"] | None = None
    created_at: datetime
    data: EventPayload


class JobStatus(BaseModel):
    job_id: str
    url: str
    status: JobState
    created_at: datetime
    updated_at: datetime
    scrape: ScrapeResult | None = None
    parse: ParseResult | None = None
    score: ScoreResult | None = None
    errors: list[StepError] = Field(default_factory=list)
    events: list[StreamEvent] = Field(default_factory=list)


async def job_worker(app: FastAPI) -> None:
    queue: asyncio.Queue[str] = app.state.job_queue
    while True:
        job_id = await queue.get()
        try:
            await process_job(app, job_id)
        finally:
            queue.task_done()


async def process_job(app: FastAPI, job_id: str) -> None:
    jobs: dict[str, JobStatus] = app.state.jobs
    job = jobs.get(job_id)
    if job is None:
        return

    job.status = JobState.RUNNING
    job.updated_at = utc_now()

    scrape_result: ScrapeResult | None = None
    parse_result: ParseResult | None = None
    encountered_error = False

    try:
        try:
            scrape_result = await scrape_step(job.url)
            await publish_event(app, job_id, "step", "scrape", scrape_result)
        except Exception as exc:  # pragma: no cover - defensive error boundary
            encountered_error = True
            await publish_step_error(app, job_id, "scrape", exc)

        try:
            if scrape_result is None:
                raise ValueError("Parse step skipped because scrape did not complete successfully.")
            parse_result = parse_step(scrape_result)
            await publish_event(app, job_id, "step", "parse", parse_result)
        except Exception as exc:  # pragma: no cover - defensive error boundary
            encountered_error = True
            await publish_step_error(app, job_id, "parse", exc)

        try:
            if scrape_result is None or parse_result is None:
                raise ValueError("Score step skipped because earlier pipeline steps did not complete successfully.")
            score_result = score_step(scrape_result, parse_result)
            await publish_event(app, job_id, "step", "score", score_result)
        except Exception as exc:  # pragma: no cover - defensive error boundary
            encountered_error = True
            await publish_step_error(app, job_id, "score", exc)
    finally:
        final_status = (
            JobState.COMPLETED_WITH_ERRORS if encountered_error else JobState.COMPLETED
        )
        job.status = final_status
        job.updated_at = utc_now()
        await publish_event(app, job_id, "done", None, DonePayload(status=final_status))


async def scrape_step(url: str) -> ScrapeResult:
    timeout = httpx.Timeout(15.0, connect=10.0)
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        response = await client.get(url, headers={"User-Agent": "DataEnrichmentPipeline/1.0"})

    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    text_content = normalize_text(soup.get_text(separator=" ", strip=True))

    return ScrapeResult(
        final_url=str(response.url),
        http_status=response.status_code,
        page_title=title,
        text_content=text_content,
        raw_html=response.text,
    )


def parse_step(scrape: ScrapeResult) -> ParseResult:
    soup = BeautifulSoup(scrape.raw_html, "html.parser")
    text_content = normalize_text(scrape.text_content)
    words = re.findall(r"\b\w+\b", text_content)
    language = detect_language(text_content)
    meta_tag = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    meta_description = (
        normalize_text(meta_tag.get("content", "")) if meta_tag and meta_tag.get("content") else None
    )
    outbound_links = extract_outbound_links(scrape.final_url, soup)

    return ParseResult(
        word_count=len(words),
        language=language,
        meta_description=meta_description,
        outbound_links=outbound_links,
    )


def score_step(scrape: ScrapeResult, parsed: ParseResult) -> ScoreResult:
    score = 0
    notes: list[str] = []

    if 200 <= scrape.http_status < 400:
        score += 20
        notes.append("page responded successfully")
    if scrape.page_title:
        score += 20
        notes.append("title is present")
    if parsed.meta_description:
        score += 15
        notes.append("meta description is present")
    if parsed.word_count >= 300:
        score += 25
        notes.append("content is substantial")
    elif parsed.word_count >= 100:
        score += 15
        notes.append("content has moderate depth")
    if parsed.language:
        score += 10
        notes.append(f"language detected as {parsed.language}")
    if parsed.outbound_links:
        score += 10
        notes.append("page includes outbound references")

    score = max(0, min(score, 100))
    rationale = "; ".join(notes[:3]) if notes else "Limited content signals were available."

    return ScoreResult(quality_score=score, rationale=rationale)


def detect_language(text: str) -> str | None:
    sample = text[:5000].strip()
    if len(sample) < 20:
        return None
    try:
        return detect(sample)
    except LangDetectException:
        return None


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def extract_outbound_links(base_url: str, soup: BeautifulSoup) -> list[str]:
    base_host = urlparse(base_url).netloc.lower()
    links: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not parsed.netloc or parsed.netloc.lower() == base_host:
            continue
        if absolute in seen:
            continue

        seen.add(absolute)
        links.append(absolute)
        if len(links) == 10:
            break

    return links


async def publish_step_error(
    app: FastAPI, job_id: str, step: Literal["scrape", "parse", "score"], exc: Exception
) -> None:
    error = StepError(step=step, error_type=type(exc).__name__, message=str(exc))
    await publish_event(app, job_id, "error", step, error)


async def publish_event(
    app: FastAPI,
    job_id: str,
    event_type: Literal["step", "error", "done"],
    step: Literal["scrape", "parse", "score"] | None,
    data: EventPayload,
) -> None:
    jobs: dict[str, JobStatus] = app.state.jobs
    subscribers: dict[str, list[asyncio.Queue[StreamEvent]]] = app.state.subscribers
    job = jobs[job_id]

    event = StreamEvent(
        job_id=job_id,
        sequence=len(job.events) + 1,
        type=event_type,
        step=step,
        created_at=utc_now(),
        data=data,
    )

    job.events.append(event)
    job.updated_at = event.created_at

    if event.type == "step" and event.step == "scrape" and isinstance(data, ScrapeResult):
        job.scrape = data
    elif event.type == "step" and event.step == "parse" and isinstance(data, ParseResult):
        job.parse = data
    elif event.type == "step" and event.step == "score" and isinstance(data, ScoreResult):
        job.score = data
    elif event.type == "error" and isinstance(data, StepError):
        job.errors.append(data)

    for subscriber in list(subscribers.get(job_id, [])):
        await subscriber.put(event)


def sse_encode(event: StreamEvent) -> str:
    return f"event: {event.type}\ndata: {event.model_dump_json()}\n\n"


async def stream_job_events(request: Request, app: FastAPI, job_id: str) -> AsyncIterator[str]:
    subscribers: dict[str, list[asyncio.Queue[StreamEvent]]] = app.state.subscribers

    subscriber: asyncio.Queue[StreamEvent] = asyncio.Queue()
    subscribers.setdefault(job_id, []).append(subscriber)
    last_sequence = 0

    try:
        for event in list(app.state.jobs[job_id].events):
            yield sse_encode(event)
            last_sequence = event.sequence
            if event.type == "done":
                return

        while True:
            if await request.is_disconnected():
                return
            try:
                event = await asyncio.wait_for(subscriber.get(), timeout=1.0)
            except TimeoutError:
                continue
            if event.sequence <= last_sequence:
                continue
            last_sequence = event.sequence
            yield sse_encode(event)
            if event.type == "done":
                return
    finally:
        with contextlib.suppress(ValueError):
            subscribers.get(job_id, []).remove(subscriber)


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Data Enrichment Pipeline</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      margin: 2rem auto;
      max-width: 900px;
      padding: 0 1rem;
      line-height: 1.5;
    }
    form {
      display: flex;
      gap: 0.75rem;
      margin-bottom: 1rem;
    }
    input[type="url"] {
      flex: 1;
      padding: 0.75rem;
    }
    button {
      padding: 0.75rem 1rem;
      cursor: pointer;
    }
    .meta {
      margin: 1rem 0;
      color: #444;
    }
    .event {
      border: 1px solid #ddd;
      border-radius: 8px;
      padding: 0.75rem;
      margin-bottom: 0.75rem;
      background: #fafafa;
    }
    .event h3 {
      margin: 0 0 0.5rem;
      text-transform: capitalize;
    }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      margin: 0;
    }
  </style>
</head>
<body>
  <h1>Data Enrichment Pipeline</h1>
  <p>Submit a URL to run the scrape, parse, and score workflow.</p>

  <form id="enrich-form">
    <input id="url-input" name="url" type="url" placeholder="https://example.com" required>
    <button type="submit">Start</button>
  </form>

  <div class="meta" id="meta"></div>
  <div id="events"></div>

  <script>
    const form = document.getElementById("enrich-form");
    const urlInput = document.getElementById("url-input");
    const meta = document.getElementById("meta");
    const eventsContainer = document.getElementById("events");
    let eventSource = null;

    function renderEvent(name, payload) {
      const wrapper = document.createElement("div");
      wrapper.className = "event";

      const heading = document.createElement("h3");
      heading.textContent = name;

      const body = document.createElement("pre");
      body.textContent = JSON.stringify(payload, null, 2);

      wrapper.appendChild(heading);
      wrapper.appendChild(body);
      eventsContainer.appendChild(wrapper);
    }

    function closeStream() {
      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      closeStream();
      eventsContainer.innerHTML = "";
      meta.textContent = "Creating job...";

      const response = await fetch("/enrich", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: urlInput.value })
      });

      if (!response.ok) {
        const detail = await response.text();
        meta.textContent = "Failed to create job: " + detail;
        return;
      }

      const data = await response.json();
      meta.textContent = "Job " + data.job_id + " created. Streaming events...";

      eventSource = new EventSource("/stream/" + data.job_id);

      ["step", "error", "done"].forEach((eventName) => {
        eventSource.addEventListener(eventName, (message) => {
          const parsed = JSON.parse(message.data);
          const label = parsed.step ? parsed.type + ":" + parsed.step : parsed.type;
          renderEvent(label, parsed.data);
          if (eventName === "done") {
            meta.textContent = "Job " + data.job_id + " finished with status: " + parsed.data.status;
            closeStream();
          }
        });
      });

      eventSource.onerror = () => {
        meta.textContent = "Stream closed. Results remain available via the job endpoint.";
      };
    });
  </script>
</body>
</html>
"""


async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.jobs = {}
    app.state.subscribers = {}
    app.state.job_queue = asyncio.Queue()
    worker_count = max(2, min(4, os.cpu_count() or 2))
    worker_tasks = [asyncio.create_task(job_worker(app)) for _ in range(worker_count)]
    try:
        yield
    finally:
        for worker_task in worker_tasks:
            worker_task.cancel()
        for worker_task in worker_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task


app = FastAPI(title="Data Enrichment Pipeline", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(content=HTML_PAGE)


@app.post(
    "/enrich",
    response_model=JobCreated,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_job(payload: EnrichRequest) -> JobCreated:
    job_id = str(uuid4())
    now = utc_now()
    job = JobStatus(
        job_id=job_id,
        url=str(payload.url),
        status=JobState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    app.state.jobs[job_id] = job
    await app.state.job_queue.put(job_id)
    return JobCreated(job_id=job_id, status=job.status)


@app.get("/stream/{job_id}")
async def stream_job(job_id: str, request: Request) -> StreamingResponse:
    if job_id not in app.state.jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        stream_job_events(request, app, job_id),
        media_type="text/event-stream",
        headers=headers,
    )


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job(job_id: str) -> JobStatus:
    job = app.state.jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
