import html
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from app.config import settings


_LOGGER = logging.getLogger(__name__)
_EMAIL_ADDRESS_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _email_debug(message: str) -> None:
    print(f"[job-email] {message}", flush=True)


_STATUS_LABELS = {
    "queued": "queued",
    "dispatched": "dispatched",
    "running": "running",
    "stop_requested": "stop requested",
    "finished": "finished",
    "failed": "failed",
    "stopped": "stopped",
    "canceled": "canceled",
}

_STATUS_COLORS = {
    "queued": "#2563eb",
    "dispatched": "#7c3aed",
    "running": "#059669",
    "stop_requested": "#d97706",
    "finished": "#16a34a",
    "failed": "#dc2626",
    "stopped": "#ca8a04",
    "canceled": "#64748b",
}


def _mapping_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalized_mapping(mapping: dict[str, str] | None) -> dict[str, str]:
    if not isinstance(mapping, dict):
        return {}
    return {_mapping_key(key): str(value).strip() for key, value in mapping.items() if _mapping_key(key) and str(value).strip()}


def _email_recipient(value: Any) -> str | None:
    text = str(value or "").strip()
    if _EMAIL_ADDRESS_RE.fullmatch(text):
        return text
    return None


def normalize_submitted_by(submitted_by: str | None) -> str | None:
    """Normalize service submitters to a human-facing name shown in the UI."""
    text = str(submitted_by or "").strip()
    if not text:
        return None
    return _normalized_mapping(settings.JOB_EMAIL_SUBMITTER_NAMES).get(_mapping_key(text), text)


def _recipient_for_submitter(submitted_by: str | None) -> str | None:
    raw_text = str(submitted_by or "").strip()
    normalized_text = normalize_submitted_by(raw_text)
    if not normalized_text:
        return None
    mapping = _normalized_mapping(settings.JOB_EMAIL_SUBMITTER_EMAILS)
    for candidate in (raw_text, normalized_text):
        recipient = mapping.get(_mapping_key(candidate))
        if recipient:
            return recipient
    for candidate in (raw_text, normalized_text):
        recipient = _email_recipient(candidate)
        if recipient:
            return recipient
    return None


def _status_is_notifiable(status: str) -> bool:
    allowed = {_mapping_key(item) for item in settings.JOB_EMAIL_NOTIFY_STATUSES}
    return _mapping_key(status) in allowed


def _format_timestamp(value: Any) -> str:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return "-"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_duration(value: Any) -> str:
    try:
        seconds = max(0, int(float(value)))
    except (TypeError, ValueError):
        return "-"
    minutes, sec = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minute}m {sec}s"
    if minute:
        return f"{minute}m {sec}s"
    return f"{sec}s"


def _job_url(job_id: str) -> str | None:
    if not settings.UI_BASE_URL:
        return None
    base = str(settings.UI_BASE_URL).rstrip("/")
    return f"{base}/app/ai/jobs/{quote(job_id, safe='')}"


def _job_title(job: dict[str, Any], job_id: str) -> str:
    for key in ("job_name", "run_name", "experiment_name"):
        value = job.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return job_id


def _detail_rows(job_id: str, status: str, previous_status: str | None, job: dict[str, Any]) -> list[tuple[str, str]]:
    rows = [
        ("Job", _job_title(job, job_id)),
        ("Job ID", job_id),
        ("Status", _STATUS_LABELS.get(status, status)),
    ]
    if previous_status:
        rows.append(("Previous status", _STATUS_LABELS.get(previous_status, previous_status)))
    rows.extend(
        [
            ("Submitted by", normalize_submitted_by(job.get("submitted_by")) or "-"),
            ("Target host", str(job.get("target_host") or job.get("preferred_host") or "-")),
            ("Config", str(job.get("config_path") or "-")),
            ("Image tag", str(job.get("image_tag") or "-")),
            ("Queued at", _format_timestamp(job.get("queued_at"))),
            ("Started at", _format_timestamp(job.get("started_at"))),
            ("Finished at", _format_timestamp(job.get("finished_at"))),
            ("Run duration", _format_duration(job.get("run_duration_seconds"))),
        ]
    )
    if job.get("error"):
        rows.append(("Error", str(job.get("error"))))
    details = job.get("details")
    if isinstance(details, dict):
        for key in ("slurm_job_id", "slurm_state", "reported_status", "exit_code"):
            if details.get(key) is not None:
                rows.append((key.replace("_", " ").title(), str(details[key])))
    elif job.get("exit_code") is not None:
        rows.append(("Exit code", str(job.get("exit_code"))))
    return rows


def _build_text_body(
    *,
    job_id: str,
    status: str,
    previous_status: str | None,
    job: dict[str, Any],
    url: str | None,
) -> str:
    rows = _detail_rows(job_id, status, previous_status, job)
    lines = [f"O job mudou para {_STATUS_LABELS.get(status, status)}.", ""]
    lines.extend(f"{label}: {value}" for label, value in rows)
    if url:
        lines.extend(["", f"Abrir na UI: {url}"])
        if settings.UI_LINK_NETWORK_NOTICE:
            lines.append(str(settings.UI_LINK_NETWORK_NOTICE))
    return "\n".join(lines)


def _build_html_body(
    *,
    job_id: str,
    status: str,
    previous_status: str | None,
    job: dict[str, Any],
    url: str | None,
) -> str:
    title = html.escape(_job_title(job, job_id))
    status_text = html.escape(_STATUS_LABELS.get(status, status))
    previous = f" de <strong>{html.escape(_STATUS_LABELS.get(previous_status, previous_status))}</strong>" if previous_status else ""
    color = _STATUS_COLORS.get(status, "#334155")
    rows = "\n".join(
        "<tr>"
        f"<th style=\"text-align:left;padding:8px 12px;color:#475569;font-weight:600;border-bottom:1px solid #e2e8f0;\">{html.escape(label)}</th>"
        f"<td style=\"padding:8px 12px;color:#0f172a;border-bottom:1px solid #e2e8f0;\">{html.escape(value)}</td>"
        "</tr>"
        for label, value in _detail_rows(job_id, status, previous_status, job)
    )
    button = ""
    if url:
        safe_url = html.escape(url, quote=True)
        notice = ""
        if settings.UI_LINK_NETWORK_NOTICE:
            notice = (
                "<p style=\"margin:12px 0 0;color:#64748b;font-size:13px;\">"
                f"{html.escape(str(settings.UI_LINK_NETWORK_NOTICE))}"
                "</p>"
            )
        button = (
            f"<a href=\"{safe_url}\" "
            "style=\"display:inline-block;margin-top:18px;padding:11px 16px;background:#0f172a;color:#ffffff;"
            "text-decoration:none;border-radius:6px;font-weight:700;\">Abrir job na UI</a>"
            f"{notice}"
        )

    return f"""<!doctype html>
<html>
  <body style="margin:0;background:#f8fafc;font-family:Inter,Arial,sans-serif;color:#0f172a;">
    <div style="max-width:680px;margin:0 auto;padding:28px 18px;">
      <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
        <div style="padding:22px 24px;border-bottom:1px solid #e2e8f0;">
          <div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:#64748b;font-weight:700;">EnergAIze job update</div>
          <h1 style="margin:8px 0 8px;font-size:22px;line-height:1.25;color:#0f172a;">{title}</h1>
          <p style="margin:0;color:#475569;font-size:15px;">O job mudou{previous} para
            <span style="display:inline-block;margin-left:4px;padding:3px 9px;background:{color};color:#ffffff;border-radius:999px;font-weight:700;">{status_text}</span>
          </p>
        </div>
        <div style="padding:18px 24px 24px;">
          <table style="width:100%;border-collapse:collapse;font-size:14px;">{rows}</table>
          {button}
        </div>
      </div>
      <p style="margin:14px 4px 0;color:#64748b;font-size:12px;">Email automatico do job orchestrator.</p>
    </div>
  </body>
</html>"""


def build_job_status_email(
    *,
    job_id: str,
    status: str,
    previous_status: str | None,
    job: dict[str, Any],
) -> dict[str, Any] | None:
    recipient = _recipient_for_submitter(job.get("submitted_by"))
    if not recipient:
        return None

    title = _job_title(job, job_id)
    status_text = _STATUS_LABELS.get(status, status)
    url = _job_url(job_id)
    subject = f"[EnergAIze] Job {status_text}: {title}"
    message: dict[str, Any] = {
        "to": [recipient],
        "subject": subject,
        "body": _build_text_body(job_id=job_id, status=status, previous_status=previous_status, job=job, url=url),
        "html_body": _build_html_body(job_id=job_id, status=status, previous_status=previous_status, job=job, url=url),
    }
    if settings.JOB_EMAIL_REPLY_TO:
        message["reply_to"] = settings.JOB_EMAIL_REPLY_TO
    return message


def _publish_email_request(message: dict[str, Any]) -> None:
    try:
        import pika
    except ImportError as exc:
        raise RuntimeError("pika is required to publish RabbitMQ email notifications") from exc

    credentials = None
    if settings.JOB_EMAIL_RABBITMQ_USERNAME or settings.JOB_EMAIL_RABBITMQ_PASSWORD:
        credentials = pika.PlainCredentials(
            settings.JOB_EMAIL_RABBITMQ_USERNAME or "",
            settings.JOB_EMAIL_RABBITMQ_PASSWORD or "",
        )

    timeout = float(settings.JOB_EMAIL_RABBITMQ_SOCKET_TIMEOUT_SECONDS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.JOB_EMAIL_RABBITMQ_HOST,
            port=int(settings.JOB_EMAIL_RABBITMQ_PORT),
            virtual_host=settings.JOB_EMAIL_RABBITMQ_VHOST,
            credentials=credentials,
            connection_attempts=1,
            retry_delay=0,
            socket_timeout=timeout,
            blocked_connection_timeout=timeout,
        )
    )
    try:
        channel = connection.channel()
        channel.queue_declare(queue=settings.JOB_EMAIL_RABBITMQ_QUEUE, durable=True)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange="",
            routing_key=settings.JOB_EMAIL_RABBITMQ_QUEUE,
            body=json.dumps(message, ensure_ascii=False).encode("utf-8"),
            mandatory=True,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
    except pika.exceptions.UnroutableError as exc:
        raise RuntimeError(f"RabbitMQ could not route message to queue {settings.JOB_EMAIL_RABBITMQ_QUEUE!r}") from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass


def notify_job_status_change(
    *,
    job_id: str,
    previous_status: str | None,
    status: str,
    job: dict[str, Any],
) -> None:
    if not settings.JOB_EMAIL_NOTIFICATIONS_ENABLED:
        _email_debug(f"skip job_id={job_id} status={status}: notifications disabled")
        return
    if previous_status == status:
        _email_debug(f"skip job_id={job_id} status={status}: status did not change")
        return
    if not _status_is_notifiable(status):
        _email_debug(f"skip job_id={job_id} status={status}: status is not notifiable")
        return

    message = build_job_status_email(
        job_id=job_id,
        status=status,
        previous_status=previous_status,
        job=job,
    )
    if not message:
        _email_debug(
            f"skip job_id={job_id} status={status}: no recipient for submitted_by={job.get('submitted_by')!r}"
        )
        _LOGGER.warning("Skipping job status email for %s: no recipient for submitter %r", job_id, job.get("submitted_by"))
        return

    recipients = message.get("to") if isinstance(message.get("to"), list) else []
    _email_debug(
        "publish attempt "
        f"job_id={job_id} status={status} previous_status={previous_status or '-'} "
        f"submitted_by={job.get('submitted_by')!r} to={recipients} "
        f"rabbit={settings.JOB_EMAIL_RABBITMQ_HOST}:{settings.JOB_EMAIL_RABBITMQ_PORT}/"
        f"{settings.JOB_EMAIL_RABBITMQ_QUEUE} subject={message.get('subject')!r}"
    )
    try:
        _publish_email_request(message)
    except Exception as exc:
        _email_debug(
            "publish failed "
            f"job_id={job_id} status={status} "
            f"error={type(exc).__name__}: {exc}"
        )
        _LOGGER.exception(
            "Failed to publish email notification for job %s status %s to RabbitMQ %s:%s/%s",
            job_id,
            status,
            settings.JOB_EMAIL_RABBITMQ_HOST,
            settings.JOB_EMAIL_RABBITMQ_PORT,
            settings.JOB_EMAIL_RABBITMQ_QUEUE,
        )
        return

    _email_debug(f"publish ok job_id={job_id} status={status} to={recipients}")
    _LOGGER.info("Published email notification for job %s status %s at %.3f", job_id, status, time.time())
