"""Durable submission identities; retained after job deletion to prevent replay."""
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

import fcntl
from sqlalchemy import select

from app.config import settings
from app.utils import sql_store


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def identity(request):
    return digest([request.workspace_id or 'laboratory', request.submitted_by or '', request.idempotency_key])


@contextmanager
def locked(key):
    root = Path(settings.VM_SHARED_DATA) / '.submissions'
    root.mkdir(parents=True, exist_ok=True)
    with sql_store.advisory_lock('submission:' + key), (root / (key + '.lock')).open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read(key):
    if sql_store.enabled():
        with sql_store.engine().connect() as c:
            return c.scalar(select(sql_store.submissions.c.payload).where(sql_store.submissions.c.id == key))
    path = Path(settings.VM_SHARED_DATA) / '.submissions' / (key + '.json')
    return json.loads(path.read_text()) if path.exists() else None


def write(key, payload):
    if sql_store.enabled():
        with sql_store.engine().begin() as c:
            sql_store.upsert(c, sql_store.submissions, {'id': key, 'payload': payload}, 'id')
        return
    root = Path(settings.VM_SHARED_DATA) / '.submissions'
    fd, temp = tempfile.mkstemp(dir=root)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, root / (key + '.json'))
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)
