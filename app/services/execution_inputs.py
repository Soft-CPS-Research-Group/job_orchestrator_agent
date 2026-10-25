"""Content-addressed execution inputs from explicitly authorized shared roots."""
import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path

from fastapi import HTTPException

from app.config import settings
from app.utils import submission_store


def inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise HTTPException(400, "Input symlinks are not permitted")
        if path.is_file():
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            result[path.relative_to(root).as_posix()] = digest.hexdigest()
    if not result:
        raise HTTPException(400, "Input directory is empty")
    return result


def digest(files):
    return submission_store.digest(files)


def input_name(principal, checksum):
    if not re.fullmatch(r"[a-f0-9]{64}", checksum):
        raise HTTPException(400, "Invalid input digest")
    owner = hashlib.sha256(principal['id'].encode()).hexdigest()[:16]
    return f"execution_{owner}_{checksum}"


def register(principal, relative_path, checksum):
    if not principal.get("input_root"):
        raise HTTPException(403, "No shared input root is authorized for this client")
    root = Path(principal['input_root']).resolve()
    relative = Path(relative_path)
    source = root / relative
    if relative.is_absolute() or '..' in relative.parts or not source.resolve().is_relative_to(root):
        raise HTTPException(400, "Invalid input path")
    name = input_name(principal, checksum)
    target = Path(settings.DATASETS_DIR) / name
    target.parent.mkdir(parents=True, exist_ok=True)
    with submission_store.locked('input-' + name):
        if not target.exists():
            if source.is_symlink() or not source.is_dir():
                raise HTTPException(400, "Input directory unavailable")
            if digest(inventory(source)) != checksum:
                raise HTTPException(409, "Input content changed")
            temporary = Path(tempfile.mkdtemp(prefix='.execution-', dir=target.parent))
            try:
                shutil.copytree(source, temporary, dirs_exist_ok=True, symlinks=True)
                if digest(inventory(temporary)) != checksum:
                    raise HTTPException(409, "Input changed during materialization")
                for item in temporary.rglob('*'):
                    item.chmod(0o555 if item.is_dir() else 0o444)
                os.rename(temporary, target)
            finally:
                if temporary.exists():
                    for item in temporary.rglob('*'):
                        if item.is_dir():
                            item.chmod(0o755)
                    shutil.rmtree(temporary)
        verify(principal, {'sha256': checksum, 'name': name})
    return {'sha256': checksum, 'name': name, 'schema_path': f'/data/datasets/{name}/schema.json'}


def verify(principal, reference):
    name = input_name(principal, reference['sha256'])
    if reference['name'] != name:
        raise HTTPException(403, "Input belongs to a different client")
    path = Path(settings.DATASETS_DIR) / name
    if not path.is_dir() or path.is_symlink() or digest(inventory(path)) != reference['sha256']:
        raise HTTPException(409, "Input is unavailable or changed")
    return name


def validate_config(config, names):
    if isinstance(config, dict):
        for key, value in config.items():
            if key == 'dataset_name' and value not in names:
                raise HTTPException(400, "Dataset must reference a registered immutable input")
            if key == 'dataset_path' and value not in {f'/data/datasets/{name}/schema.json' for name in names}:
                raise HTTPException(400, "Dataset path must reference a registered immutable input")
            validate_config(value, names)
    elif isinstance(config, list):
        for value in config:
            validate_config(value, names)
