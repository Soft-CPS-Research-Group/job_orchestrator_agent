import os
import io
import json
import zipfile
import importlib
import pytest
import sys
from pathlib import Path

@pytest.fixture
def dataset_env(tmp_path, monkeypatch):
    monkeypatch.setenv("VM_SHARED_DATA", str(tmp_path))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import app.config as config
    importlib.reload(config)
    config.settings.DATASETS_DIR = os.path.join(config.settings.VM_SHARED_DATA, "datasets")
    from app.utils import file_utils
    importlib.reload(file_utils)
    return file_utils, config.settings

def test_list_datasets_returns_description(dataset_env):
    file_utils, settings = dataset_env
    datasets_dir = settings.DATASETS_DIR
    os.makedirs(datasets_dir, exist_ok=True)

    dir_path = os.path.join(datasets_dir, "dir_ds")
    os.makedirs(dir_path)
    with open(os.path.join(dir_path, "a.parquet"), "w") as f:
        f.write("hi")

    file_path = os.path.join(datasets_dir, "file_ds.csv")
    with open(file_path, "w") as f:
        f.write("data")

    datasets = file_utils.list_available_datasets()
    names = {d["name"] for d in datasets}
    assert "dir_ds" in names
    assert "file_ds.csv" in names
    dir_meta = next(d for d in datasets if d["name"] == "dir_ds")
    assert "description" in dir_meta
    assert dir_meta["format"] == "parquet"
    assert dir_meta["type"] == "parquet"
    assert dir_meta["formats"] == ["parquet"]
    assert dir_meta["format_counts"] == {"parquet": 1}
    file_meta = next(d for d in datasets if d["name"] == "file_ds.csv")
    assert file_meta["format"] == "csv"


def test_list_datasets_uses_majority_file_format(dataset_env):
    file_utils, settings = dataset_env
    datasets_dir = settings.DATASETS_DIR
    dataset_dir = Path(datasets_dir) / "majority_ds"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "schema.json").write_text("{}", encoding="utf-8")
    (dataset_dir / "a.csv").write_text("a\n1\n", encoding="utf-8")
    (dataset_dir / "b.parquet").write_text("not-real-parquet", encoding="utf-8")
    (dataset_dir / "c.parquet").write_text("not-real-parquet", encoding="utf-8")

    dataset = next(item for item in file_utils.list_available_datasets() if item["name"] == "majority_ds")

    assert dataset["format"] == "parquet"
    assert dataset["formats"] == ["csv", "parquet"]
    assert dataset["format_counts"] == {"csv": 1, "parquet": 2}


def test_list_datasets_marks_tied_formats_as_mixed(dataset_env):
    file_utils, settings = dataset_env
    dataset_dir = Path(settings.DATASETS_DIR) / "mixed_ds"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "schema.json").write_text("{}", encoding="utf-8")
    (dataset_dir / "a.csv").write_text("a\n1\n", encoding="utf-8")
    (dataset_dir / "b.parquet").write_text("not-real-parquet", encoding="utf-8")

    dataset = next(item for item in file_utils.list_available_datasets() if item["name"] == "mixed_ds")

    assert dataset["format"] == "mixed"
    assert dataset["formats"] == ["csv", "parquet"]

def test_delete_dataset_by_name(dataset_env):
    file_utils, settings = dataset_env
    datasets_dir = settings.DATASETS_DIR
    os.makedirs(datasets_dir, exist_ok=True)

    dir_path = os.path.join(datasets_dir, "dir_ds")
    os.makedirs(dir_path)
    file_path = os.path.join(datasets_dir, "file_ds.csv")
    with open(file_path, "w") as f:
        f.write("data")

    assert file_utils.delete_dataset_by_name("dir_ds")
    assert not os.path.exists(dir_path)
    assert file_utils.delete_dataset_by_name("file_ds.csv")
    assert not os.path.exists(file_path)

def test_get_dataset_file_zips_directory(dataset_env):
    file_utils, settings = dataset_env
    datasets_dir = settings.DATASETS_DIR
    os.makedirs(datasets_dir, exist_ok=True)

    dir_path = os.path.join(datasets_dir, "dir_ds")
    os.makedirs(dir_path)
    with open(os.path.join(dir_path, "a.txt"), "w") as f:
        f.write("hi")

    archive_path = file_utils.get_dataset_file("dir_ds")
    assert archive_path.endswith(".zip")
    assert os.path.exists(archive_path)
    os.remove(archive_path)


def test_upload_dataset_archive_extracts_zip(dataset_env):
    file_utils, settings = dataset_env
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)

    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("site/input.csv", "a,b\n1,2\n")
    payload.seek(0)

    result = file_utils.upload_dataset_archive(payload, "site.zip", "uploaded_ds")
    assert result["name"] == "uploaded_ds"
    assert result["format"] == "csv"
    assert result["format_counts"] == {"csv": 1}
    extracted_file = Path(settings.DATASETS_DIR) / "uploaded_ds" / "input.csv"
    assert extracted_file.exists()
    metadata = json.loads((Path(settings.DATASETS_DIR) / "uploaded_ds" / "upload_metadata.json").read_text())
    assert metadata["uploaded_from"] == "site.zip"


def test_upload_dataset_archive_rejects_unsafe_paths(dataset_env):
    file_utils, settings = dataset_env
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)

    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("../evil.txt", "bad")
    payload.seek(0)

    with pytest.raises(ValueError):
        file_utils.upload_dataset_archive(payload, "unsafe.zip", "unsafe_ds")
