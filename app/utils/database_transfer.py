"""Copy/verify ONLY Orchestrator tables; never reads Training tables or files.

URLs come from environment variables, never command arguments or output. Run
copy only with all writers stopped. Restore into an empty database; a populated
destination is accepted only when already identical (safe retry).
"""
import argparse
import hashlib
import json
import os

from sqlalchemy import create_engine, select

from app.utils.sql_store import metadata


def snapshot(connection):
    result = {}
    for table in metadata.sorted_tables:
        rows = [dict(row) for row in connection.execute(select(table)).mappings()]
        rows.sort(key=lambda row: tuple(str(row[col.name]) for col in table.primary_key))
        result[table.name] = rows
    return result


def manifest(data):
    return {
        name: {
            "rows": len(rows),
            "sha256": hashlib.sha256(json.dumps(rows, sort_keys=True,
                separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
        }
        for name, rows in data.items()
    }


def transfer(source, destination, *, copy=False):
    if source.url == destination.url:
        raise ValueError("Source and destination must be different databases")
    # A single consistent snapshot; final cutover additionally requires stopped writers.
    with source.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            data = snapshot(conn)
    expected = manifest(data)
    if copy:
        metadata.create_all(destination)
    with destination.begin() as conn:
        existing = snapshot(conn)
        if copy and any(existing.values()) and manifest(existing) != expected:
            raise ValueError("Destination is not empty or identical; refusing to overwrite")
        if copy and not any(existing.values()):
            for table in metadata.sorted_tables:
                if data[table.name]:
                    conn.execute(table.insert(), data[table.name])
        if manifest(snapshot(conn)) != expected:
            raise ValueError("Database content verification failed")
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["copy", "verify"])
    parser.add_argument("--writers-stopped", action="store_true")
    args = parser.parse_args()
    if args.action == "copy" and not args.writers_stopped:
        parser.error("Copy requires confirmation that every source writer is stopped")
    source = create_engine(os.environ["SOURCE_JOB_DATABASE_URL"])
    destination = create_engine(os.environ["DESTINATION_JOB_DATABASE_URL"])
    try:
        print(json.dumps(transfer(source, destination, copy=args.action == "copy"), indent=2))
    finally:
        source.dispose()
        destination.dispose()


if __name__ == "__main__":
    main()
