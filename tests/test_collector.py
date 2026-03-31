# tests/test_collector.py
import sqlite3
import os
import tempfile
import pytest


@pytest.fixture
def db_path():
    """테스트용 임시 DB 파일 생성."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


def test_init_db_creates_tables(db_path):
    from collector import init_db

    conn = init_db(db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert "collections" in tables
    assert "rooms" in tables
    assert "collection_progress" in tables
    conn.close()


def test_init_db_is_idempotent(db_path):
    from collector import init_db

    conn1 = init_db(db_path)
    conn1.close()
    conn2 = init_db(db_path)
    cursor = conn2.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rooms'"
    )
    assert cursor.fetchone() is not None
    conn2.close()
