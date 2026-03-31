# tests/test_collector.py
import sqlite3
import os
import tempfile
import pytest
import requests
from unittest.mock import patch, MagicMock


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


def test_fetch_seoul_districts_returns_region_markers():
    from collector import fetch_seoul_districts

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": "SCSS_001",
        "data": {
            "regionMarkers": [
                {
                    "name": "강남구",
                    "lat": 37.49794,
                    "lng": 127.06293,
                    "fullName": "서울특별시 강남구",
                    "regionType": "PROVINCE",
                    "nextZoomLevel": 15,
                },
                {
                    "name": "서초구",
                    "lat": 37.48348,
                    "lng": 127.01268,
                    "fullName": "서울특별시 서초구",
                    "regionType": "PROVINCE",
                    "nextZoomLevel": 15,
                },
            ],
            "subwayMarkers": [],
            "landmarkMarkers": [],
        },
    }

    with patch("collector.requests.get", return_value=mock_response):
        districts = fetch_seoul_districts()

    assert len(districts) == 2
    assert districts[0]["name"] == "강남구"
    assert "lat" in districts[0]
    assert "lng" in districts[0]


def test_fetch_rooms_page_returns_rooms_and_pagination():
    from collector import fetch_rooms_page

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": "SCSS_001",
        "data": {
            "content": [
                {
                    "rid": 12345,
                    "roomName": "테스트 숙소",
                    "state": "서울특별시",
                    "province": "강남구",
                    "town": "역삼동",
                    "propertyType": "오피스텔",
                    "usingFee": 400000,
                    "mgmtFee": 80000,
                    "pyeongSize": 8,
                    "roomCnt": 1,
                    "bathroomCnt": 1,
                    "cookroomCnt": 1,
                    "sittingroomCnt": 0,
                    "isSuperHost": True,
                    "longtermDiscountPer": 5,
                    "earlyDiscountAmount": 0,
                    "isNew": False,
                    "lat": 37.5,
                    "lng": 127.0,
                    "addrLot": "서울 강남구 역삼동 123",
                    "addrStreet": "서울 강남구 테헤란로 1",
                    "picMain": "room/test.jpg",
                    "recoType1": False,
                    "recoType2": False,
                    "like": False,
                }
            ],
            "first": True,
            "last": True,
        },
    }

    with patch("collector.requests.get", return_value=mock_response):
        rooms, is_last = fetch_rooms_page(
            sw_lat=37.48, sw_lng=127.0, ne_lat=37.52, ne_lng=127.1, page=1
        )

    assert len(rooms) == 1
    assert rooms[0]["rid"] == 12345
    assert is_last is True


def test_fetch_rooms_page_retries_on_failure():
    from collector import fetch_rooms_page

    mock_fail = MagicMock()
    mock_fail.status_code = 500
    mock_fail.raise_for_status.side_effect = requests.exceptions.HTTPError("500")

    mock_success = MagicMock()
    mock_success.status_code = 200
    mock_success.json.return_value = {
        "code": "SCSS_001",
        "data": {"content": [], "first": True, "last": True},
    }

    with patch(
        "collector.requests.get", side_effect=[mock_fail, mock_success]
    ), patch("collector.time.sleep"):
        rooms, is_last = fetch_rooms_page(
            sw_lat=37.48, sw_lng=127.0, ne_lat=37.52, ne_lng=127.1, page=1
        )

    assert rooms == []
    assert is_last is True


def test_start_collection_creates_record(db_path):
    from collector import init_db, start_collection

    conn = init_db(db_path)
    cid = start_collection(conn)
    assert cid >= 1

    row = conn.execute(
        "SELECT status FROM collections WHERE id = ?", (cid,)
    ).fetchone()
    assert row[0] == "in_progress"
    conn.close()


def test_save_rooms_inserts_data(db_path):
    from collector import init_db, start_collection, save_rooms

    conn = init_db(db_path)
    cid = start_collection(conn)

    rooms = [
        {
            "rid": 100,
            "roomName": "테스트",
            "state": "서울특별시",
            "province": "강남구",
            "town": "역삼동",
            "propertyType": "오피스텔",
            "usingFee": 400000,
            "mgmtFee": 80000,
            "pyeongSize": 8,
            "roomCnt": 1,
            "bathroomCnt": 1,
            "cookroomCnt": 1,
            "sittingroomCnt": 0,
            "isSuperHost": True,
            "longtermDiscountPer": 5,
            "earlyDiscountAmount": 0,
            "isNew": False,
            "lat": 37.5,
            "lng": 127.0,
            "addrLot": "서울 강남구 역삼동 123",
            "addrStreet": "서울 강남구 테헤란로 1",
            "picMain": "room/test.jpg",
        }
    ]
    save_rooms(conn, cid, rooms)

    row = conn.execute("SELECT rid, province FROM rooms").fetchone()
    assert row == (100, "강남구")
    conn.close()


def test_save_rooms_skips_duplicate(db_path):
    from collector import init_db, start_collection, save_rooms

    conn = init_db(db_path)
    cid = start_collection(conn)
    rooms = [
        {
            "rid": 100,
            "roomName": "테스트",
            "state": "서울특별시",
            "province": "강남구",
            "town": "역삼동",
            "propertyType": "오피스텔",
            "usingFee": 400000,
            "mgmtFee": 80000,
            "pyeongSize": 8,
            "roomCnt": 1,
            "bathroomCnt": 1,
            "cookroomCnt": 1,
            "sittingroomCnt": 0,
            "isSuperHost": True,
            "longtermDiscountPer": 5,
            "earlyDiscountAmount": 0,
            "isNew": False,
            "lat": 37.5,
            "lng": 127.0,
            "addrLot": "서울 강남구 역삼동 123",
            "addrStreet": "서울 강남구 테헤란로 1",
            "picMain": "room/test.jpg",
        }
    ]
    save_rooms(conn, cid, rooms)
    save_rooms(conn, cid, rooms)  # 중복 삽입 시도

    count = conn.execute("SELECT COUNT(*) FROM rooms").fetchone()[0]
    assert count == 1
    conn.close()


def test_resume_collection_finds_incomplete(db_path):
    from collector import init_db, start_collection, mark_province_done, resume_collection

    conn = init_db(db_path)
    cid = start_collection(conn)
    mark_province_done(conn, cid, "강남구")

    result = resume_collection(conn)
    assert result is not None
    resumed_cid, done_provinces = result
    assert resumed_cid == cid
    assert "강남구" in done_provinces
    conn.close()


def test_resume_collection_returns_none_when_all_complete(db_path):
    from collector import init_db, resume_collection

    conn = init_db(db_path)
    result = resume_collection(conn)
    assert result is None
    conn.close()
