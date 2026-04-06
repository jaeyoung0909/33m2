# tests/test_app.py
import sqlite3
import os
import tempfile
import pytest
import pandas as pd


@pytest.fixture
def sample_db():
    """테스트용 DB에 샘플 데이터 삽입."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    from collector import init_db, start_collection, save_rooms

    conn = init_db(path)
    cid = start_collection(conn)
    conn.execute(
        "UPDATE collections SET status = 'completed' WHERE id = ?", (cid,)
    )

    rooms = [
        {
            "rid": i,
            "roomName": f"방{i}",
            "state": "서울특별시",
            "province": province,
            "town": town,
            "propertyType": ptype,
            "usingFee": fee,
            "mgmtFee": 50000,
            "pyeongSize": 8,
            "roomCnt": 1,
            "bathroomCnt": 1,
            "cookroomCnt": 1,
            "sittingroomCnt": 0,
            "isSuperHost": i % 3 == 0,
            "longtermDiscountPer": 5 if i % 2 == 0 else 0,
            "earlyDiscountAmount": 0,
            "isNew": False,
            "lat": 37.5,
            "lng": 127.0,
            "addrLot": "",
            "addrStreet": "",
            "picMain": "",
        }
        for i, (province, town, ptype, fee) in enumerate(
            [
                ("강남구", "역삼동", "오피스텔", 480000),
                ("강남구", "역삼동", "아파트", 800000),
                ("강남구", "삼성동", "오피스텔", 520000),
                ("서초구", "서초동", "아파트", 600000),
                ("서초구", "반포동", "오피스텔", 450000),
                ("마포구", "합정동", "원룸건물", 250000),
            ]
        )
    ]
    save_rooms(conn, cid, rooms)
    conn.commit()
    conn.close()

    yield path
    os.unlink(path)


def test_load_rooms_returns_dataframe(sample_db):
    from app import load_rooms

    df = load_rooms(sample_db, collection_id=1)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 6
    assert "province" in df.columns
    assert "using_fee" in df.columns


def test_load_collections_returns_list(sample_db):
    from app import load_collections

    collections = load_collections(sample_db)
    assert len(collections) == 1
    assert collections[0]["id"] == 1
    assert collections[0]["status"] == "completed"
