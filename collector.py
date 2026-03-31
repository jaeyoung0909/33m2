# collector.py
"""33m2 서울 단기임대 매물 데이터 수집기."""

import sqlite3
import time
import sys
from datetime import datetime, timezone

import requests

BASE_URL = "https://web.33m2.co.kr/v1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "os-type": "WEB",
    "Content-Type": "application/json",
    "Client-Language": "ko",
}
SEOUL_BBOX = {"swLat": 37.41, "swLng": 126.76, "neLat": 37.72, "neLng": 127.18}
PAGE_SIZE = 50
MAX_RETRIES = 3
DELAY_BETWEEN_PAGES = 0.3
DELAY_BETWEEN_DISTRICTS = 0.8


def init_db(db_path: str) -> sqlite3.Connection:
    """SQLite DB 초기화. 테이블이 없으면 생성."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'in_progress'
        );

        CREATE TABLE IF NOT EXISTS rooms (
            rid INTEGER NOT NULL,
            collected_id INTEGER NOT NULL,
            room_name TEXT,
            state TEXT,
            province TEXT,
            town TEXT,
            property_type TEXT,
            using_fee INTEGER,
            mgmt_fee INTEGER,
            pyeong_size INTEGER,
            room_cnt INTEGER,
            bathroom_cnt INTEGER,
            cookroom_cnt INTEGER,
            sittingroom_cnt INTEGER,
            is_super_host INTEGER,
            longterm_discount_per INTEGER,
            early_discount_amount INTEGER,
            is_new INTEGER,
            lat REAL,
            lng REAL,
            addr_lot TEXT,
            addr_street TEXT,
            pic_main TEXT,
            PRIMARY KEY (rid, collected_id),
            FOREIGN KEY (collected_id) REFERENCES collections(id)
        );

        CREATE TABLE IF NOT EXISTS collection_progress (
            collected_id INTEGER NOT NULL,
            province TEXT NOT NULL,
            completed_at TEXT,
            PRIMARY KEY (collected_id, province),
            FOREIGN KEY (collected_id) REFERENCES collections(id)
        );

        CREATE INDEX IF NOT EXISTS idx_rooms_province ON rooms(province);
        CREATE INDEX IF NOT EXISTS idx_rooms_town ON rooms(province, town);
        CREATE INDEX IF NOT EXISTS idx_rooms_type ON rooms(property_type);
    """)
    conn.commit()
    return conn


def fetch_seoul_districts() -> list[dict]:
    """서울 25개 구 좌표를 markers API에서 가져온다."""
    params = {**SEOUL_BBOX, "zoomLevel": 12}
    resp = requests.get(f"{BASE_URL}/map/markers", headers=HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()["data"]
    return data["regionMarkers"]


def make_bbox(lat: float, lng: float, delta: float = 0.02) -> dict:
    """구 중심 좌표에서 bounding box를 생성."""
    return {
        "swLat": round(lat - delta, 5),
        "swLng": round(lng - delta, 5),
        "neLat": round(lat + delta, 5),
        "neLng": round(lng + delta, 5),
    }


def fetch_rooms_page(
    sw_lat: float,
    sw_lng: float,
    ne_lat: float,
    ne_lng: float,
    page: int,
    size: int = PAGE_SIZE,
) -> tuple[list[dict], bool]:
    """매물 한 페이지를 가져온다. (rooms, is_last) 반환."""
    params = {
        "swLat": sw_lat,
        "swLng": sw_lng,
        "neLat": ne_lat,
        "neLng": ne_lng,
        "zoomLevel": 15,
        "page": page,
        "size": size,
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                f"{BASE_URL}/map/rooms", headers=HEADERS, params=params
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            return data.get("content", []), data.get("last", True)
        except (requests.exceptions.RequestException, KeyError) as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            raise
