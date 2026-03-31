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


def start_collection(conn: sqlite3.Connection) -> int:
    """새 수집 세션을 시작하고 collection id를 반환."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "INSERT INTO collections (collected_at, status) VALUES (?, 'in_progress')",
        (now,),
    )
    conn.commit()
    return cursor.lastrowid


def save_rooms(conn: sqlite3.Connection, collected_id: int, rooms: list[dict]) -> int:
    """매물 목록을 DB에 저장. 중복은 무시. 저장된 수 반환."""
    saved = 0
    for r in rooms:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO rooms (
                    rid, collected_id, room_name, state, province, town,
                    property_type, using_fee, mgmt_fee, pyeong_size,
                    room_cnt, bathroom_cnt, cookroom_cnt, sittingroom_cnt,
                    is_super_host, longterm_discount_per, early_discount_amount,
                    is_new, lat, lng, addr_lot, addr_street, pic_main
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["rid"],
                    collected_id,
                    r.get("roomName"),
                    r.get("state"),
                    r.get("province"),
                    r.get("town"),
                    r.get("propertyType"),
                    r.get("usingFee"),
                    r.get("mgmtFee"),
                    r.get("pyeongSize"),
                    r.get("roomCnt"),
                    r.get("bathroomCnt"),
                    r.get("cookroomCnt"),
                    r.get("sittingroomCnt"),
                    1 if r.get("isSuperHost") else 0,
                    r.get("longtermDiscountPer"),
                    r.get("earlyDiscountAmount"),
                    1 if r.get("isNew") else 0,
                    r.get("lat"),
                    r.get("lng"),
                    r.get("addrLot"),
                    r.get("addrStreet"),
                    r.get("picMain"),
                ),
            )
            saved += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return saved


def mark_province_done(conn: sqlite3.Connection, collected_id: int, province: str):
    """구 수집 완료를 기록."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO collection_progress (collected_id, province, completed_at) VALUES (?, ?, ?)",
        (collected_id, province, now),
    )
    conn.commit()


def finish_collection(conn: sqlite3.Connection, collected_id: int):
    """수집 완료 상태로 변경."""
    conn.execute(
        "UPDATE collections SET status = 'completed' WHERE id = ?", (collected_id,)
    )
    conn.commit()


def resume_collection(conn: sqlite3.Connection) -> tuple[int, set[str]] | None:
    """미완료 수집 세션을 찾아 (collection_id, 완료된 구 set) 반환. 없으면 None."""
    row = conn.execute(
        "SELECT id FROM collections WHERE status = 'in_progress' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    cid = row[0]
    done = conn.execute(
        "SELECT province FROM collection_progress WHERE collected_id = ?", (cid,)
    ).fetchall()
    return cid, {r[0] for r in done}
