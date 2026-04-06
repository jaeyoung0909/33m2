# collector.py
"""33m2 서울 단기임대 매물 데이터 수집기."""

import sqlite3
import time
import sys
from datetime import datetime, timezone, timedelta

import requests

BASE_URL = "https://web.33m2.co.kr/v1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "os-type": "WEB",
    "Content-Type": "application/json",
    "Client-Language": "ko",
}
SEOUL_BBOX = {"swLat": 37.41, "swLng": 126.76, "neLat": 37.72, "neLng": 127.18}
PAGE_SIZE = 100
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

        CREATE TABLE IF NOT EXISTS booking_rates (
            rid INTEGER NOT NULL,
            collected_id INTEGER NOT NULL,
            available_mid INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (rid, collected_id),
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
    start_date: str = None,
    end_date: str = None,
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
    if start_date:
        params["startDate"] = start_date
    if end_date:
        params["endDate"] = end_date
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


def fetch_all_rids(
    bbox: dict,
    start_date: str = None,
    end_date: str = None,
) -> set[int]:
    """bbox 내 모든 매물 rid를 수집."""
    rids = set()
    page = 1
    while True:
        rooms, is_last = fetch_rooms_page(
            sw_lat=bbox["swLat"],
            sw_lng=bbox["swLng"],
            ne_lat=bbox["neLat"],
            ne_lng=bbox["neLng"],
            page=page,
            start_date=start_date,
            end_date=end_date,
        )
        for r in rooms:
            rids.add(r["rid"])
        if is_last:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_PAGES)
    return rids


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


def save_booking_rates(conn: sqlite3.Connection, collected_id: int, all_rids: set[int], available_rids: set[int]):
    """예약률 데이터 저장. available_mid=1이면 중기 예약 가능, 0이면 예약됨."""
    for rid in all_rids:
        conn.execute(
            "INSERT OR IGNORE INTO booking_rates (rid, collected_id, available_mid) VALUES (?, ?, ?)",
            (rid, collected_id, 1 if rid in available_rids else 0),
        )
    conn.commit()


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


def collect_district(
    conn: sqlite3.Connection, collected_id: int, district: dict
) -> int:
    """한 구의 모든 매물을 페이지네이션으로 수집. 저장된 총 매물 수 반환."""
    bbox = make_bbox(district["lat"], district["lng"])
    page = 1
    total = 0

    while True:
        rooms, is_last = fetch_rooms_page(
            sw_lat=bbox["swLat"],
            sw_lng=bbox["swLng"],
            ne_lat=bbox["neLat"],
            ne_lng=bbox["neLng"],
            page=page,
        )
        if rooms:
            save_rooms(conn, collected_id, rooms)
            total += len(rooms)

        if is_last:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_PAGES)

    mark_province_done(conn, collected_id, district["name"])
    return total


def collect_all(db_path: str = "data/rooms.db"):
    """서울 전체 수집 (단일 bbox) + 중기 예약률."""
    conn = init_db(db_path)

    # 이전 in_progress 수집이 있으면 완료 처리 (새로 시작)
    resumed = resume_collection(conn)
    if resumed:
        old_cid, _ = resumed
        finish_collection(conn, old_cid)
        print(f"이전 미완료 수집 (id={old_cid})을 완료 처리함")

    collected_id = start_collection(conn)
    print(f"새 수집 시작 (id={collected_id})")

    # Phase 1: 서울 전체 bbox로 모든 매물 수집
    print("\n[Phase 1] 서울 전체 매물 수집 (단일 bbox)...")
    bbox = SEOUL_BBOX
    page = 1
    total = 0
    while True:
        rooms, is_last = fetch_rooms_page(
            sw_lat=bbox["swLat"],
            sw_lng=bbox["swLng"],
            ne_lat=bbox["neLat"],
            ne_lng=bbox["neLng"],
            page=page,
        )
        if rooms:
            # 서울 매물만 저장
            seoul_rooms = [r for r in rooms if r.get("state", "").startswith("서울")]
            save_rooms(conn, collected_id, seoul_rooms)
            total += len(seoul_rooms)

        if page % 20 == 0:
            print(f"  {page}페이지... ({total}개 저장)", flush=True)

        if is_last:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_PAGES)

    all_rids = set(
        r[0] for r in conn.execute(
            "SELECT rid FROM rooms WHERE collected_id = ?", (collected_id,)
        ).fetchall()
    )
    print(f"  완료! 총 {total}개 매물 (서울), {page}페이지 순회")

    # Phase 2: 중기 예약률 (2달 후 1주일 기간으로 조회)
    mid_start = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")
    mid_end = (datetime.now() + timedelta(days=67)).strftime("%Y-%m-%d")
    print(f"\n[Phase 2] 중기 예약률 수집 ({mid_start} ~ {mid_end})...")

    available_rids = set()
    page = 1
    while True:
        rooms, is_last = fetch_rooms_page(
            sw_lat=bbox["swLat"],
            sw_lng=bbox["swLng"],
            ne_lat=bbox["neLat"],
            ne_lng=bbox["neLng"],
            page=page,
            start_date=mid_start,
            end_date=mid_end,
        )
        for r in rooms:
            if r.get("state", "").startswith("서울"):
                available_rids.add(r["rid"])

        if page % 20 == 0:
            print(f"  {page}페이지... ({len(available_rids)}개 가용)", flush=True)

        if is_last:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_PAGES)

    save_booking_rates(conn, collected_id, all_rids, available_rids)

    booked = len(all_rids - available_rids)
    rate = booked / len(all_rids) * 100 if all_rids else 0
    print(f"  완료! 가용: {len(available_rids)}개, 예약됨: {booked}개, 예약률: {rate:.1f}%")

    finish_collection(conn, collected_id)
    print(f"\n수집 완료! (id={collected_id})")
    conn.close()


if __name__ == "__main__":
    collect_all()
