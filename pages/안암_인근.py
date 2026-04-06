# pages/안암_인근.py
"""안암동 인근 단기임대 상세 지도."""

import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = "data/rooms.db"

# 안암동 중심 좌표 및 인근 범위
ANAM_CENTER = {"lat": 37.5863, "lng": 127.0261}
ANAM_BBOX = {
    "swLat": 37.575,
    "neLat": 37.600,
    "swLng": 127.010,
    "neLng": 127.045,
}


def load_anam_data(db_path: str = DB_PATH, collection_id: int = None) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    query = """
        SELECT r.*, b.available_mid
        FROM rooms r
        LEFT JOIN booking_rates b ON r.rid = b.rid AND r.collected_id = b.collected_id
        WHERE r.lat BETWEEN ? AND ?
        AND r.lng BETWEEN ? AND ?
    """
    params = [ANAM_BBOX["swLat"], ANAM_BBOX["neLat"], ANAM_BBOX["swLng"], ANAM_BBOX["neLng"]]
    if collection_id is not None:
        query += " AND r.collected_id = ?"
        params.append(collection_id)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    df["total_fee"] = df["using_fee"].fillna(0) + df["mgmt_fee"].fillna(0)
    return df


def load_collections(db_path: str = DB_PATH) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, collected_at, status FROM collections WHERE status = 'completed' ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


st.set_page_config(page_title="안암 인근 단기임대 분석", layout="wide")
st.title("📍 안암동 인근 단기임대 상세 분석")

# 사이드바
with st.sidebar:
    st.header("필터")

    collections = load_collections()
    if not collections:
        st.error("수집된 데이터가 없습니다.")
        st.stop()

    collection_options = {
        f"{c['collected_at'][:10]} (#{c['id']})": c["id"] for c in collections
    }
    selected_label = st.selectbox("수집 시점", list(collection_options.keys()))
    selected_cid = collection_options[selected_label]

    df = load_anam_data(collection_id=selected_cid)

    EXCLUDE_TYPES = {"고시원", "호텔", "모텔여관"}
    property_types = sorted(df["property_type"].dropna().unique())
    default_types = [t for t in property_types if t not in EXCLUDE_TYPES]
    selected_types = st.multiselect("건물유형", property_types, default=default_types)
    if selected_types:
        df = df[df["property_type"].isin(selected_types)]

    room_counts = sorted(df["room_cnt"].dropna().unique())
    room_labels = [f"{int(r)}룸" for r in room_counts]
    selected_rooms = st.multiselect("룸 수", room_labels, default=room_labels)
    if selected_rooms:
        selected_room_vals = [int(l.replace("룸", "")) for l in selected_rooms]
        df = df[df["room_cnt"].isin(selected_room_vals)]

# KPI
has_booking = df["available_mid"].notna()
col1, col2, col3, col4 = st.columns(4)
col1.metric("매물 수", f"{len(df):,}개")
col2.metric("평균 총 임대료", f"{df['total_fee'].mean():,.0f}원")
col3.metric("건물유형 수", f"{df['property_type'].nunique()}종")
if has_booking.any():
    booked_pct = (1 - df.loc[has_booking, "available_mid"].mean()) * 100
    col4.metric("중기 예약률", f"{booked_pct:.1f}%")
else:
    col4.metric("중기 예약률", "—")

st.divider()

tab_booking, tab_fee, tab_detail = st.tabs(["🗺️ 예약률 지도", "💰 임대료 지도", "📋 매물 목록"])

with tab_booking:
    # 동별 집계
    town_agg = (
        df.groupby(["province", "town"])
        .agg(
            lat=("lat", "mean"),
            lng=("lng", "mean"),
            count=("rid", "count"),
            avg_fee=("total_fee", "mean"),
        )
        .reset_index()
    )
    if has_booking.any():
        booking_agg = (
            df[has_booking]
            .groupby(["province", "town"])
            .agg(booking_rate=("available_mid", lambda x: (1 - x.mean()) * 100))
            .reset_index()
        )
        town_agg = town_agg.merge(booking_agg, on=["province", "town"], how="left")
        town_agg["booking_rate"] = town_agg["booking_rate"].fillna(0)
    else:
        town_agg["booking_rate"] = 0
    town_agg["name"] = town_agg["province"] + " " + town_agg["town"]

    fig = px.scatter_mapbox(
        town_agg,
        lat="lat",
        lon="lng",
        size="count",
        color="booking_rate",
        color_continuous_scale="Purples",
        range_color=[0, 60],
        size_max=40,
        hover_name="name",
        hover_data={
            "lat": False,
            "lng": False,
            "name": False,
            "province": False,
            "town": False,
            "count": True,
            "booking_rate": ":.1f",
            "avg_fee": ":,.0f",
        },
        labels={
            "count": "매물 수",
            "booking_rate": "예약률 (%)",
            "avg_fee": "평균 총 임대료 (원)",
        },
        mapbox_style="open-street-map",
        zoom=14,
        center={"lat": ANAM_CENTER["lat"], "lon": ANAM_CENTER["lng"]},
    )
    fig.update_layout(
        height=700,
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
        coloraxis_colorbar_title="예약률 (%)",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("🟣 진한 보라 = 높은 예약률 · 원 크기 = 매물 수")

    # 동별 예약률 테이블
    if has_booking.any():
        display_tb = town_agg[["name", "count", "avg_fee", "booking_rate"]].sort_values("booking_rate", ascending=False).copy()
        display_tb.columns = ["동", "매물 수", "평균 총 임대료 (원)", "예약률 (%)"]
        display_tb["평균 총 임대료 (원)"] = display_tb["평균 총 임대료 (원)"].apply(lambda x: f"{x:,.0f}")
        display_tb["예약률 (%)"] = display_tb["예약률 (%)"].apply(lambda x: f"{x:.1f}")
        st.subheader("동별 예약률")
        st.dataframe(display_tb, use_container_width=True, hide_index=True)

with tab_fee:
    fig2 = px.scatter_mapbox(
        town_agg,
        lat="lat",
        lon="lng",
        size="avg_fee",
        color="avg_fee",
        color_continuous_scale="Purples",
        size_max=40,
        hover_name="name",
        hover_data={
            "lat": False,
            "lng": False,
            "name": False,
            "province": False,
            "town": False,
            "count": True,
            "avg_fee": ":,.0f",
            "booking_rate": ":.1f",
        },
        labels={
            "count": "매물 수",
            "avg_fee": "평균 총 임대료 (원)",
            "booking_rate": "예약률 (%)",
        },
        mapbox_style="open-street-map",
        zoom=14,
        center={"lat": ANAM_CENTER["lat"], "lon": ANAM_CENTER["lng"]},
    )
    fig2.update_layout(
        height=700,
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
        coloraxis_colorbar_title="평균 총 임대료 (원)",
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("🟣 진할수록 높은 임대료 · 원 크기 = 평균 임대료")

    # 동별 임대료 요약
    town_fee = (
        df.groupby(["province", "town"])
        .agg(
            count=("rid", "count"),
            avg_fee=("total_fee", "mean"),
            min_fee=("total_fee", "min"),
            max_fee=("total_fee", "max"),
        )
        .reset_index()
        .sort_values("avg_fee", ascending=False)
    )
    town_fee["name"] = town_fee["province"] + " " + town_fee["town"]
    st.subheader("동별 임대료")
    display_tf = town_fee[["name", "count", "avg_fee", "min_fee", "max_fee"]].copy()
    display_tf.columns = ["동", "매물 수", "평균 (원)", "최소 (원)", "최대 (원)"]
    for c in ["평균 (원)", "최소 (원)", "최대 (원)"]:
        display_tf[c] = display_tf[c].apply(lambda x: f"{x:,.0f}")
    st.dataframe(display_tf, use_container_width=True, hide_index=True)

with tab_detail:
    display_df = df[
        [
            "room_name",
            "province",
            "town",
            "property_type",
            "pyeong_size",
            "total_fee",
            "room_cnt",
            "is_super_host",
            "longterm_discount_per",
            "available_mid",
        ]
    ].copy()
    display_df.columns = [
        "매물명",
        "구",
        "동",
        "유형",
        "평수",
        "총 임대료(원)",
        "룸 수",
        "슈퍼호스트",
        "장기할인(%)",
        "중기예약",
    ]
    display_df["슈퍼호스트"] = display_df["슈퍼호스트"].map({1: "✓", 0: "—"})
    display_df["장기할인(%)"] = display_df["장기할인(%)"].apply(
        lambda x: f"{x}%" if pd.notna(x) and x > 0 else "—"
    )
    display_df["중기예약"] = display_df["중기예약"].apply(
        lambda x: "가능" if x == 1 else ("예약됨" if x == 0 else "—")
    )

    st.dataframe(
        display_df.sort_values("총 임대료(원)", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
