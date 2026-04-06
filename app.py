# app.py
"""33m2 서울 단기임대 시장 분석 대시보드."""

import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = "data/rooms.db"


def load_collections(db_path: str = DB_PATH) -> list[dict]:
    """완료된 수집 세션 목록을 반환."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, collected_at, status FROM collections WHERE status = 'completed' ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_rooms(db_path: str = DB_PATH, collection_id: int = None) -> pd.DataFrame:
    """매물 데이터를 DataFrame으로 반환 (예약률 포함)."""
    conn = sqlite3.connect(db_path)
    query = """
        SELECT r.*, b.available_mid
        FROM rooms r
        LEFT JOIN booking_rates b ON r.rid = b.rid AND r.collected_id = b.collected_id
    """
    params = ()
    if collection_id is not None:
        query += " WHERE r.collected_id = ?"
        params = (collection_id,)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# --- Streamlit UI ---

st.set_page_config(page_title="33m2 서울 단기임대 분석", layout="wide")
st.title("33m2 서울 단기임대 시장 분석")

# 사이드바
with st.sidebar:
    st.header("필터")

    collections = load_collections()
    if not collections:
        st.error("수집된 데이터가 없습니다. `python collector.py`를 먼저 실행하세요.")
        st.stop()

    collection_options = {
        f"{c['collected_at'][:10]} (#{c['id']})": c["id"] for c in collections
    }
    selected_label = st.selectbox("수집 시점", list(collection_options.keys()))
    selected_cid = collection_options[selected_label]

    df = load_rooms(collection_id=selected_cid)
    df["total_fee"] = df["using_fee"].fillna(0) + df["mgmt_fee"].fillna(0)

    EXCLUDE_TYPES = {"고시원", "호텔", "모텔여관"}
    property_types = sorted(df["property_type"].dropna().unique())
    default_types = [t for t in property_types if t not in EXCLUDE_TYPES]
    selected_types = st.multiselect(
        "건물유형", property_types, default=default_types
    )
    if selected_types:
        df = df[df["property_type"].isin(selected_types)]

    # 룸 수 필터
    room_counts = sorted(df["room_cnt"].dropna().unique())
    room_labels = [f"{int(r)}룸" for r in room_counts]
    selected_rooms = st.multiselect(
        "룸 수", room_labels, default=room_labels
    )
    if selected_rooms:
        selected_room_vals = [int(l.replace("룸", "")) for l in selected_rooms]
        df = df[df["room_cnt"].isin(selected_room_vals)]

# 세션 상태로 드릴다운 레벨 관리
if "level" not in st.session_state:
    st.session_state.level = 1
    st.session_state.province = None
    st.session_state.town = None


def go_to_level(level, province=None, town=None):
    st.session_state.level = level
    st.session_state.province = province
    st.session_state.town = town


# 브레드크럼
breadcrumb_parts = ["서울"]
if st.session_state.level >= 2 and st.session_state.province:
    breadcrumb_parts.append(st.session_state.province)
if st.session_state.level >= 3 and st.session_state.town:
    breadcrumb_parts.append(st.session_state.town)

cols = st.columns(len(breadcrumb_parts))
for i, part in enumerate(breadcrumb_parts):
    with cols[i]:
        if i < len(breadcrumb_parts) - 1:
            if i == 0:
                st.button(f"📍 {part}", on_click=go_to_level, args=(1,), key=f"bc_{i}")
            elif i == 1:
                st.button(
                    f"📍 {part}",
                    on_click=go_to_level,
                    args=(2, st.session_state.province),
                    key=f"bc_{i}",
                )
        else:
            st.markdown(f"**📍 {part}**")

st.divider()

# === Level 1: 서울 전체 개요 ===
if st.session_state.level == 1:
    # KPI 카드
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 매물 수", f"{len(df):,}개")
    col2.metric("평균 총 임대료", f"{df['total_fee'].mean():,.0f}원")
    super_host_pct = df["is_super_host"].mean() * 100
    col3.metric("슈퍼호스트 비율", f"{super_host_pct:.1f}%")
    has_booking = df["available_mid"].notna()
    if has_booking.any():
        booked_pct = (1 - df.loc[has_booking, "available_mid"].mean()) * 100
        col4.metric("중기 예약률", f"{booked_pct:.1f}%")
    else:
        col4.metric("중기 예약률", "—")

    st.divider()

    tab_map, tab_price, tab_revenue, tab_chart = st.tabs(["🗺️ 예약률 지도", "🏷️ 객단가별 예약률", "💰 수익 잠재력 지도", "📊 차트"])

    with tab_map:
        # 동별 집계: 중심 좌표 + 예약률 + 매물 수 + 평균 총 임대료
        map_stats = (
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
            map_stats = map_stats.merge(booking_agg, on=["province", "town"], how="left")
            map_stats["booking_rate"] = map_stats["booking_rate"].fillna(0)
        else:
            map_stats["booking_rate"] = 0

        map_stats["name"] = map_stats["province"] + " " + map_stats["town"]

        fig_map = px.scatter_mapbox(
            map_stats,
            lat="lat",
            lon="lng",
            size="count",
            color="booking_rate",
            color_continuous_scale="Purples",
            range_color=[0, 60],
            size_max=30,
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
            zoom=11,
            center={"lat": 37.5665, "lon": 126.978},
        )
        fig_map.update_layout(
            height=700,
            margin={"r": 0, "t": 30, "l": 0, "b": 0},
            coloraxis_colorbar_title="예약률 (%)",
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.caption("🟣 진한 보라 = 높은 예약률 (수요 높음) · ⚪ 연한색 = 낮은 예약률 · 원 크기 = 매물 수")

    with tab_price:
        price_tiers = [
            ("10~20만원", 100000, 200000),
            ("20~30만원", 200000, 300000),
            ("30~40만원", 300000, 400000),
            ("40만원 이상", 400000, None),
        ]

        for tier_name, lo, hi in price_tiers:
            tier_df = df[df["total_fee"] >= lo]
            if hi is not None:
                tier_df = tier_df[tier_df["total_fee"] < hi]

            tier_has_booking = tier_df["available_mid"].notna()
            tier_total = len(tier_df)
            if tier_has_booking.any():
                tier_rate = (1 - tier_df.loc[tier_has_booking, "available_mid"].mean()) * 100
            else:
                tier_rate = 0

            st.subheader(f"{tier_name} (매물 {tier_total:,}개 · 예약률 {tier_rate:.1f}%)")

            if tier_total == 0:
                st.info("해당 가격대 매물이 없습니다.")
                continue

            tier_stats = (
                tier_df.groupby(["province", "town"])
                .agg(
                    lat=("lat", "mean"),
                    lng=("lng", "mean"),
                    count=("rid", "count"),
                    avg_fee=("total_fee", "mean"),
                )
                .reset_index()
            )
            if tier_has_booking.any():
                tier_booking = (
                    tier_df[tier_has_booking]
                    .groupby(["province", "town"])
                    .agg(booking_rate=("available_mid", lambda x: (1 - x.mean()) * 100))
                    .reset_index()
                )
                tier_stats = tier_stats.merge(tier_booking, on=["province", "town"], how="left")
                tier_stats["booking_rate"] = tier_stats["booking_rate"].fillna(0)
            else:
                tier_stats["booking_rate"] = 0
            tier_stats["name"] = tier_stats["province"] + " " + tier_stats["town"]
            tier_stats = tier_stats[tier_stats["count"] > 2]

            fig_tier = px.scatter_mapbox(
                tier_stats,
                lat="lat",
                lon="lng",
                size="count",
                color="booking_rate",
                color_continuous_scale="Purples",
                range_color=[0, 60],
                size_max=35,
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
                zoom=11,
                center={"lat": 37.5665, "lon": 126.978},
            )
            fig_tier.update_layout(
                height=600,
                margin={"r": 0, "t": 30, "l": 0, "b": 0},
                coloraxis_colorbar_title="예약률 (%)",
            )
            st.plotly_chart(fig_tier, use_container_width=True, key=f"tier_{tier_name}")

            # 상위 동 테이블
            top_tier = tier_stats.nlargest(10, "booking_rate")[["name", "count", "avg_fee", "booking_rate"]].copy()
            top_tier.columns = ["동", "매물 수", "평균 총 임대료 (원)", "예약률 (%)"]
            top_tier["평균 총 임대료 (원)"] = top_tier["평균 총 임대료 (원)"].apply(lambda x: f"{x:,.0f}")
            top_tier["예약률 (%)"] = top_tier["예약률 (%)"].apply(lambda x: f"{x:.1f}")
            st.dataframe(top_tier, use_container_width=True, hide_index=True)

            st.divider()

    with tab_revenue:
        # 예약률 × 평균 총 임대료 = 수익 잠재력 스코어 (매물 5개 이하 동 제외)
        rev_stats = map_stats[map_stats["count"] > 5].copy()
        rev_stats["revenue_score"] = rev_stats["booking_rate"] / 100 * rev_stats["avg_fee"]

        fig_rev = px.scatter_mapbox(
            rev_stats,
            lat="lat",
            lon="lng",
            size="revenue_score",
            color="revenue_score",
            color_continuous_scale="Purples",
            size_max=50,
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
                "revenue_score": ":,.0f",
            },
            labels={
                "count": "매물 수",
                "booking_rate": "예약률 (%)",
                "avg_fee": "평균 총 임대료 (원)",
                "revenue_score": "수익 잠재력",
            },
            mapbox_style="open-street-map",
            zoom=11,
            center={"lat": 37.5665, "lon": 126.978},
        )
        fig_rev.update_layout(
            height=700,
            margin={"r": 0, "t": 30, "l": 0, "b": 0},
            coloraxis_colorbar_title="수익 잠재력",
        )
        st.plotly_chart(fig_rev, use_container_width=True)

        st.caption("수익 잠재력 = 예약률 × 평균 총 임대료 · 🟣 진할수록 수익 기대치 높음 · 원 크기 = 매물 수")

        # 상위 동 랭킹 테이블
        top = rev_stats.nlargest(15, "revenue_score")[["name", "count", "avg_fee", "booking_rate", "revenue_score"]].copy()
        top.columns = ["동", "매물 수", "평균 총 임대료 (원)", "예약률 (%)", "수익 잠재력"]
        top["평균 총 임대료 (원)"] = top["평균 총 임대료 (원)"].apply(lambda x: f"{x:,.0f}")
        top["예약률 (%)"] = top["예약률 (%)"].apply(lambda x: f"{x:.1f}")
        top["수익 잠재력"] = top["수익 잠재력"].apply(lambda x: f"{x:,.0f}")
        st.subheader("수익 잠재력 Top 15 동")
        st.dataframe(top, use_container_width=True, hide_index=True)

    with tab_chart:
        # 구별 매물 수 + 구별 평균 총 임대료
        province_stats = (
            df.groupby("province")
            .agg(count=("rid", "count"), avg_fee=("total_fee", "mean"))
            .reset_index()
            .sort_values("count", ascending=True)
        )

        left, right = st.columns(2)
        with left:
            fig = px.bar(
                province_stats,
                y="province",
                x="count",
                orientation="h",
                title="구별 매물 수",
                labels={"province": "", "count": "매물 수"},
            )
            fig.update_layout(height=600)
            event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="province_count")
            if event and event.selection and event.selection.points:
                clicked_province = event.selection.points[0]["y"]
                go_to_level(2, province=clicked_province)
                st.rerun()

        with right:
            fig2 = px.bar(
                province_stats.sort_values("avg_fee", ascending=True),
                y="province",
                x="avg_fee",
                orientation="h",
                title="구별 평균 총 임대료",
                labels={"province": "", "avg_fee": "평균 총 임대료 (원)"},
            )
            fig2.update_layout(height=600)
            st.plotly_chart(fig2, use_container_width=True)

        # 구별 예약률
        if has_booking.any():
            booking_stats = (
                df[has_booking]
                .groupby("province")
                .agg(booking_rate=("available_mid", lambda x: (1 - x.mean()) * 100))
                .reset_index()
                .sort_values("booking_rate", ascending=True)
            )
            fig_book = px.bar(
                booking_stats,
                y="province",
                x="booking_rate",
                orientation="h",
                title="구별 중기 예약률 (%)",
                labels={"province": "", "booking_rate": "예약률 (%)"},
            )
            fig_book.update_layout(height=600)
            st.plotly_chart(fig_book, use_container_width=True)

        st.divider()
        st.subheader("동별 분석")

        # 동별 집계 (매물 5개 초과만)
        town_full = (
            df.groupby(["province", "town"])
            .agg(count=("rid", "count"), avg_fee=("total_fee", "mean"))
            .reset_index()
        )
        town_full["name"] = town_full["province"] + " " + town_full["town"]
        town_full = town_full[town_full["count"] > 5]

        left_t, right_t = st.columns(2)
        with left_t:
            top_count = town_full.nlargest(30, "count")
            fig_tc = px.bar(
                top_count.sort_values("count", ascending=True),
                y="name",
                x="count",
                orientation="h",
                title="동별 매물 수 Top 30",
                labels={"name": "", "count": "매물 수"},
            )
            fig_tc.update_layout(height=800)
            st.plotly_chart(fig_tc, use_container_width=True)

        with right_t:
            top_fee = town_full.nlargest(30, "avg_fee")
            fig_tf = px.bar(
                top_fee.sort_values("avg_fee", ascending=True),
                y="name",
                x="avg_fee",
                orientation="h",
                title="동별 평균 총 임대료 Top 30",
                labels={"name": "", "avg_fee": "평균 총 임대료 (원)"},
            )
            fig_tf.update_layout(height=800)
            st.plotly_chart(fig_tf, use_container_width=True)

        # 동별 예약률 Top 30
        if has_booking.any():
            town_booking_full = (
                df[has_booking]
                .groupby(["province", "town"])
                .agg(
                    booking_rate=("available_mid", lambda x: (1 - x.mean()) * 100),
                    count=("rid", "count"),
                )
                .reset_index()
            )
            town_booking_full["name"] = town_booking_full["province"] + " " + town_booking_full["town"]
            town_booking_full = town_booking_full[town_booking_full["count"] > 5]
            top_booking = town_booking_full.nlargest(30, "booking_rate")
            fig_tb_full = px.bar(
                top_booking.sort_values("booking_rate", ascending=True),
                y="name",
                x="booking_rate",
                orientation="h",
                title="동별 중기 예약률 Top 30 (%)",
                labels={"name": "", "booking_rate": "예약률 (%)"},
            )
            fig_tb_full.update_layout(height=800)
            st.plotly_chart(fig_tb_full, use_container_width=True)

        st.divider()

        # 건물유형 분포 + 임대료 분포
        left2, right2 = st.columns(2)
        with left2:
            type_counts = df["property_type"].value_counts().reset_index()
            type_counts.columns = ["property_type", "count"]
            fig3 = px.pie(
                type_counts,
                values="count",
                names="property_type",
                title="건물유형 분포",
            )
            st.plotly_chart(fig3, use_container_width=True)

        with right2:
            fig4 = px.histogram(
                df,
                x="total_fee",
                nbins=30,
                title="임대료 분포",
                labels={"total_fee": "총 임대료 (원)"},
            )
            st.plotly_chart(fig4, use_container_width=True)

# === Level 2: 구 상세 ===
elif st.session_state.level == 2:
    province = st.session_state.province
    pdf = df[df["province"] == province]

    # KPI
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("매물 수", f"{len(pdf):,}개")
    col2.metric("평균 총 임대료", f"{pdf['total_fee'].mean():,.0f}원")
    col3.metric("건물유형 수", f"{pdf['property_type'].nunique()}종")
    p_has_booking = pdf["available_mid"].notna()
    if p_has_booking.any():
        p_booked_pct = (1 - pdf.loc[p_has_booking, "available_mid"].mean()) * 100
        col4.metric("중기 예약률", f"{p_booked_pct:.1f}%")
    else:
        col4.metric("중기 예약률", "—")

    st.divider()

    tab_map2, tab_chart2 = st.tabs(["🗺️ 지도", "📊 차트"])

    with tab_map2:
        # 개별 매물 위치 (예약 상태별 색상)
        map_df = pdf[pdf["lat"].notna() & pdf["lng"].notna()].copy()
        map_df["booking_status"] = map_df["available_mid"].apply(
            lambda x: "예약됨" if x == 0 else ("가능" if x == 1 else "정보없음")
        )
        color_map = {"예약됨": "#e74c3c", "가능": "#2ecc71", "정보없음": "#95a5a6"}

        fig_map2 = px.scatter_mapbox(
            map_df,
            lat="lat",
            lon="lng",
            color="booking_status",
            color_discrete_map=color_map,
            hover_name="room_name",
            hover_data={
                "lat": False,
                "lng": False,
                "town": True,
                "property_type": True,
                "total_fee": ":,",
                "booking_status": False,
            },
            labels={
                "booking_status": "예약 상태",
                "town": "동",
                "property_type": "유형",
                "total_fee": "총 임대료 (원)",
            },
            mapbox_style="open-street-map",
            zoom=13,
            center={"lat": map_df["lat"].mean(), "lon": map_df["lng"].mean()},
            opacity=0.7,
        )
        fig_map2.update_layout(
            height=600,
            margin={"r": 0, "t": 30, "l": 0, "b": 0},
        )
        st.plotly_chart(fig_map2, use_container_width=True)
        st.caption("🔴 예약됨 · 🟢 가능 · 🔘 정보없음")

    with tab_chart2:
        left, right = st.columns(2)
        with left:
            town_stats = (
                pdf.groupby("town")
                .agg(count=("rid", "count"))
                .reset_index()
                .sort_values("count", ascending=True)
            )
            fig = px.bar(
                town_stats,
                y="town",
                x="count",
                orientation="h",
                title=f"{province} 동별 매물 수",
                labels={"town": "", "count": "매물 수"},
            )
            fig.update_layout(height=max(400, len(town_stats) * 25))
            event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="town_count")
            if event and event.selection and event.selection.points:
                clicked_town = event.selection.points[0]["y"]
                go_to_level(3, province=province, town=clicked_town)
                st.rerun()

        with right:
            type_stats = (
                pdf.groupby("property_type")
                .agg(avg_fee=("total_fee", "mean"))
                .reset_index()
                .sort_values("avg_fee", ascending=True)
            )
            fig2 = px.bar(
                type_stats,
                y="property_type",
                x="avg_fee",
                orientation="h",
                title=f"{province} 건물유형별 평균 총 임대료",
                labels={"property_type": "", "avg_fee": "평균 총 임대료 (원)"},
            )
            st.plotly_chart(fig2, use_container_width=True)

        # 동별 예약률
        if p_has_booking.any():
            town_booking = (
                pdf[p_has_booking]
                .groupby("town")
                .agg(booking_rate=("available_mid", lambda x: (1 - x.mean()) * 100))
                .reset_index()
                .sort_values("booking_rate", ascending=True)
            )
            fig_tb = px.bar(
                town_booking,
                y="town",
                x="booking_rate",
                orientation="h",
                title=f"{province} 동별 중기 예약률 (%)",
                labels={"town": "", "booking_rate": "예약률 (%)"},
            )
            fig_tb.update_layout(height=max(400, len(town_booking) * 25))
            st.plotly_chart(fig_tb, use_container_width=True)

# === Level 3: 동 상세 ===
elif st.session_state.level == 3:
    province = st.session_state.province
    town = st.session_state.town
    tdf = df[(df["province"] == province) & (df["town"] == town)]

    col1, col2, col3 = st.columns(3)
    col1.metric("매물 수", f"{len(tdf):,}개")
    col2.metric("평균 총 임대료", f"{tdf['total_fee'].mean():,.0f}원")
    t_has_booking = tdf["available_mid"].notna()
    if t_has_booking.any():
        t_booked_pct = (1 - tdf.loc[t_has_booking, "available_mid"].mean()) * 100
        col3.metric("중기 예약률", f"{t_booked_pct:.1f}%")
    else:
        col3.metric("중기 예약률", "—")

    st.divider()

    # 동 내 매물 지도
    map_tdf = tdf[tdf["lat"].notna() & tdf["lng"].notna()].copy()
    if not map_tdf.empty:
        map_tdf["booking_status"] = map_tdf["available_mid"].apply(
            lambda x: "예약됨" if x == 0 else ("가능" if x == 1 else "정보없음")
        )
        color_map3 = {"예약됨": "#e74c3c", "가능": "#2ecc71", "정보없음": "#95a5a6"}
        fig_map3 = px.scatter_mapbox(
            map_tdf,
            lat="lat",
            lon="lng",
            color="booking_status",
            color_discrete_map=color_map3,
            hover_name="room_name",
            hover_data={
                "lat": False,
                "lng": False,
                "property_type": True,
                "total_fee": ":,",
                "booking_status": False,
            },
            labels={
                "booking_status": "예약 상태",
                "property_type": "유형",
                "total_fee": "총 임대료 (원)",
            },
            mapbox_style="open-street-map",
            zoom=14,
            center={"lat": map_tdf["lat"].mean(), "lon": map_tdf["lng"].mean()},
            opacity=0.8,
        )
        fig_map3.update_layout(
            height=450,
            margin={"r": 0, "t": 30, "l": 0, "b": 0},
        )
        st.plotly_chart(fig_map3, use_container_width=True)

    st.subheader("매물 목록")

    display_df = tdf[
        [
            "room_name",
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
        "유형",
        "평수",
        "총 임대료(원)",
        "방수",
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
