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
    """매물 데이터를 DataFrame으로 반환."""
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM rooms"
    params = ()
    if collection_id is not None:
        query += " WHERE collected_id = ?"
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

    property_types = sorted(df["property_type"].dropna().unique())
    selected_types = st.multiselect(
        "건물유형", property_types, default=property_types
    )
    if selected_types:
        df = df[df["property_type"].isin(selected_types)]

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
    col2.metric("평균 주간 임대료", f"{df['using_fee'].mean():,.0f}원")
    col3.metric("평균 관리비", f"{df['mgmt_fee'].mean():,.0f}원")
    super_host_pct = df["is_super_host"].mean() * 100
    col4.metric("슈퍼호스트 비율", f"{super_host_pct:.1f}%")

    st.divider()

    # 구별 매물 수 + 구별 평균 임대료
    province_stats = (
        df.groupby("province")
        .agg(count=("rid", "count"), avg_fee=("using_fee", "mean"))
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
            title="구별 평균 임대료",
            labels={"province": "", "avg_fee": "평균 임대료 (원)"},
        )
        fig2.update_layout(height=600)
        st.plotly_chart(fig2, use_container_width=True)

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
            x="using_fee",
            nbins=30,
            title="임대료 분포",
            labels={"using_fee": "주간 임대료 (원)"},
        )
        st.plotly_chart(fig4, use_container_width=True)

# === Level 2: 구 상세 ===
elif st.session_state.level == 2:
    province = st.session_state.province
    pdf = df[df["province"] == province]

    # KPI
    col1, col2, col3 = st.columns(3)
    col1.metric("매물 수", f"{len(pdf):,}개")
    col2.metric("평균 임대료", f"{pdf['using_fee'].mean():,.0f}원")
    col3.metric("건물유형 수", f"{pdf['property_type'].nunique()}종")

    st.divider()

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
            .agg(avg_fee=("using_fee", "mean"))
            .reset_index()
            .sort_values("avg_fee", ascending=True)
        )
        fig2 = px.bar(
            type_stats,
            y="property_type",
            x="avg_fee",
            orientation="h",
            title=f"{province} 건물유형별 평균 임대료",
            labels={"property_type": "", "avg_fee": "평균 임대료 (원)"},
        )
        st.plotly_chart(fig2, use_container_width=True)

# === Level 3: 동 상세 ===
elif st.session_state.level == 3:
    province = st.session_state.province
    town = st.session_state.town
    tdf = df[(df["province"] == province) & (df["town"] == town)]

    col1, col2, col3 = st.columns(3)
    col1.metric("매물 수", f"{len(tdf):,}개")
    col2.metric("평균 임대료", f"{tdf['using_fee'].mean():,.0f}원")
    col3.metric("평균 관리비", f"{tdf['mgmt_fee'].mean():,.0f}원")

    st.divider()

    display_df = tdf[
        [
            "room_name",
            "property_type",
            "pyeong_size",
            "using_fee",
            "mgmt_fee",
            "room_cnt",
            "is_super_host",
            "longterm_discount_per",
        ]
    ].copy()
    display_df.columns = [
        "매물명",
        "유형",
        "평수",
        "임대료(원)",
        "관리비(원)",
        "방수",
        "슈퍼호스트",
        "장기할인(%)",
    ]
    display_df["슈퍼호스트"] = display_df["슈퍼호스트"].map({1: "✓", 0: "—"})
    display_df["장기할인(%)"] = display_df["장기할인(%)"].apply(
        lambda x: f"{x}%" if pd.notna(x) and x > 0 else "—"
    )

    st.dataframe(
        display_df.sort_values("임대료(원)", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
