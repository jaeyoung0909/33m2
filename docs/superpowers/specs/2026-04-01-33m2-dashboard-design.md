# 33m2 서울 단기임대 시장 분석 대시보드

## 개요

부동산 투자자를 위한 서울 지역 단기임대 시장 분석 대시보드. 33m2 API에서 수집한 팩트 데이터를 기반으로, 구→동→매물 3단계 드릴다운으로 지역별/건물유형별 시장 현황을 탐색한다.

## 대상 사용자

부동산 투자자 — 서울 어느 지역의 어떤 건물유형이 단기임대 공급이 많고, 임대료 수준이 어떤지 비교하여 투자 판단에 활용.

## 데이터 소스

33m2 공개 API (인증 불필요):

| 엔드포인트 | 용도 |
|-----------|------|
| `GET /v1/map/markers` | 서울 25개 구 좌표 및 bounding box 확보 |
| `GET /v1/map/rooms` | 구별 매물 목록 수집 (zoomLevel≥15) |

Base URL: `https://web.33m2.co.kr/v1`

필수 헤더: `os-type: WEB`, `Content-Type: application/json`, `Client-Language: ko`, `User-Agent: Mozilla/5.0 ...`

## 기술 스택

- **수집**: Python 스크립트 (requests, sqlite3)
- **저장**: SQLite 단일 파일 (`data/rooms.db`)
- **대시보드**: Streamlit (plotly 차트)

## 데이터 수집

### 수집 플로우

1. `/v1/map/markers` (zoomLevel=12, 서울 bounding box)로 25개 구의 좌표 획득
2. 각 구 좌표 기준으로 bounding box 계산
3. `/v1/map/rooms` (zoomLevel=15)로 구별 매물 페이지네이션 순회
4. SQLite에 저장 (중복 시 스킵)

### SQLite 스키마

```sql
CREATE TABLE IF NOT EXISTS collections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,  -- ISO 8601 (예: 2026-04-01T12:00:00)
    status TEXT NOT NULL DEFAULT 'in_progress'  -- in_progress | completed
);

CREATE TABLE IF NOT EXISTS rooms (
    rid INTEGER NOT NULL,
    collected_id INTEGER NOT NULL,
    room_name TEXT,
    state TEXT,          -- 시/도
    province TEXT,       -- 구
    town TEXT,           -- 동
    property_type TEXT,  -- 건물유형
    using_fee INTEGER,   -- 주간 임대료 (원)
    mgmt_fee INTEGER,    -- 관리비 (원)
    pyeong_size INTEGER, -- 평수
    room_cnt INTEGER,
    bathroom_cnt INTEGER,
    cookroom_cnt INTEGER,
    sittingroom_cnt INTEGER,
    is_super_host INTEGER,  -- 0/1
    longterm_discount_per INTEGER,  -- 장기할인 %
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

CREATE INDEX IF NOT EXISTS idx_rooms_province ON rooms(province);
CREATE INDEX IF NOT EXISTS idx_rooms_town ON rooms(province, town);
CREATE INDEX IF NOT EXISTS idx_rooms_type ON rooms(property_type);
```

### 이어쓰기(Resume) 전략

- 수집 시작 시 `collections` 테이블에 새 행 삽입 (`status='in_progress'`)
- 각 구 수집 완료 시 진행상태를 별도 `collection_progress` 테이블에 기록:

```sql
CREATE TABLE IF NOT EXISTS collection_progress (
    collected_id INTEGER NOT NULL,
    province TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (collected_id, province),
    FOREIGN KEY (collected_id) REFERENCES collections(id)
);
```

- 수집 스크립트 재시작 시: 가장 최근 `in_progress` collection을 찾고, `collection_progress`에서 이미 완료된 구를 건너뜀
- 전체 완료 시 `collections.status = 'completed'`로 업데이트

### 시계열 호환성

- 날짜가 다른 시점에 다시 수집하면 새 `collected_id`로 별도 저장
- 동일 rid라도 `collected_id`가 다르면 별도 행 → 시점별 비교 가능
- 대시보드에서 수집 시점 선택 드롭다운 제공

### Rate Limiting

- API 호출 간 0.5~1초 딜레이
- 페이지 순회 시 0.3초 딜레이
- 실패 시 3회 재시도 (exponential backoff)

## 대시보드 구조

Streamlit 단일 앱, 3단계 드릴다운 탐색.

### 공통 요소

- **사이드바**: 수집 시점 선택 드롭다운, 건물유형 필터 (멀티셀렉트)
- **브레드크럼**: 현재 위치 표시 + 뒤로가기 (예: `서울 > 강남구 > 역삼동`)

### Level 1 — 서울 전체 개요

상단 KPI 카드:
- 총 매물 수
- 평균 주간 임대료
- 평균 관리비
- 슈퍼호스트 비율

차트:
- 구별 매물 수 (가로 막대 차트, 클릭 시 Level 2로)
- 구별 평균 임대료 (가로 막대 차트)
- 건물유형 분포 (파이 차트)
- 임대료 분포 (히스토그램)

### Level 2 — 구 상세

상단 KPI 카드:
- 해당 구 매물 수
- 평균 임대료
- 건물유형 수

차트:
- 동별 매물 수 (가로 막대 차트, 클릭 시 Level 3로)
- 건물유형별 평균 임대료 (가로 막대 차트)

### Level 3 — 동 상세

- 개별 매물 테이블 (정렬 가능: 임대료, 평수, 방수)
- 컬럼: 매물명, 유형, 평수, 임대료, 관리비, 방수, 슈퍼호스트, 장기할인율

## 프로젝트 구조

```
edinburgh/
├── collector.py          # 데이터 수집 스크립트
├── app.py                # Streamlit 대시보드
├── requirements.txt      # Python 의존성
├── data/
│   └── rooms.db          # SQLite 데이터베이스
├── .context/
│   └── site-analysis.md  # API 분석 문서
└── docs/
    └── superpowers/specs/
        └── 2026-04-01-33m2-dashboard-design.md
```

## 실행 방법

```bash
# 의존성 설치
pip install -r requirements.txt

# 데이터 수집
python collector.py

# 대시보드 실행
streamlit run app.py
```

## 범위 외 (Out of Scope)

- 예약율/가동률 추정 (API에서 제공하지 않음)
- 수익 추정 모델
- 외부 데이터 결합 (공공데이터, 실거래가 등)
- 사용자 인증/로그인
- 데이터 자동 갱신 스케줄링 (cron 등)
- 지도 시각화 (Naver/Kakao Map 연동)
