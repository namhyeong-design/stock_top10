"""
collect_stocks.py
FinanceDataReader 기반 KOSPI + KOSDAQ 주식 데이터 수집
- 거래대금 TOP10 / 거래량 TOP10 / 상승률 TOP10 → Supabase upsert
"""

import os
import sys
import datetime
from pathlib import Path
import pandas as pd
import FinanceDataReader as fdr
from supabase import create_client, Client

# .env 자동 로드 (부모 디렉터리 우선)
try:
    from dotenv import load_dotenv
    _here = Path(__file__).parent
    _env = _here.parent / ".env"
    load_dotenv(_env if _env.exists() else _here / ".env")
except ImportError:
    pass

supabase: Client | None = None

# ── 영문 → 한글 컬럼명 매핑 (FinanceDataReader 기준) ─────────────────────────
# fdr.StockListing 반환 컬럼: Symbol, Name, Close, Changes, ChangesRatio,
#                              Open, High, Low, Volume, Amount, Marcap 등
COLUMN_EN_TO_KR = {
    "Symbol":        "티커",
    "Name":          "종목명",
    "Open":          "시가",
    "High":          "고가",
    "Low":           "저가",
    "Close":         "종가",
    "Volume":        "거래량",
    "Amount":        "거래대금",
    "Changes":       "등락",
    "ChangesRatio":  "등락률",
    "ChagesRatio":   "등락률",   # fdr 오타 컬럼명 대응
    "Marcap":        "시가총액",
    "Stocks":        "상장주식수",
    "Market":        "시장구분",
}

# 내부 처리용 표준 컬럼명 (한글 → 내부명)
COLUMN_KR_TO_INTERNAL = {
    "티커":    "ticker",
    "종목명":  "name",
    "시가":    "open",
    "고가":    "high",
    "저가":    "low",
    "종가":    "close",
    "거래량":  "volume",
    "거래대금": "trading_value",
    "등락":    "change",
    "등락률":  "change_rate",
    "시가총액": "marcap",
    "상장주식수": "stocks",
    "시장구분": "market_label",
}


def rename_to_korean(df: pd.DataFrame) -> pd.DataFrame:
    """영문 컬럼명 → 한글 컬럼명 변환"""
    return df.rename(columns=COLUMN_EN_TO_KR)


def rename_to_internal(df: pd.DataFrame) -> pd.DataFrame:
    """한글 컬럼명 → 내부 표준명 변환"""
    return df.rename(columns=COLUMN_KR_TO_INTERNAL)


# ── 수집 날짜 결정 ──────────────────────────────────────────────────────────────
def get_trading_date() -> str:
    """
    가장 최근 영업일 날짜(YYYY-MM-DD) 반환.
    주말이면 직전 금요일로 후퇴.
    """
    today = datetime.date.today()
    weekday = today.weekday()  # 0=월 … 6=일
    if weekday == 5:           # 토요일 → 금요일
        today -= datetime.timedelta(days=1)
    elif weekday == 6:         # 일요일 → 금요일
        today -= datetime.timedelta(days=2)
    return today.strftime("%Y-%m-%d")


# ── 시장별 데이터 조회 ──────────────────────────────────────────────────────────
def fetch_market_df(market: str) -> pd.DataFrame:
    """
    fdr.StockListing(market) 으로 전 종목 시세 조회 후 정규화.
    market: 'KOSPI' 또는 'KOSDAQ'
    """
    print(f"  {market} 조회 중...")
    try:
        df = fdr.StockListing(market)
    except Exception as e:
        print(f"  ⚠️  {market} 조회 실패: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        print(f"  ⚠️  {market}: 데이터 없음")
        return pd.DataFrame()

    # 디버그: 원본 컬럼 확인
    print(f"  {market} 원본 컬럼: {list(df.columns)}")

    # 인덱스가 Symbol인 경우 컬럼으로 내림
    df = df.reset_index()
    if "index" in df.columns:
        df.drop(columns=["index"], inplace=True)

    # 영문 → 한글 → 내부 표준명 순으로 변환
    df = rename_to_korean(df)
    df = rename_to_internal(df)

    print(f"  {market} 정규화 컬럼: {list(df.columns)}")

    # ticker 컬럼 확보 (Symbol이 index였던 경우 대응)
    if "ticker" not in df.columns:
        for col in df.columns:
            if col.lower() in ("symbol", "code", "종목코드"):
                df.rename(columns={col: "ticker"}, inplace=True)
                break

    # 시장 구분 고정
    df["market"] = market

    # 숫자형 강제 변환
    for col in ["close", "change_rate", "volume", "trading_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    # 거래량/거래대금이 0이거나 NaN인 종목 제거 (미거래 종목)
    df = df[df["volume"].fillna(0) > 0]

    # ── 등락률 보완 ────────────────────────────────────────────────────────────
    # fdr.StockListing 이 ChangesRatio 를 제공하지 않거나 전부 NaN인 경우,
    # Changes(등락 금액)와 Close(종가)로 직접 계산: 등락률 = Changes / (Close - Changes) * 100
    if "change_rate" not in df.columns or df["change_rate"].isna().all():
        print(f"  {market}: change_rate 없음 → Changes/Close 로 직접 계산")
        if "change" in df.columns and "close" in df.columns:
            prev_close = df["close"] - df["change"]
            df["change_rate"] = (df["change"] / prev_close.replace(0, float("nan")) * 100).round(2)
        else:
            print(f"  ⚠️  {market}: 등락률 계산 불가 (change/close 컬럼 없음)")
            df["change_rate"] = None

    print(f"  {market}: {len(df)}개 종목 (거래 있음)")
    return df


# ── 제외 필터 (스팩 / ETF·ETN / 레버리지·인버스·선물·신주인수권) ──────────────
_ETF_PREFIXES = (
    "KODEX", "TIGER", "ARIRANG", "HANARO", "KBSTAR", "ACE", "PLUS",
    "SOL", "RISE", "KOSEF", "TIMEFOLIO", "KINDEX", "SMART", "FOCUS",
    "TRUSTON", "BNK", "WOORI", "KTOP", "마이티", "파워",
)

# 이름 내 포함 여부로 판단하는 제외 키워드 (대소문자 무관)
_EXCLUDED_KEYWORDS = ("스팩", "ETN", "레버리지", "인버스", "선물", "신주인수권")


def _is_excluded(name: str) -> bool:
    if not isinstance(name, str):
        return False
    u = name.upper()
    if any(kw.upper() in u for kw in _EXCLUDED_KEYWORDS):
        return True
    return any(u.startswith(p) for p in _ETF_PREFIXES)


# ── TOP 10 추출 ────────────────────────────────────────────────────────────────
def get_top10(df: pd.DataFrame, sort_col: str, ascending: bool = False) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    if sort_col not in df.columns:
        print(f"  ⚠️  '{sort_col}' 컬럼 없음 → 건너뜀")
        return pd.DataFrame()
    return (
        df.dropna(subset=[sort_col])
        .sort_values(sort_col, ascending=ascending)
        .head(10)
        .reset_index(drop=True)
    )


# ── Supabase upsert ────────────────────────────────────────────────────────────
def upsert_rows(rows: list) -> None:
    if not rows:
        print("  ⚠️  upsert할 행이 없습니다.")
        return
    result = supabase.table("daily_stock_rankings").upsert(
        rows, on_conflict="date,category,rank"
    ).execute()
    print(f"  Supabase 응답: {len(result.data)}개 처리됨")


# ── 헬퍼 ──────────────────────────────────────────────────────────────────────
def safe_int(val):
    try:
        return int(val) if pd.notna(val) else None
    except Exception:
        return None

def safe_float(val, decimals: int = 2):
    try:
        return round(float(val), decimals) if pd.notna(val) else None
    except Exception:
        return None


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main() -> None:
    global supabase
    supabase_url = os.environ.get("HIT_UPPER_SUPABASE_URL", "")
    supabase_key = os.environ.get("HIT_UPPER_SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not supabase_key:
        print("HIT_UPPER_SUPABASE_URL / HIT_UPPER_SUPABASE_SERVICE_KEY 환경변수가 설정되지 않았습니다.")
        return
    if supabase is None:
        supabase = create_client(supabase_url, supabase_key)

    print("=" * 50)
    print("▶ 주식 데이터 수집 시작 (FinanceDataReader)")

    date_formatted = get_trading_date()
    print(f"▶ 수집 날짜: {date_formatted}")

    # KOSPI + KOSDAQ 통합
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fetch_market_df(market)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("❌ 수집된 데이터가 없습니다.")
        return

    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)
    combined = combined[~combined["name"].apply(_is_excluded)]
    print(f"▶ 통합 종목 수: {len(combined)}개 (스팩·ETF·ETN {before - len(combined)}개 제외)")

    # 카테고리 정의: {저장 키: (정렬 컬럼, 오름차순 여부, 필터 DataFrame 또는 None)}
    # top_drop은 하락 종목(change_rate < 0)만 대상으로 오름차순 정렬
    dropping = combined[combined["change_rate"].fillna(0) < 0]
    print(f"▶ 하락 종목 수 (change_rate < 0, 스팩·ETF·ETN 제외): {len(dropping)}개")
    categories = [
        ("trading_value",  combined,  "trading_value", False),   # 거래대금 TOP10
        ("trading_volume", combined,  "volume",        False),   # 거래량 TOP10
        ("top_rise",       combined,  "change_rate",   False),   # 상승률 TOP10
        ("top_drop",       dropping,  "change_rate",   True),    # 하락률 TOP10
    ]

    all_rows: list = []

    for category, source_df, sort_col, asc in categories:
        top10 = get_top10(source_df, sort_col, ascending=asc)
        if top10.empty:
            continue
        for rank, row in enumerate(top10.itertuples(), start=1):
            record = {
                "date":          date_formatted,
                "category":      category,
                "rank":          rank,
                "ticker":        str(getattr(row, "ticker", "")),
                "name":          str(getattr(row, "name", "")),
                "market":        str(getattr(row, "market", "")),
                "close":         safe_int(getattr(row, "close", None)),
                "change_rate":   safe_float(getattr(row, "change_rate", None)),
                "volume":        safe_int(getattr(row, "volume", None)),
                "trading_value": safe_int(getattr(row, "trading_value", None)),
            }
            all_rows.append(record)
        print(f"  [{category}] {len(top10)}개 행 준비")

    if not all_rows:
        print("❌ 저장할 데이터가 없습니다.")
        return

    upsert_rows(all_rows)
    print(f"[DONE] 총 {len(all_rows)}개 행 저장 완료 ({date_formatted})")

    # ── 52주 신고가 수집 (같은 listing 데이터 재사용) ─────────────────────────
    try:
        from collect_52upper import collect_and_save_52upper
        collect_and_save_52upper(combined, supabase, date_formatted)
    except Exception as e:
        print(f"[52_UPPER] 수집 예외 (stock_top10 결과에는 영향 없음): {e}")

    print("=" * 50)


if __name__ == "__main__":
    main()
