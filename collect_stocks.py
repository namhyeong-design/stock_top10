"""
collect_stocks.py
KOSPI + KOSDAQ 거래대금 TOP10 / 거래량 TOP10 / 상승률 TOP10 수집 → Supabase upsert
pykrx==1.0.47 기준 (인증 불필요 버전)
"""

import os
import sys
import datetime
import pandas as pd
from pykrx import stock
from supabase import create_client, Client

# ── Supabase 연결 ──────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 환경변수 SUPABASE_URL / SUPABASE_KEY 가 설정되지 않았습니다.")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── 수집 날짜 결정 ──────────────────────────────────────────────────────────────
def get_trading_date() -> str:
    """가장 최근 영업일 날짜(YYYYMMDD)를 반환."""
    today = datetime.date.today()
    for offset in range(10):
        candidate = (today - datetime.timedelta(days=offset)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_ticker(candidate, market="KOSPI")
            if df is not None and not df.empty:
                print(f"  영업일 확인: {candidate}")
                return candidate
        except Exception as e:
            print(f"  {candidate} 조회 실패: {e}")
            continue
    raise RuntimeError("최근 10일 이내 거래일 데이터를 찾을 수 없습니다.")


# ── 시장별 OHLCV 조회 ──────────────────────────────────────────────────────────
def fetch_market_df(date: str, market: str) -> pd.DataFrame:
    """pykrx 1.0.47 기준 OHLCV 데이터 조회."""
    try:
        df = stock.get_market_ohlcv_by_ticker(date, market=market)
    except Exception as e:
        print(f"  ⚠️  {market} 조회 오류: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        print(f"  ⚠️  {market}: 데이터 없음")
        return pd.DataFrame()

    # 디버그: 실제 컬럼 확인
    print(f"  {market} 컬럼: {list(df.columns)}")

    df = df.reset_index()
    df.columns.name = None

    # pykrx 1.0.47 컬럼명: 티커, 시가, 고가, 저가, 종가, 거래량, 거래대금, 등락률
    rename_map = {
        "티커":    "ticker",
        "시가":    "open",
        "고가":    "high",
        "저가":    "low",
        "종가":    "close",
        "거래량":  "volume",
        "거래대금": "trading_value",
        "등락률":  "change_rate",
    }
    df.rename(columns=rename_map, inplace=True)

    # index 컬럼(티커)이 'index' 또는 'Ticker'로 들어오는 경우 대응
    if "ticker" not in df.columns:
        for col in df.columns:
            if col.lower() in ("index", "ticker", "종목코드"):
                df.rename(columns={col: "ticker"}, inplace=True)
                break

    # 종목명 추가
    def safe_name(t: str) -> str:
        try:
            return stock.get_market_ticker_name(t) or t
        except Exception:
            return t

    df["name"] = df["ticker"].apply(safe_name)
    df["market"] = market

    # 숫자형 변환
    for col in ["close", "change_rate", "volume", "trading_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    return df


# ── TOP 10 추출 ────────────────────────────────────────────────────────────────
def get_top10(df: pd.DataFrame, sort_col: str, ascending: bool = False) -> pd.DataFrame:
    if df.empty or sort_col not in df.columns:
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

def safe_float(val, decimals=2):
    try:
        return round(float(val), decimals) if pd.notna(val) else None
    except Exception:
        return None


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("=" * 50)
    print("▶ 주식 데이터 수집 시작")

    date_str = get_trading_date()
    date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    print(f"▶ 수집 날짜: {date_formatted}")

    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fetch_market_df(date_str, market)
        if not df.empty:
            frames.append(df)
            print(f"  {market}: {len(df)}개 종목 수집 완료")

    if not frames:
        print("❌ 수집된 데이터가 없습니다.")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    print(f"▶ 통합 종목 수: {len(combined)}개")

    categories = {
        "trading_value":  ("trading_value", False),
        "trading_volume": ("volume", False),
        "top_rise":       ("change_rate", False),
    }

    all_rows: list = []

    for category, (sort_col, asc) in categories.items():
        top10 = get_top10(combined, sort_col, ascending=asc)
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
        sys.exit(1)

    upsert_rows(all_rows)
    print(f"✅ 완료: 총 {len(all_rows)}개 행 저장 ({date_formatted})")
    print("=" * 50)


if __name__ == "__main__":
    main()
