"""
collect_stocks.py
KOSPI + KOSDAQ 거래대금 TOP10 / 거래량 TOP10 / 상승률 TOP10 수집 → Supabase upsert
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


# ── 컬럼명 정규화 ───────────────────────────────────────────────────────────────
# pykrx 버전에 따라 한글 또는 영문 컬럼명이 혼재하므로 양쪽 모두 대응
COLUMN_ALIASES = {
    # 한글 → 내부 표준명
    "티커":   "ticker",
    "시가":   "open",
    "고가":   "high",
    "저가":   "low",
    "종가":   "close",
    "거래량": "volume",
    "거래대금": "trading_value",
    "등락률": "change_rate",
    "등락률(%)": "change_rate",
    # 영문(혹시 있을 경우)
    "open":  "open",
    "high":  "high",
    "low":   "low",
    "close": "close",
    "volume": "volume",
    "amount": "trading_value",
    "changes": "change",
    "changescode": "changes_code",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼명을 내부 표준명으로 변환."""
    df = df.reset_index()
    df.columns.name = None
    new_cols = []
    for c in df.columns:
        mapped = COLUMN_ALIASES.get(c, COLUMN_ALIASES.get(c.lower(), c.lower()))
        new_cols.append(mapped)
    df.columns = new_cols
    return df


# ── 시장별 OHLCV 조회 ──────────────────────────────────────────────────────────
def fetch_market_df(date: str, market: str) -> pd.DataFrame:
    """지정 시장의 OHLCV 데이터를 가져와 정리."""
    try:
        df = stock.get_market_ohlcv_by_ticker(date, market=market)
    except Exception as e:
        print(f"  ⚠️  {market} 조회 오류: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        print(f"  ⚠️  {market}: 데이터 없음")
        return pd.DataFrame()

    # 디버그: 실제 컬럼명 출력 (트러블슈팅용)
    print(f"  {market} 원본 컬럼: {list(df.columns)}")

    df = normalize_columns(df)
    print(f"  {market} 정규화 컬럼: {list(df.columns)}")

    # change_rate 컬럼이 없으면 pykrx 별도 API로 보완
    if "change_rate" not in df.columns:
        print(f"  {market}: change_rate 없음 → fundamental 데이터로 보완 시도")
        try:
            df_fund = stock.get_market_ohlcv_by_ticker(date, market=market)
            # 등락률 재시도: get_market_cap 등에서 가져오는 방법
        except Exception:
            pass

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
            df[col] = None  # 컬럼 없으면 None으로 채움

    return df


# ── change_rate 보완 (get_market_price_change_by_ticker) ───────────────────────
def enrich_change_rate(df: pd.DataFrame, date: str, market: str) -> pd.DataFrame:
    """change_rate가 전부 None이면 별도 API로 보완."""
    if df.empty or df["change_rate"].notna().any():
        return df
    print(f"  {market}: change_rate 보완 시도 (get_market_price_change_by_ticker)")
    try:
        df_ch = stock.get_market_price_change_by_ticker(date, date, market=market)
        df_ch = df_ch.reset_index()
        df_ch.columns.name = None
        # 컬럼명 정규화
        for col in df_ch.columns:
            if "등락률" in col or "change" in col.lower():
                df_ch = df_ch.rename(columns={col: "change_rate_fill"})
                break
        if "change_rate_fill" in df_ch.columns and "ticker" in df_ch.columns:
            df_ch["change_rate_fill"] = pd.to_numeric(df_ch["change_rate_fill"], errors="coerce")
            df = df.merge(
                df_ch[["ticker", "change_rate_fill"]],
                on="ticker", how="left"
            )
            df["change_rate"] = df["change_rate"].combine_first(df["change_rate_fill"])
            df.drop(columns=["change_rate_fill"], inplace=True)
    except Exception as e:
        print(f"  ⚠️  change_rate 보완 실패: {e}")
    return df


# ── TOP 10 추출 ────────────────────────────────────────────────────────────────
def get_top10(df: pd.DataFrame, sort_col: str, ascending: bool = False) -> pd.DataFrame:
    if df.empty or sort_col not in df.columns:
        print(f"  ⚠️  {sort_col} 컬럼 없음, 건너뜀")
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


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("=" * 50)
    print("▶ 주식 데이터 수집 시작")

    date_str = get_trading_date()
    date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    print(f"▶ 수집 날짜: {date_formatted}")

    # KOSPI + KOSDAQ 통합
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fetch_market_df(date_str, market)
        if not df.empty:
            df = enrich_change_rate(df, date_str, market)
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
            def safe_int(val):
                try:
                    return int(val) if pd.notna(val) else None
                except Exception:
                    return None

            def safe_float(val):
                try:
                    return round(float(val), 2) if pd.notna(val) else None
                except Exception:
                    return None

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
