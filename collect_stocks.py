"""
collect_stocks.py
KOSPI + KOSDAQ 거래대금 TOP10 / 거래량 TOP10 / 상승률 TOP10 수집 → Supabase upsert
"""

import os
import datetime
import pandas as pd
from pykrx import stock
from supabase import create_client, Client

# ── Supabase 연결 ──────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── 수집 날짜 결정 (장 마감 후 실행이므로 오늘 날짜 사용) ──────────────────────
def get_trading_date() -> str:
    """오늘 날짜를 반환. 주말/공휴일에 실행될 경우를 대비해 가장 최근 영업일을 사용."""
    today = datetime.date.today()
    date_str = today.strftime("%Y%m%d")
    # pykrx: 해당 날짜 OHLCV를 조회해 데이터가 없으면 전날로 후퇴
    for offset in range(7):
        candidate = (today - datetime.timedelta(days=offset)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_ticker(candidate, market="KOSPI")
            if not df.empty:
                return candidate
        except Exception:
            continue
    raise RuntimeError("최근 7일 이내 거래일 데이터를 찾을 수 없습니다.")


# ── 시장별 OHLCV + 거래대금 조회 ──────────────────────────────────────────────
def fetch_market_df(date: str, market: str) -> pd.DataFrame:
    """지정 시장의 OHLCV 데이터를 가져와 정리."""
    df = stock.get_market_ohlcv_by_ticker(date, market=market)
    if df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    df.columns.name = None

    # 컬럼명 통일 (pykrx 버전에 따라 한글/영문 혼재)
    rename_map = {
        "티커": "ticker", "시가": "open", "고가": "high", "저가": "low",
        "종가": "close", "거래량": "volume", "거래대금": "trading_value",
        "등락률": "change_rate",
    }
    df.rename(columns=rename_map, inplace=True)

    # 영문 컬럼명도 있을 수 있으므로 소문자 통일
    df.columns = [c.lower() for c in df.columns]

    # 종목명 추가
    df["name"] = df["ticker"].apply(
        lambda t: stock.get_market_ticker_name(t) or t
    )
    df["market"] = market

    # 숫자형 보장
    for col in ["close", "change_rate", "volume", "trading_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ── TOP 10 추출 ────────────────────────────────────────────────────────────────
def get_top10(df: pd.DataFrame, sort_col: str, ascending: bool = False) -> pd.DataFrame:
    if df.empty or sort_col not in df.columns:
        return pd.DataFrame()
    return (
        df.dropna(subset=[sort_col])
        .sort_values(sort_col, ascending=ascending)
        .head(10)
        .reset_index(drop=True)
    )


# ── Supabase upsert ────────────────────────────────────────────────────────────
def upsert_rows(rows: list[dict]) -> None:
    if not rows:
        return
    supabase.table("daily_stock_rankings").upsert(
        rows, on_conflict="date,category,rank"
    ).execute()


# ── 메인 ──────────────────────────────────────────────────────────────────────
def main() -> None:
    date_str = get_trading_date()
    date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"  # YYYY-MM-DD
    print(f"▶ 수집 날짜: {date_formatted}")

    # KOSPI + KOSDAQ 통합
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fetch_market_df(date_str, market)
        if not df.empty:
            frames.append(df)
            print(f"  {market}: {len(df)}개 종목 조회")

    if not frames:
        raise RuntimeError("수집된 데이터가 없습니다.")

    combined = pd.concat(frames, ignore_index=True)

    categories = {
        "trading_value": ("trading_value", False),  # 거래대금 TOP10
        "trading_volume": ("volume", False),         # 거래량 TOP10
        "top_rise": ("change_rate", False),          # 상승률 TOP10
    }

    all_rows: list[dict] = []

    for category, (sort_col, asc) in categories.items():
        top10 = get_top10(combined, sort_col, ascending=asc)
        for rank, row in enumerate(top10.itertuples(), start=1):
            record = {
                "date": date_formatted,
                "category": category,
                "rank": rank,
                "ticker": str(row.ticker),
                "name": str(row.name),
                "market": str(row.market),
                "close": int(row.close) if pd.notna(row.close) else None,
                "change_rate": round(float(row.change_rate), 2) if pd.notna(row.change_rate) else None,
                "volume": int(row.volume) if pd.notna(row.volume) else None,
                "trading_value": int(row.trading_value) if pd.notna(row.trading_value) else None,
            }
            all_rows.append(record)
        print(f"  [{category}] {len(top10)}개 행 준비 완료")

    upsert_rows(all_rows)
    print(f"✅ Supabase upsert 완료: 총 {len(all_rows)}개 행 ({date_formatted})")


if __name__ == "__main__":
    main()
