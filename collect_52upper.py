"""
52주 신고가 종목 수집 → Supabase 저장.

collect_stocks.py main()에서 이미 수집한 combined DataFrame을 받아
52주 신고가 달성 종목을 판별 후 category='52_upper'로 upsert.

수집 방법:
  상승률 상위 MAX_CANDIDATES개 후보 → fdr.DataReader로 52주 최고 종가 조회
  → 오늘 종가 ≥ 52주 최고 종가이면 신고가 달성으로 판정 → TOP10 저장

환경변수: collect_stocks.py와 동일한 HIT_UPPER_SUPABASE_URL/KEY 사용
"""

import datetime
import logging
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
from supabase import Client

logger = logging.getLogger(__name__)

_TABLE = "daily_stock_rankings"
_CATEGORY = "52_upper"
_TOP_N = 10
_MAX_CANDIDATES = 50  # 52주 고가 확인 최대 후보 수


def _is_52week_high(ticker: str, today_close: float, today: datetime.date) -> bool:
    """오늘 종가가 직전 52주 최고 종가 이상이면 True."""
    one_year_ago = today - datetime.timedelta(days=365)
    prev_day = today - datetime.timedelta(days=1)
    try:
        hist = fdr.DataReader(
            ticker,
            one_year_ago.strftime("%Y-%m-%d"),
            prev_day.strftime("%Y-%m-%d"),
        )
        if hist is None or hist.empty:
            return False
        return today_close >= float(hist["Close"].max())
    except Exception as e:
        logger.debug("[52_UPPER] %s 이력 조회 실패: %s", ticker, e)
        return False


def collect_and_save_52upper(
    combined_df: pd.DataFrame,
    client: Client,
    date_str: str,
) -> None:
    """
    이미 수집·정규화된 combined_df(스팩·ETF 제외)로부터
    52주 신고가 TOP10을 추출해 Supabase에 저장.

    Args:
        combined_df : collect_stocks.fetch_market_df() 결과 (ticker, name, close,
                      change_rate, market 컬럼 포함, 스팩·ETF 이미 제외됨)
        client      : Supabase Client (collect_stocks의 global supabase)
        date_str    : "YYYY-MM-DD" 형식 수집 날짜
    """
    today = datetime.date.fromisoformat(date_str)

    rising = (
        combined_df[combined_df["change_rate"].fillna(0) > 0]
        .sort_values("change_rate", ascending=False)
        .head(_MAX_CANDIDATES)
    )
    logger.info("[52_UPPER] 신고가 확인 후보: %d개", len(rising))

    confirmed: list[dict] = []
    for _, row in rising.iterrows():
        ticker = str(row.get("ticker", "")).strip()
        close_val = row.get("close")
        rate_val = row.get("change_rate")

        if not ticker or close_val is None or pd.isna(close_val):
            continue

        today_close = float(close_val)
        if _is_52week_high(ticker, today_close, today):
            confirmed.append({
                "ticker":      ticker,
                "name":        str(row.get("name", "")),
                "market":      str(row.get("market", "")),
                "close":       int(today_close),
                "change_rate": round(float(rate_val), 2) if rate_val is not None else 0.0,
            })
            logger.info(
                "[52_UPPER] 신고가: %s (%s) +%.2f%%",
                confirmed[-1]["name"], ticker, confirmed[-1]["change_rate"],
            )

        if len(confirmed) >= _TOP_N:
            break

    if not confirmed:
        logger.warning("[52_UPPER] 오늘 52주 신고가 달성 종목 없음")
        return

    rows = [
        {
            "date":          date_str,
            "category":      _CATEGORY,
            "rank":          rank,
            "ticker":        s["ticker"],
            "name":          s["name"],
            "market":        s["market"],
            "close":         s["close"],
            "change_rate":   s["change_rate"],
            "volume":        None,
            "trading_value": None,
        }
        for rank, s in enumerate(confirmed, start=1)
    ]

    result = client.table(_TABLE).upsert(
        rows, on_conflict="date,category,rank"
    ).execute()
    logger.info("[52_UPPER] Supabase upsert: %d개 저장", len(result.data))


# ── 단독 실행용 ───────────────────────────────────────────────────────────────

def _standalone_main() -> None:
    """collect_stocks.py 없이 단독으로 실행할 때만 사용."""
    import os
    from dotenv import load_dotenv
    from supabase import create_client

    _here = Path(__file__).parent
    for _p in [_here] + list(_here.parents):
        if (_p / ".env").exists():
            load_dotenv(_p / ".env")
            break

    logging.basicConfig(level=logging.INFO)

    url = os.environ.get("HIT_UPPER_SUPABASE_URL", "").strip()
    key = os.environ.get("HIT_UPPER_SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        logger.error("[52_UPPER] HIT_UPPER_SUPABASE_URL / KEY 미설정")
        return

    sb = create_client(url, key)

    # 임시로 전체 listing 수집
    from collect_stocks import fetch_market_df, _is_excluded, get_trading_date

    date_str = get_trading_date()
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fetch_market_df(market)
        if not df.empty:
            frames.append(df)

    if not frames:
        logger.error("[52_UPPER] 시세 데이터 없음")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[~combined["name"].apply(_is_excluded)]

    collect_and_save_52upper(combined, sb, date_str)


if __name__ == "__main__":
    _standalone_main()
