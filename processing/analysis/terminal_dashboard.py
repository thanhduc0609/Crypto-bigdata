"""
=============================================================================
Crypto Big Data - Terminal Analytics & Quant Trading Dashboard (TUI)
=============================================================================
Ung dung truc quan hoa phan tich thi truong tien dien tu truc tiep tren Terminal:
  - Can can ap luc Mua/Ban (Order Flow & Trade Imbalance Gauge Bar)
  - Ma tran thi truong da khung thoi gian (OHLC, VWAP, Volatility %, Return %)
  - Bieu do Sparkline xu huong gia mini (  ▂▃▄▅▆▇█)
  - Radar theo doi giao dich Ca map (Live Whale Alert Table)
  - Thong so Thi truong Phai sinh (Funding Rate, APY %, Open Interest)
  - Che do Live Auto-Refresh (Cap nhat lien tuc moi 2 - 3 giay)
=============================================================================
"""

import os
import sys
import time
import math
import argparse
from datetime import datetime, timezone
from pathlib import Path

# Thiet lap ma hoa UTF-8 va bat ANSI Virtual Terminal tren Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
        # Bat ANSI escape codes tren cmd/PowerShell cua Windows
        os.system("")
    except Exception:
        pass

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import load_env_vars

load_env_vars()

# =============================================================================
# ANSI COLOR CODES & STYLES
# =============================================================================
C_RESET = "\033[0m"
C_BOLD = "\033[1m"
C_DIM = "\033[2m"
C_ITALIC = "\033[3m"

# Foreground Colors
C_RED = "\033[91m"
C_GREEN = "\033[92m"
C_YELLOW = "\033[93m"
C_BLUE = "\033[94m"
C_MAGENTA = "\033[95m"
C_CYAN = "\033[96m"
C_WHITE = "\033[97m"
C_GRAY = "\033[90m"

# Background Colors
BG_HEADER = "\033[44m\033[97m"  # Blue background with white text
BG_DARK = "\033[40m"


def color_text(text: str, color: str, bold: bool = False) -> str:
    prefix = C_BOLD + color if bold else color
    return f"{prefix}{text}{C_RESET}"


def sparkline(values: list, length: int = 12) -> str:
    """Tao bieu do dang sparkline unicode dua tren danh sach gia tri"""
    if not values or len(values) < 2:
        return " ─── "
    
    # Lay toi da `length` phan tu gan nhat
    vals = values[-length:]
    min_v = min(vals)
    max_v = max(vals)
    
    ticks = [" ", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
    if max_v == min_v:
        return ticks[3] * len(vals)
    
    chars = []
    for v in vals:
        ratio = (v - min_v) / (max_v - min_v)
        idx = min(int(ratio * (len(ticks) - 1)), len(ticks) - 1)
        chars.append(ticks[idx])
        
    line = "".join(chars)
    # To mau sparkline: xanh neu xu huong tang, do neu xu huong giam
    if vals[-1] >= vals[0]:
        return f"{C_GREEN}{line}{C_RESET}"
    else:
        return f"{C_RED}{line}{C_RESET}"


def make_gauge_bar(imbalance: float, width: int = 24) -> str:
    """
    Tao thanh thuoc do ap luc Mua/Ban:
    imbalance in [-1.0, +1.0]
    Tra ve thanh dang: [████████████░░░░░░░░]
    """
    # Chuan hoa ve khoang [0.0, 1.0]
    buy_ratio = max(0.0, min(1.0, (imbalance + 1.0) / 2.0))
    buy_blocks = int(round(buy_ratio * width))
    sell_blocks = width - buy_blocks
    
    buy_bar = f"{C_GREEN}{'█' * buy_blocks}{C_RESET}"
    sell_bar = f"{C_RED}{'░' * sell_blocks}{C_RESET}"
    
    # Nhan xet trang thai
    if imbalance >= 0.25:
        badge = f"{C_BOLD}{C_GREEN}🟢 MUA MẠNH{C_RESET} ({buy_ratio*100:.1f}% Buy)"
    elif imbalance <= -0.25:
        badge = f"{C_BOLD}{C_RED}🔴 BÁN THÁO{C_RESET} ({(1-buy_ratio)*100:.1f}% Sell)"
    else:
        badge = f"{C_DIM}{C_YELLOW}⚪ CÂN BẰNG{C_RESET} ({buy_ratio*100:.1f}% Buy)"
        
    sign = "+" if imbalance > 0 else ""
    return f"[{buy_bar}{sell_bar}] {sign}{imbalance:.2f} {badge}"


# =============================================================================
# DATA PROVIDER (PostgreSQL Data Warehouse + OKX API Fallback)
# =============================================================================
class MarketDataProvider:
    def __init__(self, demo_mode: bool = False):
        self.demo_mode = demo_mode
        self.conn = None
        self.pg_host = os.getenv("POSTGRES_HOST", "127.0.0.1")
        self.pg_port = int(os.getenv("POSTGRES_PORT", "5433"))
        self.pg_db = os.getenv("POSTGRES_DB", "crypto_dw")
        self.pg_user = os.getenv("POSTGRES_USER", "postgres")
        self.pg_pass = os.getenv("POSTGRES_PASSWORD", "")
        self.okx_rest_url = os.getenv("OKX_REST_URL", "https://www.okx.com")
        self.last_derivatives_cache = {}
        self.last_derivatives_time = 0

    def connect(self):
        if self.demo_mode:
            return True
        try:
            import psycopg2
            self.conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_db,
                user=self.pg_user,
                password=self.pg_pass,
                connect_timeout=3
            )
            self.conn.autocommit = True
            return True
        except Exception:
            self.conn = None
            return False

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def get_order_flow_imbalance(self):
        """Lay ap luc mua/ban cho tung symbol"""
        if self.demo_mode or not self.conn:
            return [
                {"symbol": "BTC-USDT", "buy_vol": 142.50, "sell_vol": 45.20, "imbalance": 0.52, "ratio": 3.15},
                {"symbol": "ETH-USDT", "buy_vol": 1250.0, "sell_vol": 2100.0, "imbalance": -0.25, "ratio": 0.60},
                {"symbol": "SOL-USDT", "buy_vol": 8900.0, "sell_vol": 7800.0, "imbalance": 0.07, "ratio": 1.14},
            ]

        try:
            cur = self.conn.cursor()
            # 1. Uu tien tinh ap luc Order Flow tu cac nến 1m gan nhat (30 nến = 30 phut gan nhat)
            cur.execute("""
                WITH recent_candles AS (
                    SELECT symbol, buy_volume, sell_volume
                    FROM (
                        SELECT symbol, buy_volume, sell_volume,
                               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) as rn
                        FROM warehouse.fact_crypto_1m_candles
                    ) sub
                    WHERE rn <= 30
                )
                SELECT 
                    symbol,
                    COALESCE(SUM(buy_volume), 0) AS total_buy_vol,
                    COALESCE(SUM(sell_volume), 0) AS total_sell_vol,
                    ROUND(COALESCE(SUM(buy_volume), 0) / NULLIF(COALESCE(SUM(sell_volume), 0), 0), 3) AS ratio,
                    ROUND((COALESCE(SUM(buy_volume), 0) - COALESCE(SUM(sell_volume), 0)) / 
                          NULLIF(COALESCE(SUM(buy_volume), 0) + COALESCE(SUM(sell_volume), 0), 0), 4) AS imbalance
                FROM recent_candles
                GROUP BY symbol;
            """)
            rows = cur.fetchall()

            if rows:
                cur.close()
                results = []
                for r in rows:
                    results.append({
                        "symbol": r[0],
                        "buy_vol": float(r[1] or 0),
                        "sell_vol": float(r[2] or 0),
                        "ratio": float(r[3] or 1.0),
                        "imbalance": float(r[4] or 0.0)
                    })
                return results

            # 2. Fallback: Kiem tra view vw_order_flow_pressure
            cur.execute("""
                SELECT symbol, total_buy_volume, total_sell_volume, buy_to_sell_ratio, net_order_imbalance
                FROM warehouse.vw_order_flow_pressure;
            """)
            rows = cur.fetchall()

            if rows:
                cur.close()
                results = []
                for r in rows:
                    results.append({
                        "symbol": r[0],
                        "buy_vol": float(r[1] or 0),
                        "sell_vol": float(r[2] or 0),
                        "ratio": float(r[3] or 1.0),
                        "imbalance": float(r[4] or 0.0)
                    })
                return results

            # 3. Fallback: Tinh tu fact_crypto_trades neu chua co nến
            cur.execute("""
                SELECT 
                    symbol,
                    COALESCE(SUM(CASE WHEN side = 'buy' THEN size ELSE 0 END), 0) AS buy_vol,
                    COALESCE(SUM(CASE WHEN side = 'sell' THEN size ELSE 0 END), 0) AS sell_vol
                FROM warehouse.fact_crypto_trades
                GROUP BY symbol;
            """)
            rows = cur.fetchall()
            cur.close()

            results = []
            for r in rows:
                sym, b_vol, s_vol = r[0], float(r[1]), float(r[2])
                tot = b_vol + s_vol
                imb = (b_vol - s_vol) / tot if tot > 0 else 0.0
                rat = b_vol / s_vol if s_vol > 0 else 1.0
                results.append({
                    "symbol": sym,
                    "buy_vol": b_vol,
                    "sell_vol": s_vol,
                    "imbalance": imb,
                    "ratio": rat
                })
            return results or self.get_order_flow_imbalance_demo()
        except Exception:
            return self.get_order_flow_imbalance_demo()

    def get_order_flow_imbalance_demo(self):
        return [
            {"symbol": "BTC-USDT", "buy_vol": 85.4, "sell_vol": 32.1, "imbalance": 0.45, "ratio": 2.66},
            {"symbol": "ETH-USDT", "buy_vol": 450.2, "sell_vol": 620.0, "imbalance": -0.16, "ratio": 0.73},
            {"symbol": "SOL-USDT", "buy_vol": 3200.0, "sell_vol": 2900.0, "imbalance": 0.05, "ratio": 1.10},
        ]

    def get_market_overview(self):
        """Lay bang ma tran tong quan thi truong (OHLC, VWAP, Volatility, Returns)"""
        if self.demo_mode or not self.conn:
            return [
                {
                    "symbol": "BTC-USDT", "last_price": 68450.50, "open": 67120.00, "high": 68900.00,
                    "low": 66950.00, "vwap": 68120.25, "volume": 521.80, "turnover": 35680000.0,
                    "return_pct": 1.98, "volatility_pct": 2.91,
                    "history": [67120, 67300, 67250, 67600, 67800, 67700, 68100, 68350, 68450]
                },
                {
                    "symbol": "ETH-USDT", "last_price": 2425.80, "open": 2465.00, "high": 2480.00,
                    "low": 2410.00, "vwap": 2442.10, "volume": 4120.50, "turnover": 10060000.0,
                    "return_pct": -1.59, "volatility_pct": 2.90,
                    "history": [2465, 2460, 2450, 2445, 2435, 2430, 2420, 2428, 2425]
                },
                {
                    "symbol": "SOL-USDT", "last_price": 149.20, "open": 145.50, "high": 151.30,
                    "low": 144.80, "vwap": 148.60, "volume": 32800.0, "turnover": 4870000.0,
                    "return_pct": 2.54, "volatility_pct": 4.49,
                    "history": [145.5, 146.2, 145.8, 147.0, 148.2, 147.9, 149.5, 148.8, 149.2]
                }
            ]

        try:
            cur = self.conn.cursor()
            # 1. Uu tien doc tu bang fact_crypto_1m_candles (toi da 60 nến gan nhat ~ 1 gio)
            cur.execute("""
                WITH recent_candles AS (
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) as rn
                        FROM warehouse.fact_crypto_1m_candles
                    ) sub
                    WHERE rn <= 60
                )
                SELECT 
                    symbol,
                    (array_agg(close ORDER BY window_start DESC))[1] AS last_price,
                    (array_agg(open ORDER BY window_start ASC))[1] AS open_price,
                    MAX(high) AS high_price,
                    MIN(low) AS low_price,
                    SUM(volume) AS total_vol,
                    SUM(total_trade_value) AS total_val,
                    ROUND(SUM(total_trade_value) / NULLIF(SUM(volume), 0), 2) AS vwap
                FROM recent_candles
                GROUP BY symbol;
            """)
            rows = cur.fetchall()

            # 2. Neu bang nến 1m chua co, fallback doc tu fact_crypto_trades
            is_from_candles = True
            if not rows:
                is_from_candles = False
                cur.execute("""
                    SELECT 
                        symbol,
                        (array_agg(price ORDER BY trade_time DESC))[1] AS last_price,
                        (array_agg(price ORDER BY trade_time ASC))[1] AS open_price,
                        MAX(price) AS high_price,
                        MIN(price) AS low_price,
                        SUM(size) AS total_vol,
                        SUM(trade_value) AS total_val,
                        ROUND(SUM(trade_value) / NULLIF(SUM(size), 0), 2) AS vwap
                    FROM warehouse.fact_crypto_trades
                    GROUP BY symbol;
                """)
                rows = cur.fetchall()
            cur.close()

            results = []
            for r in rows:
                sym = r[0]
                last_p = float(r[1] or 0)
                open_p = float(r[2] or last_p)
                high_p = float(r[3] or last_p)
                low_p = float(r[4] or last_p)
                vol = float(r[5] or 0)
                val = float(r[6] or 0)
                vwap = float(r[7] or last_p)
                ret = ((last_p - open_p) / open_p * 100) if open_p > 0 else 0.0
                volat = ((high_p - low_p) / low_p * 100) if low_p > 0 else 0.0

                # Lay 10 gia gan nhat cho sparkline
                cur = self.conn.cursor()
                if is_from_candles:
                    cur.execute("""
                        SELECT close FROM warehouse.fact_crypto_1m_candles
                        WHERE symbol = %s ORDER BY window_start DESC LIMIT 12;
                    """, (sym,))
                else:
                    cur.execute("""
                        SELECT price FROM warehouse.fact_crypto_trades
                        WHERE symbol = %s ORDER BY trade_time DESC LIMIT 12;
                    """, (sym,))
                prices = [float(p[0]) for p in reversed(cur.fetchall())]
                cur.close()

                results.append({
                    "symbol": sym,
                    "last_price": last_p,
                    "open": open_p,
                    "high": high_p,
                    "low": low_p,
                    "vwap": vwap,
                    "volume": vol,
                    "turnover": val,
                    "return_pct": ret,
                    "volatility_pct": volat,
                    "history": prices or [last_p]
                })
            return results or self.get_market_overview_demo()
        except Exception:
            return self.get_market_overview_demo()

    def get_market_overview_demo(self):
        return [
            {
                "symbol": "BTC-USDT", "last_price": 68450.50, "open": 67120.00, "high": 68900.00,
                "low": 66950.00, "vwap": 68120.25, "volume": 521.80, "turnover": 35680000.0,
                "return_pct": 1.98, "volatility_pct": 2.91,
                "history": [67120, 67300, 67250, 67600, 67800, 67700, 68100, 68350, 68450]
            },
            {
                "symbol": "ETH-USDT", "last_price": 2425.80, "open": 2465.00, "high": 2480.00,
                "low": 2410.00, "vwap": 2442.10, "volume": 4120.50, "turnover": 10060000.0,
                "return_pct": -1.59, "volatility_pct": 2.90,
                "history": [2465, 2460, 2450, 2445, 2435, 2430, 2420, 2428, 2425]
            },
            {
                "symbol": "SOL-USDT", "last_price": 149.20, "open": 145.50, "high": 151.30,
                "low": 144.80, "vwap": 148.60, "volume": 32800.0, "turnover": 4870000.0,
                "return_pct": 2.54, "volatility_pct": 4.49,
                "history": [145.5, 146.2, 145.8, 147.0, 148.2, 147.9, 149.5, 148.8, 149.2]
            }
        ]

    def get_whale_radar(self, limit: int = 5):
        """Lay cac giao dich ca map gan nhat"""
        if self.demo_mode or not self.conn:
            return [
                {"time": "13:25:12", "symbol": "BTC-USDT", "side": "buy", "price": 68450.0, "size": 1.85, "val": 126632.5, "tier": "🚨 MEGA WHALE (>= $100K)"},
                {"time": "13:23:45", "symbol": "ETH-USDT", "side": "sell", "price": 2422.5, "size": 25.00, "val": 60562.5, "tier": "⚡ BIG WHALE (>= $50K)"},
                {"time": "13:21:10", "symbol": "SOL-USDT", "side": "buy", "price": 148.9, "size": 125.00, "val": 18612.5, "tier": "🐋 WHALE (>= $10K)"},
                {"time": "13:19:02", "symbol": "BTC-USDT", "side": "sell", "price": 68390.0, "size": 0.85, "val": 58131.5, "tier": "⚡ BIG WHALE (>= $50K)"},
                {"time": "13:16:30", "symbol": "BTC-USDT", "side": "buy", "price": 68410.0, "size": 0.25, "val": 17102.5, "tier": "🐋 WHALE (>= $10K)"},
            ]

        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT 
                    TO_CHAR(trade_time, 'HH24:MI:SS') AS t_time,
                    symbol,
                    side,
                    price,
                    size,
                    trade_value,
                    CASE 
                        WHEN trade_value >= 2000000 THEN '🚨 MEGA WHALE (>= $2M)'
                        WHEN trade_value >= 1000000 THEN '⚡ BIG WHALE (>= $1M)'
                        WHEN trade_value >= 500000 THEN '🐋 WHALE (>= $500K)'
                        ELSE '🐟 LARGE TRADE'
                    END AS tier
                FROM warehouse.fact_crypto_trades
                WHERE trade_value >= 500000
                ORDER BY trade_time DESC, trade_value DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
            cur.close()

            results = []
            for r in rows:
                results.append({
                    "time": r[0],
                    "symbol": r[1],
                    "side": r[2],
                    "price": float(r[3]),
                    "size": float(r[4]),
                    "val": float(r[5]),
                    "tier": r[6]
                })
            return results or self.get_whale_radar_demo()
        except Exception:
            return self.get_whale_radar_demo()

    def get_whale_radar_demo(self):
        return [
            {"time": "13:25:12", "symbol": "BTC-USDT", "side": "buy", "price": 68450.0, "size": 1.85, "val": 126632.5, "tier": "🚨 MEGA WHALE (>= $100K)"},
            {"time": "13:23:45", "symbol": "ETH-USDT", "side": "sell", "price": 2422.5, "size": 25.00, "val": 60562.5, "tier": "⚡ BIG WHALE (>= $50K)"},
            {"time": "13:21:10", "symbol": "SOL-USDT", "side": "buy", "price": 148.9, "size": 125.00, "val": 18612.5, "tier": "🐋 WHALE (>= $10K)"},
        ]

    def get_derivatives_data(self):
        """Lay du lieu phai sinh (Funding Rate & Open Interest)"""
        # Cache du lieu trong 30s de khong spam API
        now = time.time()
        if self.last_derivatives_cache and (now - self.last_derivatives_time < 30):
            return self.last_derivatives_cache

        # Lay nhanh tu OKX REST API neu co mang
        try:
            import json
            import urllib.request
            results = []
            symbols = [("BTC-USDT", "BTC-USDT-SWAP"), ("ETH-USDT", "ETH-USDT-SWAP"), ("SOL-USDT", "SOL-USDT-SWAP")]
            
            for sym, inst in symbols:
                funding_url = f"{self.okx_rest_url}/api/v5/public/funding-rate?instId={inst}"
                req = urllib.request.Request(funding_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=3) as resp:
                    f_data = json.loads(resp.read().decode("utf-8")).get("data", [{}])[0]
                    fr_raw = f_data.get("fundingRate")
                    fr = float(fr_raw) if fr_raw not in (None, "") else 0.0001
                    next_raw = f_data.get("nextFundingRate")
                    next_fr = float(next_raw) if next_raw not in (None, "") else fr

                oi_url = f"{self.okx_rest_url}/api/v5/public/open-interest?instType=SWAP&instId={inst}"
                req_oi = urllib.request.Request(oi_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req_oi, timeout=3) as resp_oi:
                    oi_data = json.loads(resp_oi.read().decode("utf-8")).get("data", [{}])[0]
                    oi_raw = oi_data.get("oiCcy")
                    oi_usd = float(oi_raw) if oi_raw not in (None, "") else 150000000.0

                apy = fr * 3 * 365 * 100
                results.append({
                    "symbol": sym,
                    "funding_rate": fr,
                    "next_funding_rate": next_fr,
                    "apy": apy,
                    "oi_usd": oi_usd
                })

            self.last_derivatives_cache = results
            self.last_derivatives_time = now
            return results
        except Exception:
            # Fallback mock data
            return [
                {"symbol": "BTC-USDT", "funding_rate": 0.00010, "next_funding_rate": 0.00012, "apy": 10.95, "oi_usd": 3850000000.0},
                {"symbol": "ETH-USDT", "funding_rate": -0.00005, "next_funding_rate": -0.00002, "apy": -5.47, "oi_usd": 1420000000.0},
                {"symbol": "SOL-USDT", "funding_rate": 0.00015, "next_funding_rate": 0.00018, "apy": 16.42, "oi_usd": 680000000.0},
            ]


# =============================================================================
# DASHBOARD RENDERER
# =============================================================================
def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def render_dashboard(provider: MarketDataProvider, is_live: bool = False, loop_count: int = 1):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_status = f"{C_GREEN}● PostgreSQL Connected (Port 5433){C_RESET}" if provider.conn else f"{C_YELLOW}● Simulation Mode (Demo Data){C_RESET}"

    # Header Box
    term_width = 96
    print(f"{C_BLUE}╭{'─' * (term_width - 2)}╮{C_RESET}")
    title = "🚀 CRYPTO BIG DATA - QUANT & MARKET ANALYTICS TERMINAL"
    mode_str = f"Live Refreshed #{loop_count}" if is_live else "Single Snapshot"
    line1 = f" {C_BOLD}{C_CYAN}{title}{C_RESET} "
    line2 = f" {C_DIM}{now_str} | {mode_str}{C_RESET} "
    
    pad_len = term_width - 4 - len(title) - len(f"{now_str} | {mode_str}")
    if pad_len < 0: pad_len = 2
    print(f"{C_BLUE}│{C_RESET}{line1}{' ' * pad_len}{line2}{C_BLUE}│{C_RESET}")
    print(f"{C_BLUE}│{C_RESET}  Status: {db_status}{' ' * (term_width - 40)}{C_BLUE}│{C_RESET}")
    print(f"{C_BLUE}╰{'─' * (term_width - 2)}╯{C_RESET}\n")

    # -------------------------------------------------------------------------
    # 1. ORDER FLOW & TRADE IMBALANCE
    # -------------------------------------------------------------------------
    print(f"{C_BOLD}{C_YELLOW}📊 1. CÁN CÂN ÁP LỰC MUA / BÁN (ORDER FLOW & TRADE IMBALANCE){C_RESET}")
    print(f"{C_GRAY}{'─' * term_width}{C_RESET}")
    
    imbalance_data = provider.get_order_flow_imbalance()
    for item in imbalance_data:
        sym = item["symbol"]
        imb = item["imbalance"]
        gauge = make_gauge_bar(imb, width=22)
        print(f"  {C_BOLD}{sym:<9}{C_RESET} : {gauge}")
    print()

    # -------------------------------------------------------------------------
    # 2. MARKET MATRIX & VWAP
    # -------------------------------------------------------------------------
    print(f"{C_BOLD}{C_CYAN}📈 2. MA TRẬN THỊ TRƯỜNG & CHỈ BÁO VWAP (MARKET MATRIX){C_RESET}")
    print(f"{C_GRAY}{'─' * term_width}{C_RESET}")
    
    print(f"  {C_BOLD}{'Symbol':<9} │ {'Last Price':<12} │ {'Trend':<8} │ {'Return %':<10} │ {'Volatility':<10} │ {'VWAP':<12} │ {'Vị thế vs VWAP':<15}{C_RESET}")
    print(f"  {C_GRAY}{'─'*9}─┼─{'─'*12}─┼─{'─'*8}─┼─{'─'*10}─┼─{'─'*10}─┼─{'─'*12}─┼─{'─'*15}{C_RESET}")
    
    market_data = provider.get_market_overview()
    for m in market_data:
        sym = m["symbol"]
        last_p = f"${m['last_price']:,.2f}"
        spark = sparkline(m.get("history", []), length=8)
        
        ret_val = m["return_pct"]
        if ret_val > 0:
            ret_str = f"{C_GREEN}+{ret_val:.2f}% ▲{C_RESET}"
        elif ret_val < 0:
            ret_str = f"{C_RED}{ret_val:.2f}% ▼{C_RESET}"
        else:
            ret_str = f"{C_GRAY} 0.00% ─{C_RESET}"
            
        volat_str = f"{m['volatility_pct']:.2f}%"
        vwap_val = f"${m['vwap']:,.2f}"
        
        # Vi the so voi VWAP
        if m["last_price"] >= m["vwap"]:
            pos_str = f"{C_GREEN}🟢 TRÊN VWAP (Tốt){C_RESET}"
        else:
            pos_str = f"{C_RED}🔴 DƯỚI VWAP (Yếu){C_RESET}"
            
        print(f"  {C_BOLD}{sym:<9}{C_RESET} │ {last_p:<12} │ {spark:<17} │ {ret_str:<19} │ {volat_str:<10} │ {vwap_val:<12} │ {pos_str}")
    print()

    # -------------------------------------------------------------------------
    # 3. WHALE RADAR TABLE
    # -------------------------------------------------------------------------
    print(f"{C_BOLD}{C_MAGENTA}🐋 3. RADAR THEO DÕI GIAO DỊCH CÁ MẬP (LIVE WHALE ALERTS){C_RESET}")
    print(f"{C_GRAY}{'─' * term_width}{C_RESET}")
    
    print(f"  {C_BOLD}{'Time':<8} │ {'Symbol':<9} │ {'Lệnh':<6} │ {'Giá khớp':<12} │ {'Khối lượng':<12} │ {'Giá trị (USD)':<15} │ {'Phân cấp':<18}{C_RESET}")
    print(f"  {C_GRAY}{'─'*8}─┼─{'─'*9}─┼─{'─'*6}─┼─{'─'*12}─┼─{'─'*12}─┼─{'─'*15}─┼─{'─'*18}{C_RESET}")
    
    whales = provider.get_whale_radar(limit=5)
    for w in whales:
        t_str = w["time"]
        sym = w["symbol"]
        side_badge = f"{C_GREEN}MUA {C_RESET}" if w["side"].lower() == "buy" else f"{C_RED}BÁN {C_RESET}"
        p_str = f"${w['price']:,.2f}"
        sz_str = f"{w['size']:,.4f}"
        val_str = f"${w['val']:,.2f}"
        
        tier = w["tier"]
        if "MEGA" in tier:
            tier_badge = f"{C_BOLD}{C_YELLOW}{tier}{C_RESET}"
        elif "BIG" in tier:
            tier_badge = f"{C_BOLD}{C_CYAN}{tier}{C_RESET}"
        else:
            tier_badge = f"{C_BLUE}{tier}{C_RESET}"
            
        print(f"  {t_str:<8} │ {sym:<9} │ {side_badge:<15} │ {p_str:<12} │ {sz_str:<12} │ {C_BOLD}{val_str:<15}{C_RESET} │ {tier_badge}")
    print()

    # -------------------------------------------------------------------------
    # 4. DERIVATIVES & SENTIMENT
    # -------------------------------------------------------------------------
    print(f"{C_BOLD}{C_BLUE}🔮 4. TÂM LÝ PHÁI SINH (DERIVATIVES & SENTIMENT FEED){C_RESET}")
    print(f"{C_GRAY}{'─' * term_width}{C_RESET}")
    
    print(f"  {C_BOLD}{'Symbol':<9} │ {'Funding Rate (8h)':<18} │ {'APY Dự phóng':<14} │ {'Open Interest (OI USD)':<24} │ {'Đánh giá tâm lý':<20}{C_RESET}")
    print(f"  {C_GRAY}{'─'*9}─┼─{'─'*18}─┼─{'─'*14}─┼─{'─'*24}─┼─{'─'*20}{C_RESET}")
    
    deriv_data = provider.get_derivatives_data()
    for d in deriv_data:
        sym = d["symbol"]
        fr = d["funding_rate"]
        apy = d["apy"]
        oi_str = f"${d['oi_usd'] / 1e6:,.1f} Triệu USD" if d['oi_usd'] < 1e9 else f"${d['oi_usd'] / 1e9:,.2f} Tỷ USD"
        
        if fr > 0:
            fr_str = f"{C_GREEN}+{fr*100:.4f}% (Long trả){C_RESET}"
            apy_str = f"{C_GREEN}+{apy:.2f}%/năm{C_RESET}"
            bias = f"{C_GREEN}Bullish FOMO{C_RESET}"
        elif fr < 0:
            fr_str = f"{C_RED}{fr*100:.4f}% (Short trả){C_RESET}"
            apy_str = f"{C_RED}{apy:.2f}%/năm{C_RESET}"
            bias = f"{C_RED}Bearish FUD{C_RESET}"
        else:
            fr_str = f"{C_GRAY} 0.0000%{C_RESET}"
            apy_str = f"{C_GRAY} 0.00%{C_RESET}"
            bias = f"{C_GRAY}Neutral{C_RESET}"
            
        print(f"  {C_BOLD}{sym:<9}{C_RESET} │ {fr_str:<27} │ {apy_str:<23} │ {oi_str:<24} │ {bias}")
    print()

    # Footer
    print(f"{C_GRAY}{'─' * term_width}{C_RESET}")
    if is_live:
        print(f"  {C_DIM}[Nhấn CTRL + C để dừng Live Dashboard | Tự động làm mới mỗi 3 giây]{C_RESET}")
    else:
        print(f"  {C_DIM}[Để xem tự động cập nhật liên tục: .\\scripts\\setup_env.ps1 -Dashboard]{C_RESET}")
    print()


# =============================================================================
# MAIN ENTRYPOINT
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Crypto Big Data - Terminal Analytics & Quant Dashboard")
    parser.add_argument("--once", action="store_true", help="Chay 1 lan roi thoat (Snapshot mode)")
    parser.add_argument("--demo", action="store_true", help="Chay che do mo phong (Demo data)")
    parser.add_argument("--interval", type=int, default=3, help="Khoang thoi gian lam moi (giay), mac dinh: 3")
    args = parser.parse_args()

    provider = MarketDataProvider(demo_mode=args.demo)
    is_connected = provider.connect()

    if not is_connected and not args.demo:
        print(f"{C_YELLOW}[!] Khong the ket noi PostgreSQL (Port 5433). Chuyen sang che do Demo du lieu.{C_RESET}")
        provider.demo_mode = True

    try:
        if args.once:
            render_dashboard(provider, is_live=False)
        else:
            loop = 1
            while True:
                clear_screen()
                render_dashboard(provider, is_live=True, loop_count=loop)
                time.sleep(args.interval)
                loop += 1
    except KeyboardInterrupt:
        print(f"\n{C_YELLOW}Da dung Dashboard.{C_RESET}\n")
    finally:
        provider.close()


if __name__ == "__main__":
    main()
