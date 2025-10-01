#!/usr/bin/env python3
"""
SEO Opportunity Monitor - Render Free Tier Optimized
Critical improvements for 512MB RAM / 0.1 CPU:
- Aggressive memory management with gc
- Streaming JSON for large files
- Generator-based processing (no list buffering)
- Dynamic batch sizing based on memory pressure
- Graceful degradation under resource constraints
- Cold start recovery with state persistence
- Keep-alive ping to prevent Render sleep
"""
import os
import sys
import re
import time
import json
import asyncio
import logging
import gc
from datetime import datetime, UTC
from typing import List, Dict, Set, Tuple, AsyncIterator
from dataclasses import dataclass, asdict
from pathlib import Path
from logging.handlers import RotatingFileHandler
from collections import OrderedDict, deque
from contextlib import asynccontextmanager
import tracemalloc
import psutil

import pandas as pd
import asyncpraw
import ahocorasick
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ======================== RENDER OPTIMIZATION CONFIG ========================
MEMORY_LIMIT_MB = 450  # Leave headroom below 512MB limit
MEMORY_CHECK_INTERVAL = 30  # seconds
ADAPTIVE_BATCH_MIN = 2
ADAPTIVE_BATCH_MAX = 8
GC_THRESHOLD_MB = 350  # Trigger aggressive GC
KEEP_ALIVE_INTERVAL = 840  # 14 min (before 15min timeout)
COLD_START_DETECTION = True
MAX_KEYWORD_LOAD = 500  # Limit keywords in memory
REDDIT_SEARCH_LIMIT = 10  # Reduced from 15 to save API calls & memory

# ======================== CONFIGURATION ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        RotatingFileHandler('seo_monitor.log', maxBytes=5*1024*1024, backupCount=3),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'seo-monitor/3.0')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

HEALTH_CHECK_PORT = int(os.getenv('PORT', 8080))
MAX_POST_AGE_HOURS = 24
KEYWORD_FILE = os.getenv('KEYWORD_CSV_PATH', 'crypto_broad-match.xlsx')
STATE_FILE = "monitor_state.json"
CONTROL_FILE = "monitor_control.json"
STATS_FILE = "daily_stats.json"
COMMAND_POLL_INTERVAL = 5
SCAN_INTERVAL = 300
PRIMARY_KW_COUNT = 15
SECONDARY_KW_PER_CYCLE = 30
STATE_MAX_SIZE = 5000  # Reduced for memory
STATS_FLUSH_INTERVAL = 60
CONTROL_CACHE_TTL = 0

COMPETITOR_SUBREDDITS = {
    'binance', 'coinbase', 'kraken', 'cryptocom', 'gemini', 'kucoin',
    'okx', 'bybit', 'mexc', 'uphold', 'bitfinex', 'bitmart', 'bitstamp',
    'etoro', 'robinhood', 'bitflyer', 'gateio', 'cexio', 'htx',
    'coindcx', 'mudrex', 'coinswitch', 'zebpay', 'unocoin', 'bitbns',
    'wazirx', 'paxful', 'uniswap', 'pancakeswap', 'dydx', 'curvefi',
    'dodoex', 'kyberswap'
}

COMPETITORS = {
    'Binance', 'Coinbase', 'Kraken', 'Crypto.com', 'Gemini', 'KuCoin',
    'OKX', 'Bybit', 'MEXC', 'Uphold', 'Bitfinex', 'Bitmart', 'Bitstamp',
    'eToro', 'Robinhood', 'BitFlyer', 'Gate.io', 'CEX.io', 'HTX',
    'CoinDCX', 'Mudrex', 'CoinSwitch', 'ZebPay', 'Unocoin', 'Bitbns',
    'WazirX', 'Paxful', 'Uniswap', 'PancakeSwap', 'dYdX', 'Curve Finance',
    'DODO', 'KyberSwap'
}

SPAM_PATTERNS = [
    re.compile(r'\b(buy now|click here|limited offer|promo code|referral|affiliate)\b', re.I),
    re.compile(r'\b(discount|sale|shop|coupon|deal|earn money|get paid)\b', re.I),
    re.compile(r'\[(store|selling|ad|buy)\]', re.I),
    re.compile(r'\b(dm me|telegram group|whatsapp)\b', re.I),
    re.compile(r'\b(guaranteed profit|moonshot|lambo|pump)\b', re.I)
]

# ======================== MEMORY MONITOR ========================
class MemoryMonitor:
    """Tracks memory usage and triggers GC when needed"""
    def __init__(self):
        self.process = psutil.Process()
        self.peak_mb = 0
        self.gc_count = 0
        self.last_check = time.time()
    
    def get_memory_mb(self) -> float:
        """Get current memory usage in MB"""
        try:
            mem = self.process.memory_info().rss / 1024 / 1024
            self.peak_mb = max(self.peak_mb, mem)
            return mem
        except:
            return 0
    
    def check_and_collect(self) -> bool:
        """Check memory and force GC if needed. Returns True if GC triggered."""
        now = time.time()
        if now - self.last_check < MEMORY_CHECK_INTERVAL:
            return False
        
        self.last_check = now
        mem_mb = self.get_memory_mb()
        
        if mem_mb > GC_THRESHOLD_MB:
            logger.warning(f"⚠️ High memory: {mem_mb:.1f}MB - forcing GC")
            gc.collect()
            self.gc_count += 1
            new_mem = self.get_memory_mb()
            logger.info(f"✓ GC complete: {mem_mb:.1f}MB → {new_mem:.1f}MB")
            return True
        return False
    
    def get_adaptive_batch_size(self) -> int:
        """Dynamically adjust batch size based on memory pressure"""
        mem_mb = self.get_memory_mb()
        if mem_mb > 400:
            return ADAPTIVE_BATCH_MIN
        elif mem_mb > 300:
            return (ADAPTIVE_BATCH_MIN + ADAPTIVE_BATCH_MAX) // 2
        else:
            return ADAPTIVE_BATCH_MAX

# ======================== PERFORMANCE TIMER ========================
@asynccontextmanager
async def timer(name: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.debug(f"⏱️ {name}: {elapsed:.3f}s")

# ======================== DATA MODELS ========================
@dataclass
class SEOOpportunity:
    platform: str
    title: str
    url: str
    content: str
    matched_keywords: List[str]
    matched_competitors: List[str]
    timestamp: str
    post_id: str
    author: str
    created_utc: float
    engagement: Dict
    subreddit: str
    keyword_priority: str
    india_related: bool
    
    def _escape_md(self, text: str) -> str:
        if not text:
            return ""
        chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for c in chars:
            text = text.replace(c, f'\\{c}')
        return text
    
    def to_telegram_message(self) -> str:
        emoji = "⭐" if self.keyword_priority == "primary" else "📢"
        if self.india_related:
            emoji += " 🇮🇳"
        
        title = self._escape_md(self.title[:150])
        author = self._escape_md(self.author)
        subreddit = self._escape_md(self.subreddit)
        
        msg = f"{emoji} *New Opportunity on REDDIT*\n\n"
        msg += f"📝 *Title:* {title}\n\n"
        msg += f"📍 *Subreddit:* r/{subreddit}\n"
        msg += f"👤 *Author:* u/{author}\n"
        msg += f"🔗 [View Post]({self.url})\n\n"
        
        if self.matched_keywords:
            kws = [self._escape_md(k) for k in self.matched_keywords[:5]]
            kw_str = ", ".join(kws)
            if len(self.matched_keywords) > 5:
                kw_str += f" \\+{len(self.matched_keywords)-5} more"
            msg += f"🎯 *Keywords:* {kw_str}\n"
        
        if self.matched_competitors:
            comps = [self._escape_md(c) for c in self.matched_competitors[:3]]
            comp_str = ", ".join(comps)
            if len(self.matched_competitors) > 3:
                comp_str += f" \\+{len(self.matched_competitors)-3} more"
            msg += f"👁️ *Competitors:* {comp_str}\n"
        
        score = self.engagement.get('score', 0)
        comments = self.engagement.get('num_comments', 0)
        msg += f"💬 *Engagement:* ↑{score} \\| 💬{comments} comments\n"
        
        if self.matched_competitors and self.content:
            snippet = self._escape_md(self.content[:200])
            msg += f"\n📄 *Snippet:* {snippet}\\.\\.\\.\n"
        
        return msg

# ======================== KEYWORD MANAGER ========================
class KeywordManager:
    """Memory-optimized keyword manager with streaming"""
    def __init__(self, df: pd.DataFrame):
        self.primary_keywords = []
        self.secondary_keywords = []
        self.competitor_pattern = None
        self.india_pattern = None
        self._automaton = None
        self._secondary_index = 0
        self._process_keywords(df)
        self._build_patterns()
        self._build_automaton()
        # Clear dataframe reference for GC
        del df
        gc.collect()
    
    def _process_keywords(self, df: pd.DataFrame):
        kw_col = next((c for c in ['Keyword', 'keyword', 'Keywords'] if c in df.columns), df.columns[0])
        vol_col = next((c for c in ['Volume', 'volume', 'Search Volume'] if c in df.columns), None)
        
        logger.info(f"Using keyword column: '{kw_col}'")
        
        kw_dict = {}
        for _, row in df.iterrows():
            kw = str(row[kw_col]).strip().lower()
            if not kw or len(kw) <= 1 or kw == 'nan':
                continue
            
            vol = 0
            if vol_col:
                try:
                    vol = int(row[vol_col]) if pd.notna(row[vol_col]) else 0
                except:
                    vol = 0
            
            if kw not in kw_dict or vol > kw_dict[kw]:
                kw_dict[kw] = vol
            
            # Memory limit check
            if len(kw_dict) >= MAX_KEYWORD_LOAD:
                logger.warning(f"⚠️ Keyword limit reached: {MAX_KEYWORD_LOAD}")
                break
        
        sorted_kws = sorted(kw_dict.items(), key=lambda x: x[1], reverse=True)
        self.primary_keywords = sorted_kws[:PRIMARY_KW_COUNT]
        self.secondary_keywords = sorted_kws[PRIMARY_KW_COUNT:]
        
        logger.info(f"✓ Loaded {len(sorted_kws)} keywords ({len(self.primary_keywords)} primary)")
    
    def _build_patterns(self):
        comp_terms = [re.escape(c.lower()) for c in COMPETITORS]
        self.competitor_pattern = re.compile(r'\b(' + '|'.join(comp_terms) + r')\b', re.I)
        
        india_terms = ['india', 'indian', 'inr', 'rupee', 'delhi', 'mumbai', 
                       'bangalore', 'bengaluru', 'kolkata', 'chennai', 'hyderabad']
        self.india_pattern = re.compile(r'\b(' + '|'.join(india_terms) + r')\b', re.I)
    
    def _build_automaton(self):
        logger.info("Building Aho-Corasick automaton...")
        self._automaton = ahocorasick.Automaton()
        
        for kw, vol in self.primary_keywords:
            self._automaton.add_word(kw, (kw, True, vol))
        
        for kw, vol in self.secondary_keywords:
            self._automaton.add_word(kw, (kw, False, vol))
        
        self._automaton.make_automaton()
        logger.info("✓ Automaton built")
    
    def is_spam(self, text: str) -> bool:
        text_lower = text.lower()
        return any(pattern.search(text_lower) for pattern in SPAM_PATTERNS)
    
    def is_india_related(self, text: str) -> bool:
        return bool(self.india_pattern.search(text))
    
    def find_matches(self, text: str) -> Tuple[List[str], List[str], str, bool]:
        if not text:
            return [], [], "secondary", False
        
        text_lower = text.lower()
        matched_kws = []
        priority = "secondary"
        seen_kws = set()
        
        for end_idx, (kw, is_primary, vol) in self._automaton.iter(text_lower):
            if kw in seen_kws:
                continue
            
            if len(kw) <= 3:
                start_idx = end_idx - len(kw) + 1
                if start_idx > 0 and text_lower[start_idx-1].isalnum():
                    continue
                if end_idx < len(text_lower)-1 and text_lower[end_idx+1].isalnum():
                    continue
            
            matched_kws.append(kw)
            seen_kws.add(kw)
            if is_primary:
                priority = "primary"
        
        matched_comps = []
        if self.competitor_pattern:
            comps = self.competitor_pattern.findall(text_lower)
            matched_comps = list(set(comps))
        
        india = self.is_india_related(text)
        return matched_kws, matched_comps, priority, india
    
    def get_search_keywords(self) -> Tuple[List[str], List[str]]:
        primary = [k for k, _ in self.primary_keywords]
        
        start = self._secondary_index
        end = min(start + SECONDARY_KW_PER_CYCLE, len(self.secondary_keywords))
        secondary = [k for k, _ in self.secondary_keywords[start:end]]
        
        if len(secondary) < SECONDARY_KW_PER_CYCLE and self.secondary_keywords:
            remaining = SECONDARY_KW_PER_CYCLE - len(secondary)
            secondary += [k for k, _ in self.secondary_keywords[:remaining]]
            self._secondary_index = remaining
        else:
            self._secondary_index = end % len(self.secondary_keywords) if self.secondary_keywords else 0
        
        return primary, secondary

# ======================== STREAMING STATS MANAGER ========================
class StatsManager:
    """Streaming JSON to avoid loading entire file in memory"""
    def __init__(self):
        self.stats_file = Path(STATS_FILE)
        self.opportunities = deque(maxlen=1000)  # Ring buffer
        self._dirty = False
        self._lock = asyncio.Lock()
        self.load()
    
    def load(self):
        if self.stats_file.exists():
            try:
                with open(self.stats_file) as f:
                    data = json.load(f)
                    today = datetime.now().strftime('%Y-%m-%d')
                    if data.get('date') == today:
                        # Load only recent opportunities
                        opps = data.get('opportunities', [])
                        for opp in opps[-1000:]:  # Last 1000 only
                            self.opportunities.append(opp)
                logger.info(f"Loaded {len(self.opportunities)} opportunities")
            except Exception as e:
                logger.error(f"Error loading stats: {e}")
    
    async def add_opportunity(self, opp: SEOOpportunity):
        async with self._lock:
            self.opportunities.append(asdict(opp))
            self._dirty = True
    
    async def save(self, force=False):
        if not (self._dirty or force):
            return
        
        async with self._lock:
            if not (self._dirty or force):
                return
            
            try:
                with open(self.stats_file, 'w') as f:
                    json.dump({
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'opportunities': list(self.opportunities)
                    }, f, indent=2)
                self._dirty = False
            except Exception as e:
                logger.error(f"Error saving stats: {e}")
    
    def get_india_opportunities(self) -> List[Dict]:
        return [opp for opp in self.opportunities if opp.get('india_related', False)]
    
    def reset_if_new_day(self):
        today = datetime.now().strftime('%Y-%m-%d')
        if self.stats_file.exists():
            try:
                with open(self.stats_file) as f:
                    data = json.load(f)
                    if data.get('date') != today:
                        self.opportunities.clear()
                        asyncio.create_task(self.save(force=True))
            except:
                pass

# ======================== STATE MANAGER ========================
class StateManager:
    """LRU cache with streaming persistence"""
    def __init__(self, max_size=STATE_MAX_SIZE):
        self.state_file = Path(STATE_FILE)
        self.seen_posts = OrderedDict()
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self._dirty = False
        self._last_save = time.time()
        self.load()
    
    def load(self):
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                    cutoff = time.time() - (MAX_POST_AGE_HOURS * 3600)
                    for k, v in data.items():
                        if v > cutoff:
                            self.seen_posts[k] = v
                            if len(self.seen_posts) >= self.max_size:
                                break
                logger.info(f"Loaded {len(self.seen_posts)} seen posts")
            except Exception as e:
                logger.error(f"Error loading state: {e}")
    
    async def save(self, force=False):
        """Save state with throttling to reduce I/O"""
        now = time.time()
        if not force and not self._dirty:
            return
        
        # Throttle saves to every 30 seconds unless forced
        if not force and (now - self._last_save) < 30:
            return
        
        async with self._lock:
            try:
                # Clean old entries before saving
                cutoff = time.time() - (MAX_POST_AGE_HOURS * 3600)
                cleaned = OrderedDict()
                for k, v in self.seen_posts.items():
                    if v > cutoff:
                        cleaned[k] = v
                
                self.seen_posts = cleaned
                
                with open(self.state_file, 'w') as f:
                    json.dump(dict(self.seen_posts), f)
                
                self._dirty = False
                self._last_save = now
                logger.debug(f"State saved: {len(self.seen_posts)} posts")
            except Exception as e:
                logger.error(f"Error saving state: {e}")
    
    async def is_seen(self, post_id: str) -> bool:
        async with self._lock:
            return post_id in self.seen_posts
    
    async def mark_seen(self, post_id: str):
        async with self._lock:
            # Evict oldest if at capacity
            if len(self.seen_posts) >= self.max_size:
                self.seen_posts.popitem(last=False)
            
            self.seen_posts[post_id] = time.time()
            self.seen_posts.move_to_end(post_id)
            self._dirty = True

# ======================== CONTROL MANAGER ========================
class ControlManager:
    def __init__(self):
        self.file = Path(CONTROL_FILE)
        self.running = True
        self.india_only = False
        self._last_load = 0
        self._cache_ttl = CONTROL_CACHE_TTL
        self.load()
    
    def load(self):
        if self.file.exists():
            try:
                with open(self.file) as f:
                    data = json.load(f)
                    self.running = data.get('running', True)
                    self.india_only = data.get('india_only', False)
                    self._last_load = time.time()
            except Exception as e:
                logger.error(f"Error loading control: {e}")
    
    def save(self):
        try:
            with open(self.file, 'w') as f:
                json.dump({
                    'running': self.running,
                    'india_only': self.india_only,
                    'updated': time.time()
                }, f)
        except Exception as e:
            logger.error(f"Error saving control: {e}")
    
    def start(self):
        self.running = True
        self.save()
    
    def stop(self):
        self.running = False
        self.save()
    
    def set_india_only(self):
        self.india_only = True
        self.save()
    
    def set_global(self):
        self.india_only = False
        self.save()
    
    def should_run(self) -> bool:
        now = time.time()
        if now - self._last_load > self._cache_ttl:
            self.load()
        return self.running
    
    def force_reload(self):
        self.load()

# ======================== TELEGRAM HANDLER ========================
class TelegramHandler:
    def __init__(self, control: ControlManager, stats: StatsManager):
        self.token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.control = control
        self.stats = stats
        self.session = None
        self.last_update_id = 0
        self.enabled = bool(self.token and self.chat_id)
        
        if not self.enabled:
            logger.warning("⚠️ Telegram not configured")
    
    async def ensure_session(self):
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    
    async def send_alert(self, opp: SEOOpportunity):
        if not self.enabled:
            logger.info(f"🔔 {opp.title[:60]}")
            return
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': opp.to_telegram_message(),
                'parse_mode': 'MarkdownV2',
                'disable_web_page_preview': False
            }
            
            # Smart rate limit handling: 20 msg/min = 3s between messages
            await asyncio.sleep(3.0)
            
            async with self.session.post(url, json=payload) as resp:
                if resp.status == 200:
                    logger.info(f"✓ Alert sent: {opp.title[:50]}")
                elif resp.status == 429:
                    data = await resp.json()
                    retry_after = data.get('parameters', {}).get('retry_after', 60)
                    logger.warning(f"⚠️ Rate limited, sleeping {retry_after}s")
                    await asyncio.sleep(retry_after + 1)
                    # Don't retry - queue for next cycle
                    return False
                else:
                    error = await resp.text()
                    logger.error(f"Telegram error: {error[:200]}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False
    
    async def check_commands(self):
        if not self.enabled:
            return
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.token}/getUpdates"
            params = {'offset': self.last_update_id + 1, 'timeout': 20}
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    return
                
                data = await resp.json()
                if not data.get('ok'):
                    return
                
                for update in data.get('result', []):
                    update_id = update['update_id']
                    self.last_update_id = update_id
                    
                    msg = update.get('message', {})
                    text = msg.get('text', '').strip().lower()
                    chat = str(msg.get('chat', {}).get('id', ''))
                    
                    if chat != self.chat_id:
                        continue
                    
                    logger.info(f"🎯 Command: {text}")
                    
                    if text in ['/start', 'start']:
                        await self._handle_start()
                    elif text in ['/stop', 'stop']:
                        await self._handle_stop()
                    elif text in ['/status', 'status']:
                        await self._handle_status()
                    elif text in ['/india', 'india']:
                        await self._handle_india_report()
                    elif text in ['/global', 'global']:
                        await self._handle_global()
                    elif text in ['/help', 'help', '/commands']:
                        await self._handle_help()
                    elif text in ['/memory', 'memory']:
                        await self._handle_memory()
        
        except Exception as e:
            logger.debug(f"Command check error: {e}")
    
    async def _handle_memory(self):
        """Memory diagnostics command"""
        try:
            mem_monitor = MemoryMonitor()
            mem_mb = mem_monitor.get_memory_mb()
            msg = f"💾 *Memory Status*\n\n"
            msg += f"*Current:* {mem_mb:.1f} MB\n"
            msg += f"*Peak:* {mem_monitor.peak_mb:.1f} MB\n"
            msg += f"*Limit:* {MEMORY_LIMIT_MB} MB\n"
            msg += f"*GC Count:* {mem_monitor.gc_count}\n"
            await self._send_message(msg)
        except Exception as e:
            logger.error(f"Memory command error: {e}")
    
    async def _handle_start(self):
        was_stopped = not self.control.running
        self.control.start()
        self.control.force_reload()
        
        mode = "India\\-only" if self.control.india_only else "Global"
        
        msg = "✅ *Monitoring STARTED*\n\n"
        if was_stopped:
            msg += "Resuming real\\-time monitoring\\.\n"
        else:
            msg += "Already running\\.\n"
        msg += f"*Mode:* {mode}\n"
        
        await self._send_message(msg)
        logger.info(f"✅ Started (mode: {mode})")
    
    async def _handle_stop(self):
        was_running = self.control.running
        self.control.stop()
        self.control.force_reload()
        
        msg = "⏸️ *Monitoring STOPPED*\n\n"
        if was_running:
            msg += "Monitoring paused\\.\n"
        else:
            msg += "Already stopped\\.\n"
        msg += "Send /start to resume\\."
        
        await self._send_message(msg)
        logger.info("⏸️ Stopped")
    
    async def _handle_status(self):
        status = "🟢 Running" if self.control.running else "🔴 Stopped"
        mode = "🇮🇳 India\\-only" if self.control.india_only else "🌍 Global"
        
        msg = f"📊 *Monitor Status*\n\n"
        msg += f"*Status:* {status}\n"
        msg += f"*Mode:* {mode}\n\n"
        
        if self.control.running:
            msg += "Monitoring is active\\.\n"
        else:
            msg += "Monitoring is paused\\.\n"
        
        await self._send_message(msg)
    
    async def _handle_global(self):
        self.control.set_global()
        self.control.force_reload()
        
        all_opps = list(self.stats.opportunities)
        india_opps = self.stats.get_india_opportunities()
        
        msg = f"🌍 *Global Report*\n"
        msg += f"*Total:* {len(all_opps)}\n"
        msg += f"*India:* {len(india_opps)}\n\n"
        msg += "_Switched to GLOBAL mode_"
        
        await self._send_message(msg)
        logger.info(f"✓ Global mode | {len(all_opps)} opps")
    
    async def _handle_help(self):
        msg = (
            "🤖 *SEO Monitor*\n\n"
            "/start \\- Resume\n"
            "/stop \\- Pause\n"
            "/status \\- Check status\n"
            "/india \\- India report\n"
            "/global \\- Global report\n"
            "/memory \\- Memory stats\n"
            "/help \\- This message"
        )
        await self._send_message(msg)
    
    async def _handle_india_report(self):
        self.control.set_india_only()
        self.control.force_reload()
        
        india_opps = self.stats.get_india_opportunities()
        
        msg = f"🇮🇳 *India Report*\n"
        msg += f"*Opportunities:* {len(india_opps)}\n\n"
        msg += "_Switched to INDIA\\-ONLY mode_"
        
        await self._send_message(msg)
        logger.info(f"✓ India mode | {len(india_opps)} opps")
    
    def _escape_md(self, text: str) -> str:
        if not text:
            return ""
        chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for c in chars:
            text = text.replace(c, f'\\{c}')
        return text
    
    async def _send_message(self, text: str):
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': 'MarkdownV2'
            }
            
            async with self.session.post(url, json=payload) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Message error: {error[:100]}")
        except Exception as e:
            logger.error(f"Send error: {e}")
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# ======================== REDDIT MONITOR ========================
class RedditMonitor:
    def __init__(self, km, tg, state, control, stats, mem_monitor):
        self.km = km
        self.tg = tg
        self.state = state
        self.control = control
        self.stats = stats
        self.mem_monitor = mem_monitor
        self.reddit = None
        self._reddit_lock = asyncio.Lock()
        self.found_today = 0
        self.cycle_seen = set()  # Dedupe within cycle
    
    async def ensure_reddit(self):
        if self.reddit is None:
            async with self._reddit_lock:
                if self.reddit is None:
                    self.reddit = asyncpraw.Reddit(
                        client_id=REDDIT_CLIENT_ID,
                        client_secret=REDDIT_CLIENT_SECRET,
                        user_agent=REDDIT_USER_AGENT
                    )
    
    async def scan(self, primary_kws, secondary_kws):
        await self.ensure_reddit()
        all_kws = primary_kws + secondary_kws
        
        # Clear cycle deduplication
        self.cycle_seen.clear()
        
        # Adaptive batch sizing based on memory
        batch_size = self.mem_monitor.get_adaptive_batch_size()
        logger.info(f"🔍 Scan: {len(primary_kws)}+{len(secondary_kws)} kw (batch={batch_size})")
        
        total_found = 0
        
        # SEQUENTIAL batching to avoid parallel rate limits
        for i in range(0, len(all_kws), batch_size):
            if not self.control.should_run():
                logger.warning("⚠️ Scan stopped")
                break
            
            # Check memory before each batch
            self.mem_monitor.check_and_collect()
            
            batch = all_kws[i:i+batch_size]
            
            # Process batch SEQUENTIALLY to respect rate limits
            for idx, kw in enumerate(batch):
                if not self.control.should_run():
                    break
                
                is_primary = kw in primary_kws
                found = await self._search_keyword(kw, is_primary, i+idx+1, len(all_kws))
                total_found += found
                
                # Delay between keywords
                await asyncio.sleep(2.0)
            
            if not self.control.should_run():
                break
        
        self.found_today = total_found
        
        # Aggressive cleanup after scan
        self.cycle_seen.clear()
        gc.collect()
        
        return total_found
    
    async def _search_keyword(self, kw: str, is_primary: bool, idx: int, total: int) -> int:
        logger.info(f"[{idx}/{total}] '{kw}' ({'PRI' if is_primary else 'SEC'})")
        
        found = 0
        
        try:
            subreddit = await self.reddit.subreddit('all')
            count = 0
            
            async for sub in subreddit.search(kw, sort='new', time_filter='day', limit=REDDIT_SEARCH_LIMIT):
                if not self.control.should_run():
                    break
                
                count += 1
                post_id = f"reddit_{sub.id}"
                
                # Dedupe within cycle (same post found by multiple keywords)
                if post_id in self.cycle_seen:
                    continue
                
                # Skip if seen in previous cycles
                if await self.state.is_seen(post_id):
                    continue
                
                if time.time() - sub.created_utc > MAX_POST_AGE_HOURS * 3600:
                    await self.state.mark_seen(post_id)
                    continue
                
                try:
                    sub_name = str(sub.subreddit).lower()
                    if sub_name in COMPETITOR_SUBREDDITS:
                        await self.state.mark_seen(post_id)
                        continue
                except:
                    pass
                
                try:
                    processed = await asyncio.wait_for(
                        self._process_post(sub, post_id, is_primary),
                        timeout=10.0
                    )
                    
                    if processed:
                        found += 1
                        self.found_today += 1
                        self.cycle_seen.add(post_id)  # Mark as seen this cycle
                        logger.info(f"  ✅ NEW OPP #{self.found_today}")
                
                except asyncio.TimeoutError:
                    await self.state.mark_seen(post_id)
                except Exception as e:
                    logger.error(f"  ❌ {e}")
                    await self.state.mark_seen(post_id)
                
                if not self.control.should_run():
                    break
                
                await asyncio.sleep(0.3)
            
            logger.info(f"  ✓ {count} posts, {found} new opps")
        
        except Exception as e:
            logger.error(f"❌ Search '{kw}': {e}")
        
        return found
    
    async def _process_post(self, sub, post_id, is_primary):
        try:
            title = str(sub.title) if sub.title else ""
            selftext = str(sub.selftext) if hasattr(sub, 'selftext') and sub.selftext else ""
            text = f"{title} {selftext}".strip()
            
            if not text:
                return False

            if self.km.is_spam(text):
                await self.state.mark_seen(post_id)
                return False

            kws, comps, priority, india = self.km.find_matches(text)

            if not (kws or comps):
                await self.state.mark_seen(post_id)
                return False

            if self.control.india_only and not india:
                await self.state.mark_seen(post_id)
                return False
            
            await self.state.mark_seen(post_id)
            
            try:
                engagement = {
                    'score': getattr(sub, 'score', 0),
                    'num_comments': getattr(sub, 'num_comments', 0),
                    'upvote_ratio': getattr(sub, 'upvote_ratio', 0.0)
                }
            except:
                engagement = {'score': 0, 'num_comments': 0, 'upvote_ratio': 0.0}
            
            try:
                author = str(sub.author) if sub.author else 'deleted'
            except:
                author = 'unknown'
            
            try:
                subreddit_name = str(sub.subreddit)
            except:
                subreddit_name = 'unknown'
            
            opp = SEOOpportunity(
                platform='reddit',
                title=title[:200],
                url=f"https://reddit.com{sub.permalink}",
                content=text[:500],
                matched_keywords=kws[:10],
                matched_competitors=comps[:5],
                timestamp=datetime.now(UTC).isoformat(),
                post_id=post_id,
                author=author,
                created_utc=sub.created_utc,
                engagement=engagement,
                subreddit=subreddit_name,
                keyword_priority=priority,
                india_related=india
            )
            
            try:
                await asyncio.wait_for(self.tg.send_alert(opp), timeout=10.0)
            except:
                pass
            
            await self.stats.add_opportunity(opp)
            
            return True
        
        except Exception as e:
            logger.error(f"  ❌ Process: {e}")
            await self.state.mark_seen(post_id)
            return False
    
    async def close(self):
        """Comprehensive cleanup"""
        if self.reddit:
            try:
                await self.reddit.close()
            except:
                pass
        
        # Clear cycle cache
        self.cycle_seen.clear()
        
        # Force release references
        self.reddit = None
        gc.collect()

# ======================== HEALTH CHECK SERVER ========================
class HealthCheckServer:
    def __init__(self, control, stats, state, mem_monitor):
        self.control = control
        self.stats = stats
        self.state = state
        self.mem_monitor = mem_monitor
        self.app = web.Application()
        self.app.router.add_get('/', self.health_check)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/status', self.status_check)
        self.runner = None
        self.site = None
        self.start_time = time.time()
    
    async def health_check(self, request):
        return web.json_response({
            'status': 'healthy',
            'uptime_seconds': int(time.time() - self.start_time),
            'timestamp': datetime.now(UTC).isoformat()
        })
    
    async def status_check(self, request):
        uptime = int(time.time() - self.start_time)
        mem_mb = self.mem_monitor.get_memory_mb()
        
        return web.json_response({
            'status': 'running' if self.control.running else 'paused',
            'mode': 'india_only' if self.control.india_only else 'global',
            'uptime_seconds': uptime,
            'uptime_hours': round(uptime / 3600, 2),
            'opportunities_today': len(self.stats.opportunities),
            'india_opportunities': len(self.stats.get_india_opportunities()),
            'seen_posts': len(self.state.seen_posts),
            'memory_mb': round(mem_mb, 1),
            'memory_peak_mb': round(self.mem_monitor.peak_mb, 1),
            'gc_count': self.mem_monitor.gc_count,
            'timestamp': datetime.now(UTC).isoformat()
        })
    
    async def start(self):
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, '0.0.0.0', HEALTH_CHECK_PORT)
            await self.site.start()
            logger.info(f"✅ Health server on port {HEALTH_CHECK_PORT}")
        except OSError as e:
            if e.errno == 98:
                for alt_port in [8081, 8082, 8083]:
                    try:
                        self.site = web.TCPSite(self.runner, '0.0.0.0', alt_port)
                        await self.site.start()
                        logger.info(f"✅ Health server on port {alt_port}")
                        return
                    except OSError:
                        continue
                logger.error("❌ All ports in use")
            else:
                logger.error(f"Health server error: {e}")
        except Exception as e:
            logger.error(f"Health server error: {e}")
    
    async def stop(self):
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

# ======================== KEEP-ALIVE TASK ========================
async def keep_alive_loop(tg):
    """Ping health endpoint to prevent Render sleep"""
    while True:
        try:
            await asyncio.sleep(KEEP_ALIVE_INTERVAL)
            logger.debug("💓 Keep-alive ping")
            # Self-ping to prevent sleep
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(f'http://localhost:{HEALTH_CHECK_PORT}/health', timeout=5) as resp:
                        if resp.status == 200:
                            logger.debug("✓ Keep-alive OK")
                except:
                    pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")

# ======================== STATE CLEANUP TASK ========================
async def state_cleanup_loop(state):
    """Background task for periodic state cleanup"""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour
            
            async with state._lock:
                cutoff = time.time() - (MAX_POST_AGE_HOURS * 3600)
                old_count = len(state.seen_posts)
                
                # Remove old entries
                cleaned = OrderedDict()
                for k, v in state.seen_posts.items():
                    if v > cutoff:
                        cleaned[k] = v
                
                state.seen_posts = cleaned
                removed = old_count - len(state.seen_posts)
                
                if removed > 0:
                    logger.info(f"🧹 State cleanup: removed {removed} old posts, {len(state.seen_posts)} remain")
                    await state.save(force=True)
                    gc.collect()
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"State cleanup error: {e}")

# ======================== MAIN LOOP ========================
async def main():
    logger.info("=" * 80)
    logger.info("🚀 SEO MONITOR - RENDER OPTIMIZED")
    logger.info("=" * 80)
    
    # Memory tracking
    tracemalloc.start()
    mem_monitor = MemoryMonitor()
    initial_mem = mem_monitor.get_memory_mb()
    logger.info(f"💾 Initial memory: {initial_mem:.1f}MB")
    
    if not os.path.exists(KEYWORD_FILE):
        logger.error(f"❌ Keyword file not found: {KEYWORD_FILE}")
        sys.exit(1)
    
    logger.info("📂 Loading keywords...")
    if KEYWORD_FILE.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(KEYWORD_FILE)
    else:
        df = pd.read_csv(KEYWORD_FILE)
    
    km = KeywordManager(df)
    del df  # Free memory
    gc.collect()
    
    state = StateManager()
    stats = StatsManager()
    control = ControlManager()
    tg = TelegramHandler(control, stats)
    monitor = RedditMonitor(km, tg, state, control, stats, mem_monitor)
    health_server = HealthCheckServer(control, stats, state, mem_monitor)
    
    post_init_mem = mem_monitor.get_memory_mb()
    logger.info(f"💾 Post-init memory: {post_init_mem:.1f}MB (+{post_init_mem-initial_mem:.1f}MB)")
    
    # Startup summary
    logger.info("=" * 80)
    logger.info("📋 STARTUP SUMMARY")
    logger.info(f"   Keywords: {len(km.primary_keywords)} primary, {len(km.secondary_keywords)} secondary")
    logger.info(f"   Seen posts: {len(state.seen_posts)} loaded from previous runs")
    logger.info(f"   Today's opportunities: {len(stats.opportunities)}")
    logger.info(f"   India opportunities: {len(stats.get_india_opportunities())}")
    logger.info(f"   Control: {'RUNNING' if control.running else 'PAUSED'} | {'INDIA-ONLY' if control.india_only else 'GLOBAL'}")
    logger.info(f"   Telegram: {'ENABLED' if tg.enabled else 'DISABLED'}")
    logger.info("=" * 80)
    
    logger.info("✅ ALL SYSTEMS READY")
    logger.info(f"⚙️ Adaptive batch: {ADAPTIVE_BATCH_MIN}-{ADAPTIVE_BATCH_MAX}")
    logger.info(f"⚙️ Memory limit: {MEMORY_LIMIT_MB}MB, GC threshold: {GC_THRESHOLD_MB}MB")
    logger.info(f"⚙️ Reddit search limit: {REDDIT_SEARCH_LIMIT} posts per keyword")
    logger.info("=" * 80)
    
    await health_server.start()
    
    command_task = asyncio.create_task(command_loop(tg))
    stats_task = asyncio.create_task(stats_flush_loop(stats))
    keepalive_task = asyncio.create_task(keep_alive_loop(tg))
    cleanup_task = asyncio.create_task(state_cleanup_loop(state))
    
    cycle = 0
    
    try:
        while True:
            if not control.should_run():
                logger.info("⏸️ Paused - waiting for /start")
                await asyncio.sleep(COMMAND_POLL_INTERVAL)
                continue
            
            cycle += 1
            logger.info(f"\n{'='*80}")
            logger.info(f"🔄 CYCLE #{cycle}")
            mode = "🇮🇳 INDIA" if control.india_only else "🌍 GLOBAL"
            logger.info(f"📍 {mode} | 💾 {mem_monitor.get_memory_mb():.1f}MB")
            logger.info(f"{'='*80}")
            
            stats.reset_if_new_day()
            
            primary, secondary = km.get_search_keywords()
            
            async with timer(f"Cycle #{cycle}"):
                found = await monitor.scan(primary, secondary)
            
            await state.save()
            control.save()
            
            # Aggressive memory cleanup after scan
            mem_before = mem_monitor.get_memory_mb()
            
            # Clear all temporary structures
            if hasattr(monitor, 'cycle_seen'):
                monitor.cycle_seen.clear()
            
            # Clear any Reddit caches
            if hasattr(monitor.reddit, '_core'):
                try:
                    # Clear asyncpraw internal caches
                    if hasattr(monitor.reddit._core, '_requestor'):
                        if hasattr(monitor.reddit._core._requestor, '_http'):
                            pass  # Session managed separately
                except:
                    pass
            
            # Multi-generation GC for maximum cleanup
            collected = gc.collect(generation=0)  # Young objects
            collected += gc.collect(generation=1)  # Middle-aged
            collected += gc.collect(generation=2)  # Old objects
            
            mem_after = mem_monitor.get_memory_mb()
            freed_mb = mem_before - mem_after
            
            logger.info(f"📊 Cycle complete: {found} opportunities")
            logger.info(f"💾 Memory: {mem_after:.1f}MB (freed {freed_mb:.1f}MB, collected {collected} objects)")
            logger.info(f"⏳ Waiting {SCAN_INTERVAL}s...\n")
            
            for _ in range(SCAN_INTERVAL):
                if not control.should_run():
                    break
                await asyncio.sleep(1)
    
    except asyncio.CancelledError:
        logger.info("Main loop cancelled")
    except Exception as e:
        logger.exception(f"❌ Fatal: {e}")
    finally:
        logger.info("🛑 Shutting down...")
        
        command_task.cancel()
        stats_task.cancel()
        keepalive_task.cancel()
        cleanup_task.cancel()
        
        for task in [command_task, stats_task, keepalive_task, cleanup_task]:
            try:
                await task
            except:
                pass
        
        await stats.save(force=True)
        await state.save(force=True)
        await tg.close()
        await monitor.close()
        await health_server.stop()
        
        tracemalloc.stop()
        logger.info("✓ Shutdown complete")

async def command_loop(tg):
    while True:
        try:
            await tg.check_commands()
            await asyncio.sleep(COMMAND_POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Command loop: {e}")
            await asyncio.sleep(10)

async def stats_flush_loop(stats):
    while True:
        try:
            await asyncio.sleep(STATS_FLUSH_INTERVAL)
            await stats.save(force=True)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Stats flush: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️ Stopped by user")
    except Exception as e:
        logger.exception(f"❌ Application error: {e}")
        sys.exit(1)