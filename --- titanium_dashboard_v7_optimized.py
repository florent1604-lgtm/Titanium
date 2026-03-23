from __future__ import annotations

"""
Titanium Dashboard v8 — ORDER BOOK ENGINE INTÉGRÉ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[v8] Intégration TitaniumOrderBookEngine :
     [OB-1] Carnet d'ordres Niveau 2 en temps réel (Binance Futures WS)
     [OB-2] Simulation hybride réaliste Gold (XAUUSD) via mouvement brownien
     [OB-3] Détection SMC : Murs (WALL_BUY/SELL), Imbalance, Absorption
     [OB-4] Broadcast BOOK_UPDATE via WS existant /ws/{symbol} (port 8080)
     [OB-5] Endpoint REST /api/orderbook/{symbol} + /api/orderbook/signals
     [OB-6] Suppression du serveur WS séparé (port 8765) — unifié port 8080
[v7] Optimisations de performance :
     [OPT-1] Vectorisation NumPy + Numba JIT pour backtests (×10 vitesse)
     [OPT-2] Cache LRU pour détections FVG/OB (TTL 30s, ×5 rapidité)
     [OPT-3] Pool de connexions aiohttp optimisé (50 connexions, 25/hôte)
     [OPT-4] Configurations réduites (7 configs au lieu de 12, -42%)
     [OPT-5] Réduction allocations mémoire dans boucles critiques
[v6] Optimisation annuelle multi-actifs : Module 18 — optimise_strategies_year()
     Filtre les bougies sur OPT_YEAR, teste toutes les OPT_CONFIGURATIONS,
     sélectionne la meilleure (Sharpe + Expectancy + Winrate + Drawdown).
     Résultats injectés dans signals[sym]['best_config_2025'] et /api/state.
     Boucle async périodique (OPT_REFRESH_HOURS), endpoint /api/optim/results
     et /api/optim/run pour recalcul à la demande.
[v5] SL/TP Adaptatif : compute_adaptive_levels() — ATR clampé p20–p80,
     frais Binance 4bps intégrés, ratios RR calibrés (1.2 / 1.8 / 2.4),
     4 TPs générés. Remplacement de compute_atr_levels() pour sl/tps.
[v4] Dashboard HTML v4 — neon dark, collapse sidebar, STRICT monitor table,
     Vision IA, Learning adaptatif, heatmap TRIX, pako WS compress.
- Bridge conservé mais désactivé par défaut (ENABLE_BRIDGE=0)
- Lifespan context manager (FastAPI)
- Compteur de clients WS + métriques /api/metrics
- Scoring 24/7 : suppression du critère SESSION 8-18
- Compression HTTP via GZipMiddleware (utile pour /api/state).
- Compression WS optionnelle (gzip+base64) via WS_COMPRESS=gzip_base64.
- Vision locale Ollama: /api/chat + images:[base64]
- LLaVA prioritaire + Qwen2.5-VL 3B fallback
"""

import asyncio
import base64
import gzip
import hashlib
import json
import logging
import multiprocessing
import os
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from functools import lru_cache
from collections import deque as _deque

import aiohttp
import numpy as np
import pandas as pd
import pandas_ta as ta
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi import Request as FARequest
from fastapi.responses import HTMLResponse
from starlette.middleware.gzip import GZipMiddleware

# Optionnel: numba pour JIT (accélération ×10-100 sur boucles numériques)
try:
    from numba import jit, prange
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False
    # Fallback decorators no-op
    def jit(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    prange = range

# Optionnel (monitoring CPU/RAM des workers STRICT)
try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None


# ---------------------------------------------------------------------------
# .env
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().with_name(".env")
    load_dotenv(env_path if env_path.exists() else None)
except Exception:
    pass

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("titanium_dashboard")


# ---------------------------------------------------------------------------
# CONFIG OPTIMISÉE [v8]
# ---------------------------------------------------------------------------
SYMBOLS = [s.strip() for s in os.getenv("BINANCE_SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT,PAXG/USDT").split(",") if s.strip()]

WS_BASE = "wss://stream.binance.com:9443"
WS_BASES = [x.strip() for x in os.getenv("WS_BASES", "wss://stream.binance.com:9443,wss://stream.binance.com:443,wss://data-stream.binance.vision").split(",") if x.strip()]
REST_BASE = "https://api.binance.com"
REST_FALLBACK = "https://data-api.binance.vision"

# ---------------------------------------------------------------------------
# ORDER BOOK ENGINE [v8] — Carnet d'ordres N2 en temps réel
# ---------------------------------------------------------------------------
OB_ENABLED          = os.getenv("OB_ENABLED", "1").strip().lower() in ("1","true","yes","on")
OB_BINANCE_WS_URL   = "wss://fstream.binance.com/ws"
# Symboles avec leur stream Binance Futures (depth20@100ms)
OB_SYMBOL_MAP: Dict[str, Dict[str, str]] = {
    "BTC/USDT":  {"exchange": "binance", "stream": "btcusdt@depth20@100ms"},
    "ETH/USDT":  {"exchange": "binance", "stream": "ethusdt@depth20@100ms"},
    "SOL/USDT":  {"exchange": "binance", "stream": "solusdt@depth20@100ms"},
    "PAXG/USDT": {"exchange": "hybrid",  "stream": "simulation"},   # Gold → simulation réaliste
}
OB_WALL_THRESHOLD    = float(os.getenv("OB_WALL_THRESHOLD",    "0.05"))   # 5% du volume total
OB_IMBALANCE_THRESH  = float(os.getenv("OB_IMBALANCE_THRESH",  "0.75"))   # 75% d'imbalance
OB_ABSORPTION_WINDOW = int(os.getenv("OB_ABSORPTION_WINDOW",   "10"))     # nb snapshots pour absorption
OB_HISTORY_MAXLEN    = int(os.getenv("OB_HISTORY_MAXLEN",      "200"))    # snapshots gardés par symbole
OB_SIGNALS_MAXLEN    = int(os.getenv("OB_SIGNALS_MAXLEN",      "50"))     # signaux SMC gardés

# [OPT-3] Pool de connexions HTTP optimisé [v7]
HTTP_POOL_SIZE = int(os.getenv("HTTP_POOL_SIZE", "50"))  # ↑ de défaut à 50
HTTP_CONNECT_LIMIT = int(os.getenv("HTTP_CONNECT_LIMIT", "25"))
HTTP_TIMEOUT_TOTAL = int(os.getenv("HTTP_TIMEOUT_TOTAL", "30"))

# ---------------------------------------------------------------------------
# BINANCE API — Clé lecture seule (rate-limits levés + endpoints privés légers)
# ---------------------------------------------------------------------------
BINANCE_KEY    = os.getenv("BINANCE_KEY",    "").strip()
BINANCE_SECRET = os.getenv("BINANCE_SECRET", "").strip()

# ---------------------------------------------------------------------------
# BINANCE FUTURES — Open Interest, Funding Rate, Liquidations (read-only)
# ---------------------------------------------------------------------------
FUTURES_BASE        = os.getenv("FUTURES_BASE",        "https://fapi.binance.com")
FUTURES_ENABLED     = os.getenv("FUTURES_ENABLED",     "1").strip().lower() in ("1","true","yes","on")
FUTURES_CACHE_TTL   = int(os.getenv("FUTURES_CACHE_TTL",   "60"))   # 60s (funding change toutes les 8h, OI change souvent)
# Symboles pour lesquels on fetch les métriques Futures (perps USDT)
FUTURES_SYMBOLS_MAP = {
    "BTC/USDT": "BTCUSDT",
    "ETH/USDT": "ETHUSDT",
    "SOL/USDT": "SOLUSDT",
}

# ---------------------------------------------------------------------------
# XAU/USD — Gold réel (remplace le proxy PAXG/USDT)
#
# Architecture dual-provider :
#   1. Twelve Data (PRIMARY)  : API REST gratuite, 800 req/jour, clé simple
#      → Inscription gratuite : https://twelvedata.com  (onglet "Free")
#      → Clé dispo immédiatement après inscription
#   2. Yahoo Finance (FALLBACK) : aucune clé requise, symbole GC=F (Gold Futures)
#      → Activé automatiquement si Twelve Data indisponible ou quota dépassé
#
# Pour utiliser Gold réel : renseigner uniquement TWELVEDATA_API_KEY dans .env
# Sans clé : le bot reste sur PAXG/USDT (proxy Binance)
# ---------------------------------------------------------------------------
TWELVEDATA_API_KEY   = os.getenv("TWELVEDATA_API_KEY", "").strip()
TWELVEDATA_BASE_URL  = "https://api.twelvedata.com"
# Activer si clé configurée OU si fallback Yahoo autorisé
GOLD_REAL_ENABLED    = os.getenv("GOLD_REAL_ENABLED", "1").strip().lower() in ("1","true","yes","on")
GOLD_CACHE_TTL       = int(os.getenv("GOLD_CACHE_TTL", "180"))  # 3min (données forex moins fréquentes)
# Symboles pour lesquels on utilise Gold réel (PAXG → XAU/USD)
GOLD_SYMBOL_MAP: Dict[str, str] = {
    "PAXG/USDT": "XAU/USD",   # Gold physique réel
}
# Mapping timeframe Titanium → Twelve Data interval
_TD_TF_MAP = {
    "4h": "4h", "2h": "2h", "1h": "1h", "30m": "30min",
    "15m": "15min", "5m": "5min", "3m": "3min", "1m": "1min", "1d": "1day",
}
# Mapping timeframe Titanium → Yahoo Finance interval (pour fallback)
_YF_TF_MAP = {
    "4h": "1h", "2h": "1h", "1h": "1h", "30m": "30m",
    "15m": "15m", "5m": "5m", "3m": "5m", "1m": "1m", "1d": "1d",
}

# Rétrocompatibilité : si une ancienne clé OANDA est encore dans .env, on l'ignore silencieusement
OANDA_API_KEY    = ""   # désactivé — remplacé par Twelve Data / Yahoo Finance
OANDA_ENABLED    = False
OANDA_SYMBOL_MAP: Dict[str, str] = {}  # vide — utiliser GOLD_SYMBOL_MAP

# ---------------------------------------------------------------------------
# DELTA VOLUME — Pression acheteur/vendeur via flux aggTrade Binance
# ---------------------------------------------------------------------------
DELTA_VOL_ENABLED = os.getenv("DELTA_VOL_ENABLED", "1").strip().lower() in ("1","true","yes","on")
DELTA_VOL_WINDOW  = int(os.getenv("DELTA_VOL_WINDOW",  "100"))  # nb de trades rolling pour le calcul
# Seuil delta significatif (en % du volume total) pour qualifier la pression directionnelle
DELTA_VOL_SIGNAL_PCT = float(os.getenv("DELTA_VOL_SIGNAL_PCT", "0.60"))  # 60% buy ou sell = signal fort

# ---------------------------------------------------------------------------
# 1D — Biais journalier macro (EMA200 daily + structure daily)
# ---------------------------------------------------------------------------
USE_1D_BIAS  = os.getenv("USE_1D_BIAS",  "1").strip().lower() in ("1","true","yes","on")
D1_CACHE_TTL = int(os.getenv("D1_CACHE_TTL", "3600"))  # 1h (bougies daily changent peu)
D1_LIMIT     = int(os.getenv("D1_LIMIT",     "250"))    # 250 jours d'historique (~1 an)

MAX_1S = int(os.getenv("MAX_1S", "1800"))                 # buffer 1s (30 min)
MAX_CANDLES_30S = int(os.getenv("MAX_CANDLES_30S", "500"))# cap 30s (~4h10)
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "5"))
# TF actif affiché sur le dashboard — pilote les lookbacks OB/FVG et la TF de confirmation
# Valeurs valides : "1m","3m","5m","15m","30m","1h","4h"
# Mapping attente de confirmation : 1m→5m | 3m→15m | 5m→45m | 15m→1h | 30m→2h | 1h→4h | 4h→1d
ACTIVE_TF = os.getenv("ACTIVE_TF", "5m").strip().lower() or "5m"
MIN_DF30_FOR_SCAN = int(os.getenv("MIN_DF30_FOR_SCAN", "3"))   # 3 bougies 30s = 90s de warmup (réduit de 10)

# ---------------------------------------------------------------------------
# Tolérance proximité OB/FVG — dynamique Fibonacci [0.618–0.786]
# ---------------------------------------------------------------------------
# La tolérance est recalculée à chaque appel d'_has_ob_or_fvg_alignment()
# en fonction du dernier retracement Fibonacci détecté sur le graphique.
# Logique :
#   - On cherche le dernier swing H/L (lookback 60 bougies)
#   - On calcule les niveaux Fib 0.618 et 0.786 de ce swing
#   - La tolérance = 50% de la largeur de cette zone Fib × ATR normalisé
#   - Fallback : ATR × OB_FVG_ATR_MULT si aucun swing propre n'est trouvé
OB_FVG_ATR_MULT = float(os.getenv("OB_FVG_ATR_MULT", "0.702"))   # milieu zone Fib [0.618, 0.786]
OB_FVG_PCT_FALLBACK = float(os.getenv("OB_FVG_PCT_FALLBACK", "0.004"))
# Active le calcul dynamique Fibonacci (désactiver = retour à ATR fixe)
OB_FVG_FIB_DYNAMIC = os.getenv("OB_FVG_FIB_DYNAMIC", "1").strip().lower() in ("1", "true", "yes", "on")
FIB_LEVEL_LOW  = float(os.getenv("FIB_LEVEL_LOW",  "0.618"))
FIB_LEVEL_HIGH = float(os.getenv("FIB_LEVEL_HIGH", "0.786"))
FIB_SWING_LOOKBACK = int(os.getenv("FIB_SWING_LOOKBACK", "60"))

# Seuils RSI — resserrés sur 5m (plus de 1m), double confirmation 5m+10m
RSI_ENTRY_LONG  = float(os.getenv("RSI_ENTRY_LONG",  "30"))
RSI_ENTRY_SHORT = float(os.getenv("RSI_ENTRY_SHORT", "70"))
RSI_TF_PRIMARY  = os.getenv("RSI_TF_PRIMARY", "5m")   # timeframe principal du RSI fallback
RSI_CONFIRM_5M  = os.getenv("RSI_CONFIRM_5M", "1").strip().lower() in ("1", "true", "yes", "on")

# ---------------------------------------------------------------------------
# MODULE 19 — Paramètres par symbole (overrides)
# ---------------------------------------------------------------------------
# Permet d'ajuster les paramètres SL/TP, RSI et scoring par actif.
# Contexte PAXG/USDT (Or) :
#   - ATR absolu élevé (~$25-50 sur 5m) mais volatilité RELATIVE faible
#   - Marché mean-reverting, TPs larges rarement atteints
#   - Backtest 2025 : meilleure config ATR×1.3 / TP(1.2,1.8,2.4), Sharpe -3.679
#   - WR 40.3% → manque de confluence → score minimum 5/7 requis
#   - RSI or : oversold/overbought plus tôt (35/65 au lieu de 30/70)
#
# Format : {sym: {atr_mult, tp_ratios, rsi_long, rsi_short, score_min, rr_sl_pct}}
# rr_sl_pct : pourcentage du prix utilisé comme SL minimum absolu (sécurité floor)

SYM_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "PAXG/USDT": {
        # SL/TP adaptatifs — calibrés sur le backtest Gold 2025
        # Meilleure config brute : ATR×1.3, TP(1.2,1.8,2.4)
        # → on affine : SL légèrement plus serré + TP1 raccourci pour capter les mouvements courts
        "atr_mult":    float(os.getenv("PAXG_ATR_MULT",  "1.2")),   # ← 1.3 → 1.2 (SL plus serré)
        "tp_ratios":   tuple(float(x) for x in os.getenv("PAXG_TP_RATIOS", "1.0,1.5,2.0").split(",")),
        # RSI — Or réagit plus tôt (zones de retournement plus étroites)
        "rsi_long":    float(os.getenv("PAXG_RSI_LONG",  "35")),    # 35 au lieu de 30
        "rsi_short":   float(os.getenv("PAXG_RSI_SHORT", "65")),    # 65 au lieu de 70
        # Score minimum pour déclencher un signal — 5/7 requis (filtre les setups faibles)
        "score_min":   int(os.getenv("PAXG_SCORE_MIN",   "5")),
        # Floor SL absolu en % du prix (évite les SL trop serrés sur prix élevé)
        # Ex: 4985 × 0.0025 = $12.5 minimum de SL pour l'or
        "sl_floor_pct": float(os.getenv("PAXG_SL_FLOOR_PCT", "0.0025")),
    },
    # Extendable : ajouter BTC/USDT, ETH/USDT, SOL/USDT ici si besoin
}

# Configurations OPT dédiées à PAXG (exclut les configs agressives inadaptées à l'or)
OPT_CONFIGURATIONS_PAXG: List[Dict[str, Any]] = [
    # Configs conservatrices — TP courts, SL proportionnels au prix de l'or
    {"atr_mult": 0.8,  "tp_ratios": (0.8, 1.2, 1.6), "trailing": False},
    {"atr_mult": 0.8,  "tp_ratios": (1.0, 1.5, 2.0), "trailing": False},
    {"atr_mult": 1.0,  "tp_ratios": (0.8, 1.2, 1.6), "trailing": False},
    {"atr_mult": 1.0,  "tp_ratios": (1.0, 1.5, 2.0), "trailing": False},
    {"atr_mult": 1.0,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},  # best config backtest
    {"atr_mult": 1.2,  "tp_ratios": (0.8, 1.2, 1.6), "trailing": False},
    {"atr_mult": 1.2,  "tp_ratios": (1.0, 1.5, 2.0), "trailing": False},
    {"atr_mult": 1.2,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},
    {"atr_mult": 1.3,  "tp_ratios": (1.0, 1.5, 2.0), "trailing": False},
    {"atr_mult": 1.3,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},  # best config backtest exact
    {"atr_mult": 1.5,  "tp_ratios": (0.8, 1.2, 1.6), "trailing": False},
    {"atr_mult": 1.5,  "tp_ratios": (1.0, 1.5, 2.0), "trailing": False},
]

def get_sym_override(sym: str, key: str, default: Any = None) -> Any:
    """Retourne le paramètre override pour un symbole, ou la valeur globale par défaut."""
    return (SYM_OVERRIDES.get(sym) or {}).get(key, default)

# ---------------------------------------------------------------------------
# MODULE 18 — Optimisation annuelle multi-actifs
# ---------------------------------------------------------------------------
# OPT_YEAR           : année filtrée pour le backtest (défaut : 2025)
# OPT_REFRESH_HOURS  : relance automatique toutes les N heures (défaut : 24)
# OPT_MIN_CANDLES    : nb minimum de bougies pour lancer l'optimisation
# OPT_FEE_BPS        : commission Binance en bps (cohérent avec adaptive levels)
# OPT_SCORE_CRITERIA : critère de sélection principal
#   "sharpe"       → Sharpe Ratio (recommandé, défaut)
#   "expectancy"   → Espérance mathématique par trade
#   "combined"     → 0.5×Sharpe + 0.3×Expectancy + 0.2×(1−|MaxDD|)
OPT_YEAR            = int(os.getenv("OPT_YEAR",            "2025"))   # conservé pour compatibilité
OPT_REFRESH_HOURS   = int(os.getenv("OPT_REFRESH_HOURS",   "24"))
OPT_MIN_CANDLES     = int(os.getenv("OPT_MIN_CANDLES",     "200"))
OPT_FEE_BPS         = float(os.getenv("OPT_FEE_BPS",       "4"))
OPT_SCORE_CRITERIA  = os.getenv("OPT_SCORE_CRITERIA",      "combined").strip().lower()
# v7 fix: OPT utilise maintenant fetch_klines_history (5m, 30j) comme STRICT
# → plus de dépendance au candle_store 30s (~4h, insuffisant)
OPT_TF              = os.getenv("OPT_TF",              "5m").strip()  # TF pour backtest optim
OPT_IN_SAMPLE_DAYS  = int(os.getenv("OPT_IN_SAMPLE_DAYS", "60"))       # 60j de 5m = ~17280 bougies

# Configurations candidates : toutes combinaisons atr_mult × tp_ratios testées
# [OPT-4] v7: Réduit à 7 configs représentatives (vs 12 précédemment) → -42% de temps de calcul
OPT_CONFIGURATIONS: List[Dict[str, Any]] = [
    {"atr_mult": 0.7,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},
    {"atr_mult": 0.7,  "tp_ratios": (1.5, 2.1, 2.6), "trailing": False},
    {"atr_mult": 1.0,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},
    {"atr_mult": 1.0,  "tp_ratios": (1.5, 2.1, 2.6), "trailing": False},
    {"atr_mult": 1.2,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},
    {"atr_mult": 1.2,  "tp_ratios": (1.5, 2.1, 2.6), "trailing": False},
    {"atr_mult": 1.5,  "tp_ratios": (1.2, 1.8, 2.4), "trailing": False},
]

ENABLE_BRIDGE = os.getenv("ENABLE_BRIDGE", "0").strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# TELEGRAM ALERTS (optionnel)
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_IDS = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
TELEGRAM_MIN_INTERVAL_SEC = int(os.getenv("TELEGRAM_MIN_INTERVAL_SEC", "300"))  # anti-spam par symbole
# Score minimum pour déclencher une alerte (sur 9). Alertes à 5/9 minimum.
TELEGRAM_SCORE_THRESHOLD = float(os.getenv("TELEGRAM_SCORE_THRESHOLD", "5"))
# Labels de qualité par palier de score /9 (v7)
TELEGRAM_SCORE_LABELS: Dict[int, str] = {
    4: "📡 Surveillance",
    5: "✅ Bon setup",
    6: "🔥 Setup fort",
    7: "🚀 Setup optimal",
    8: "💎 Signal premium",
    9: "🌟 Setup parfait",
}

_tg_last_sent: Dict[str, float] = {s: 0.0 for s in SYMBOLS}

async def telegram_send(session: aiohttp.ClientSession, text: str, symbol: str = "", force: bool = False) -> None:
    """Envoie un message Telegram (si configuré).

    - N'envoie rien si TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_IDS manquent.
    - Throttle anti-spam par symbole.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        return

    now = datetime.now(timezone.utc).timestamp()
    if symbol and (not force):
        last = float(_tg_last_sent.get(symbol, 0.0) or 0.0)
        if now - last < TELEGRAM_MIN_INTERVAL_SEC:
            return

    url = "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_BOT_TOKEN)
    payload_base = {
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    for chat_id in TELEGRAM_CHAT_IDS:
        payload = dict(payload_base)
        payload["chat_id"] = chat_id
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                await resp.text()
        except Exception:
            pass

    if symbol:
        _tg_last_sent[symbol] = now

# ---------------------------------------------------------------------------
# WS COMPRESSION (gzip + base64) — optionnel
# ---------------------------------------------------------------------------
# WS_COMPRESS=off            -> JSON texte brut (comportement historique)
# WS_COMPRESS=gzip_base64    -> enveloppe {"_compressed":"gzip+base64", ...} si payload volumineux
WS_COMPRESS = os.getenv("WS_COMPRESS", "off").strip().lower()  # off | gzip_base64
WS_COMPRESS_MIN_BYTES = int(os.getenv("WS_COMPRESS_MIN_BYTES", "25000"))
# Pour optimiser CPU: compresser uniquement certains types (par défaut: signal)
WS_COMPRESS_TYPES = {t.strip().lower() for t in os.getenv("WS_COMPRESS_TYPES", "signal").split(",") if t.strip()}

H4_CACHE_TTL = int(os.getenv("H4_CACHE_TTL", "240"))
M1_CACHE_TTL = int(os.getenv("M1_CACHE_TTL", "30"))
M1_LIMIT = int(os.getenv("M1_LIMIT", "260"))
H4_LIMIT = int(os.getenv("H4_LIMIT", "210"))

# ---------------------------------------------------------------------------
# STRICT INDICATOR (TRIX strict) + Recalibrage (Walk-Forward simplifié)
# ---------------------------------------------------------------------------
# Logique inspirée de l'optimisation / Walk-Forward : recalibrage périodique
# des paramètres sur des bougies 5 minutes, objectif Sharpe.
#
# Notes perf : compromis intermédiaire — 300 iters, 120j in-sample, recalib 120j.
# Résultat : CPU réduit ~25% vs v3, robustesse maintenue sur 4 sous-périodes.
STRICT_TF = os.getenv("STRICT_TF", "5m").strip() or "5m"
STRICT_RECALIB_DAYS = int(os.getenv("STRICT_RECALIB_DAYS", "120"))   # ← 90 → 120j
# Fenêtre d'apprentissage — 120j = ~34k bougies 5m, équilibre fiabilité/vitesse
STRICT_IN_SAMPLE_DAYS = int(os.getenv("STRICT_IN_SAMPLE_DAYS", "120"))  # ← 180 → 120j
# Garde-fou perf: nb max de bougies utilisées pour l'optimisation STRICT.
_env_max_bars = os.getenv("STRICT_MAX_BARS")
if _env_max_bars is None or str(_env_max_bars).strip() == "":
    # 5m ~= 288 bar/jour. Plancher à 20000 (suffisant sur 120j).
    STRICT_MAX_BARS = max(20000, int(STRICT_IN_SAMPLE_DAYS * 288 * 1.1))
else:
    STRICT_MAX_BARS = int(_env_max_bars)
# Nombre d'itérations random search — 300 = intermédiaire (400 trop lent, 200 trop peu)
STRICT_RANDOM_ITERS = int(os.getenv("STRICT_RANDOM_ITERS", "300"))  # ← 400 → 300
# Frais approximatifs (bps) utilisés pendant la calibration Sharpe
STRICT_FEE_BPS = float(os.getenv("STRICT_FEE_BPS", "4"))
# Exiger un minimum de trades pour éviter les Sharpe "fantômes"
STRICT_MIN_TRADES = int(os.getenv("STRICT_MIN_TRADES", "3"))   # 3 = min réaliste (8 trop strict)
STRICT_SHARPE_FLOOR = float(os.getenv("STRICT_SHARPE_FLOOR", "0.2"))  # 0.2 = seuil assoupli (0.5 trop strict)
STRICT_SUBPERIOD_DAYS = int(os.getenv("STRICT_SUBPERIOD_DAYS", "30"))
STRICT_ROBUST_MIN_PERIODS = int(os.getenv("STRICT_ROBUST_MIN_PERIODS", "2"))
STRICT_TOPK_ZONES = int(os.getenv("STRICT_TOPK_ZONES", "3"))
STRICT_ZONE_MIN_DIST = int(os.getenv("STRICT_ZONE_MIN_DIST", "10"))

# Mode PRO: recalibrage STRICT dans un process séparé.
# DESACTIVÉ par défaut (0) — subprocess spawn cause des erreurs SSL/aiohttp sur Windows.
# Activer via STRICT_OFFLOAD_PROCESS=1 en .env si Linux/Mac sans problème SSL.
STRICT_OFFLOAD_PROCESS = os.getenv("STRICT_OFFLOAD_PROCESS", "0").strip().lower() in ("1","true","yes","on")
STRICT_WORKERS = int(os.getenv("STRICT_WORKERS", "1"))
STRICT_JOB_QUEUE_MAX = int(os.getenv("STRICT_JOB_QUEUE_MAX", "64"))

# Cache 5m (pour l'affichage et le strict indicator)
M5_CACHE_TTL = int(os.getenv("M5_CACHE_TTL", "60"))
M5_LIMIT = int(os.getenv("M5_LIMIT", "600"))

def rest_sym(s: str) -> str:
    return s.replace("/", "").upper()

def ws_sym(s: str) -> str:
    return s.replace("/", "").lower()


# ---------------------------------------------------------------------------
# STORES
# Persistance optionnelle du candle_store (debug/restart)
CANDLE_PERSIST = os.getenv('CANDLE_PERSIST', '0').strip().lower() in ('1','true','yes','on')
CANDLE_PERSIST_EVERY_SEC = int(os.getenv('CANDLE_PERSIST_EVERY_SEC', '300'))  # 5 min
CANDLE_PERSIST_FILE = Path(os.getenv('CANDLE_PERSIST_FILE', 'candle_store.json'))
_last_candle_persist_ts = 0.0

async def _persist_candle_store_if_needed():
    global _last_candle_persist_ts
    if not CANDLE_PERSIST:
        return
    now = datetime.now(timezone.utc).timestamp()
    if now - float(_last_candle_persist_ts or 0.0) < CANDLE_PERSIST_EVERY_SEC:
        return
    try:
        payload = {}
        for sym, df in candle_store.items():
            if isinstance(df, pd.DataFrame) and (not df.empty):
                payload[sym] = df.to_json(orient='split', date_format='iso')
        CANDLE_PERSIST_FILE.write_text(json.dumps(payload), encoding='utf-8')
        _last_candle_persist_ts = now
    except Exception as e:
        logger.warning('Persist candle_store failed: %s', e)


# ---------------------------------------------------------------------------
candle_store: Dict[str, pd.DataFrame] = {}
raw_1s: Dict[str, List[dict]] = {}
# Live tick health
_last_tick_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS}
_last_bar_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS}
# Trade bucketing (1s OHLCV)
_trade_bucket: Dict[str, dict] = {}  # {sym:{sec:int,o,h,l,c,v}}
h4_store: Dict[str, pd.DataFrame] = {}
h2_store: Dict[str, pd.DataFrame] = {}
h1_store: Dict[str, pd.DataFrame] = {}
m30_store: Dict[str, pd.DataFrame] = {}
m15_store: Dict[str, pd.DataFrame] = {}
m3_store:  Dict[str, pd.DataFrame] = {}   # ← MODULE 8 : nouveau TF 3m pour entrée précise
m1_store:  Dict[str, pd.DataFrame] = {}
m5_store:  Dict[str, pd.DataFrame] = {}
signals: Dict[str, dict] = {}

# ---------------------------------------------------------------------------
# MODULE 18 — Store des résultats d'optimisation annuelle
# ---------------------------------------------------------------------------
# _best_results_2025[sym] = {
#     'atr_mult', 'tp_ratios', 'trailing',
#     'sharpe', 'expectancy', 'winrate', 'max_drawdown',
#     'trades', 'sample_size', 'return',
#     'computed_at', 'year', 'score_criteria', 'all_results': [...]
# }
_best_results_2025: Dict[str, Dict[str, Any]] = {}
_opt_running: bool = False              # verrou anti-concurrence
_opt_last_run_ts: float = 0.0           # timestamp UNIX du dernier run complet

_h4_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}
_h2_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}
_h1_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}
_m30_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
_m15_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
_m3_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}   # ← cache 3m
_m1_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}
_m5_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}
_1d_cache:  Dict[str, Tuple[float, pd.DataFrame]] = {}   # ← NEW: biais journalier macro

# ---------------------------------------------------------------------------
# STORES supplémentaires — 1D, Futures, Gold (XAU/USD), Delta Volume
# ---------------------------------------------------------------------------
d1_store:       Dict[str, pd.DataFrame] = {}  # bougies daily par symbole

# Futures data {sym: (ts, {"oi": float, "funding": float, "mark_price": float, ...})}
_futures_cache: Dict[str, Tuple[float, dict]] = {}
futures_store:  Dict[str, dict] = {}   # dernières métriques Futures par symbole

# Gold XAU/USD data {tf: (ts, df)} — provider: Twelve Data ou Yahoo Finance
_gold_cache:    Dict[str, Tuple[float, pd.DataFrame]] = {}
gold_store:     Dict[str, pd.DataFrame] = {}   # bougies Gold réel (pour PAXG uniquement)

# Delta Volume (aggTrade) — pression acheteur/vendeur en temps réel
# Structure: {sym: {"buy_vol": float, "sell_vol": float, "delta": float,
#                   "delta_pct": float, "bullish": bool, "bearish": bool}}
_delta_vol: Dict[str, dict] = {
    s: {
        "buy_vol":   0.0,
        "sell_vol":  0.0,
        "delta":     0.0,
        "delta_pct": 0.0,
        "bullish":   False,
        "bearish":   False,
        "trades":    _deque(maxlen=DELTA_VOL_WINDOW),
        "ts":        0.0,
    }
    for s in SYMBOLS
}

# ---------------------------------------------------------------------------
# MODULE 4 — Apprentissage adaptatif : historique des signaux + pondérations
# ---------------------------------------------------------------------------
# signal_history[sym] = liste de résultats de signaux passés :
#   {ts, score, side, confs, entry, sl, tp1, tp2, outcome, outcome_ts}
#   outcome ∈ {"tp1_hit", "tp2_hit", "sl_hit", "expired", "pending"}
#
# scoring_weights[sym] = poids adaptatifs par critère (1.0 = neutre)
#   Les poids sont ajustés à chaque rapport biheure si confirmation humaine OK.
#   Confirmation via /api/learning/confirm (endpoint dédié).

SIGNAL_HISTORY_FILE   = Path(os.getenv("SIGNAL_HISTORY_FILE",   "signal_history.json"))
SCORING_WEIGHTS_FILE  = Path(os.getenv("SCORING_WEIGHTS_FILE",  "scoring_weights.json"))
LEARNING_REPORT_EVERY = int(os.getenv("LEARNING_REPORT_EVERY_SEC", str(2 * 3600)))  # 2h
LEARNING_MIN_SIGNALS  = int(os.getenv("LEARNING_MIN_SIGNALS", "10"))   # min signaux pour adapter
LEARNING_ADAPT_RATE   = float(os.getenv("LEARNING_ADAPT_RATE", "0.05"))  # taux d'adaptation par cycle

# Critères du scoring — noms canoniques (utilisés comme clés de poids)
# v7: score /9 (ajout EMA200_1D + DELTA_VOL)
SCORE_CRITERIA = ["EMA200_H4", "STRUCT_H2H1", "OB_FVG_30M", "OB_FVG_15M_CONFIRM",
                  "REJET_15M", "TRIX_5M", "ALIGN_H2H1",
                  "EMA200_1D",   # ← NEW v7 : biais macro journalier
                  "DELTA_VOL",   # ← NEW v7 : pression acheteur/vendeur aggTrade
                  ]

# Poids adaptatifs par symbole et par critère (initialisés à 1.0)
signal_history:   Dict[str, List[dict]] = {s: [] for s in SYMBOLS}
scoring_weights:  Dict[str, Dict[str, float]] = {
    s: {c: 1.0 for c in SCORE_CRITERIA} for s in SYMBOLS
}
_learning_pending_confirm: Dict[str, dict] = {}   # rapport en attente de confirmation humaine
_last_learning_report_ts: float = 0.0


def _load_learning_state() -> None:
    """Charge signal_history et scoring_weights depuis les fichiers JSON au démarrage."""
    global signal_history, scoring_weights
    try:
        if SIGNAL_HISTORY_FILE.exists():
            raw = json.loads(SIGNAL_HISTORY_FILE.read_text(encoding="utf-8"))
            for sym in SYMBOLS:
                if sym in raw:
                    signal_history[sym] = list(raw[sym])[-500:]  # max 500 entrées par symbole
    except Exception as e:
        logger.warning("[LEARNING] Chargement signal_history échoué: %s", e)
    try:
        if SCORING_WEIGHTS_FILE.exists():
            raw = json.loads(SCORING_WEIGHTS_FILE.read_text(encoding="utf-8"))
            for sym in SYMBOLS:
                if sym in raw:
                    for c in SCORE_CRITERIA:
                        if c in raw[sym]:
                            scoring_weights[sym][c] = float(raw[sym][c])
    except Exception as e:
        logger.warning("[LEARNING] Chargement scoring_weights échoué: %s", e)


def _save_learning_state() -> None:
    """Sauvegarde signal_history et scoring_weights."""
    try:
        SIGNAL_HISTORY_FILE.write_text(
            json.dumps({s: signal_history[s] for s in SYMBOLS}, default=str), encoding="utf-8"
        )
    except Exception as e:
        logger.warning("[LEARNING] Sauvegarde signal_history échouée: %s", e)
    try:
        SCORING_WEIGHTS_FILE.write_text(
            json.dumps(scoring_weights, default=str), encoding="utf-8"
        )
    except Exception as e:
        logger.warning("[LEARNING] Sauvegarde scoring_weights échouée: %s", e)


def record_signal_outcome(sym: str, entry: float, sl: float, tp1: float, current_price: float,
                          ts_signal: str, confs: List[str], score: int, side: str) -> None:
    """Met a jour l outcome des signaux pending avec le prix exact de sortie et les timestamps."""
    hist = signal_history.get(sym, [])
    now_iso = datetime.now(timezone.utc).isoformat()
    for rec in hist:
        if rec.get("outcome") != "pending":
            continue
        ep    = float(rec.get("entry", 0.0) or 0.0)
        sl_r  = float(rec.get("sl",    0.0) or 0.0)
        tp1_r = float(rec.get("tp1",   0.0) or 0.0)
        tp2_r = float(rec.get("tp2",   0.0) or tp1_r)
        tp3_r = float(rec.get("tp3",   0.0) or tp2_r)
        tp4_r = float(rec.get("tp4",   0.0) or tp3_r)
        if ep <= 0:
            continue

        new_outcome: Optional[str] = None
        exit_px:     Optional[float] = None

        if "ACHAT" in rec.get("side", ""):
            # LONG : TP = price monte, SL = price descend
            if tp4_r > 0 and current_price >= tp4_r:
                new_outcome = "tp4_hit"; exit_px = tp4_r
            elif tp3_r > 0 and current_price >= tp3_r:
                new_outcome = "tp3_hit"; exit_px = tp3_r
            elif tp2_r > 0 and current_price >= tp2_r:
                new_outcome = "tp2_hit"; exit_px = tp2_r
            elif tp1_r > 0 and current_price >= tp1_r:
                new_outcome = "tp1_hit"; exit_px = tp1_r
            elif sl_r > 0 and current_price <= sl_r:
                new_outcome = "sl_hit";  exit_px = sl_r
        else:
            # SHORT : TP = price descend, SL = price monte
            if tp4_r > 0 and current_price <= tp4_r:
                new_outcome = "tp4_hit"; exit_px = tp4_r
            elif tp3_r > 0 and current_price <= tp3_r:
                new_outcome = "tp3_hit"; exit_px = tp3_r
            elif tp2_r > 0 and current_price <= tp2_r:
                new_outcome = "tp2_hit"; exit_px = tp2_r
            elif tp1_r > 0 and current_price <= tp1_r:
                new_outcome = "tp1_hit"; exit_px = tp1_r
            elif sl_r > 0 and current_price >= sl_r:
                new_outcome = "sl_hit";  exit_px = sl_r

        if new_outcome:
            rec["outcome"]    = new_outcome
            rec["outcome_ts"] = now_iso
            rec["ts_close"]   = now_iso
            rec["exit_price"] = round(float(exit_px), 8) if exit_px else current_price

    signal_history[sym] = hist


def add_signal_to_history(sym: str, score: int, side: str, confs: List[str],
                          entry: float, sl: float, tps: List[float]) -> None:
    """Ajoute un nouveau signal en statut pending avec tous les champs de cloture."""
    hist = signal_history.setdefault(sym, [])
    now_iso = datetime.now(timezone.utc).isoformat()
    hist.append({
        "ts":         now_iso,   # timestamp ouverture ISO UTC
        "ts_open":    now_iso,   # alias explicite pour la periode
        "ts_close":   None,      # rempli lors de la cloture
        "score":      score,
        "side":       side,
        "confs":      confs,
        "entry":      entry,
        "sl":         sl,
        "tp1":        tps[0] if len(tps) > 0 else None,
        "tp2":        tps[1] if len(tps) > 1 else None,
        "tp3":        tps[2] if len(tps) > 2 else None,
        "tp4":        tps[3] if len(tps) > 3 else None,
        "exit_price": None,   # prix reel de sortie (TP ou SL touche)
        "outcome":    "pending",
        "outcome_ts": None,   # timestamp ISO UTC de cloture
    })
    signal_history[sym] = hist[-500:]


def _compute_learning_report(sym: str) -> dict:
    """Calcule un rapport de performance par critere avec metadonnees de periode."""
    hist = [r for r in signal_history.get(sym, []) if r.get("outcome") != "pending"]
    if len(hist) < LEARNING_MIN_SIGNALS:
        return {"sym": sym, "status": "not_enough_data", "count": len(hist)}

    total  = len(hist)
    wins   = [r for r in hist if r.get("outcome") in ("tp1_hit", "tp2_hit", "tp3_hit", "tp4_hit")]
    losses = [r for r in hist if r.get("outcome") == "sl_hit"]
    win_rate = len(wins) / total if total else 0.0

    # ── Periode analysee ──────────────────────────────────────────────────────
    # ts_open ou ts selon disponibilite (anciens enregistrements n ont que ts)
    def _get_ts(r: dict) -> Optional[str]:
        return r.get("ts_open") or r.get("ts")

    all_ts = [_get_ts(r) for r in hist if _get_ts(r)]
    period_start: Optional[str] = None
    period_end:   Optional[str] = None
    period_days:  Optional[float] = None
    if all_ts:
        all_ts_sorted = sorted(all_ts)
        period_start  = all_ts_sorted[0]
        period_end    = all_ts_sorted[-1]
        try:
            dt_start = datetime.fromisoformat(period_start.replace("Z", "+00:00"))
            dt_end   = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            period_days = round((dt_end - dt_start).total_seconds() / 86400, 1)
        except Exception:
            period_days = None

    # Performance par critere
    crit_stats: Dict[str, dict] = {}
    for crit in SCORE_CRITERIA:
        crit_signals = [r for r in hist if crit in (r.get("confs") or [])]
        if not crit_signals:
            crit_stats[crit] = {"count": 0, "win_rate": None, "suggested_weight": 1.0}
            continue
        crit_wins = [r for r in crit_signals if r.get("outcome") in ("tp1_hit", "tp2_hit", "tp3_hit", "tp4_hit")]
        crit_wr   = len(crit_wins) / len(crit_signals)
        delta     = crit_wr - win_rate
        current_w = scoring_weights[sym].get(crit, 1.0)
        new_w     = max(0.5, min(2.0, current_w + LEARNING_ADAPT_RATE * delta * 10))
        crit_stats[crit] = {
            "count":            len(crit_signals),
            "win_rate":         round(crit_wr, 3),
            "current_weight":   round(current_w, 3),
            "suggested_weight": round(new_w, 3),
        }

    return {
        "sym":          sym,
        "total":        total,
        "wins":         len(wins),
        "losses":       len(losses),
        "win_rate":     round(win_rate, 3),
        "criteria":     crit_stats,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status":       "ready_for_confirm",
        # ── Periode analysee (nouvelles cles) ─────────────────────────────
        "period_start":  period_start,   # ISO UTC premier signal clos
        "period_end":    period_end,     # ISO UTC dernier signal clos
        "period_days":   period_days,    # duree en jours
        "signal_count":  total,          # nombre de signaux analyses
    }


async def learning_report_loop(session: aiohttp.ClientSession) -> None:
    """Génère un rapport d'apprentissage toutes les 2h et l'envoie via Telegram.

    v6 fix : délai initial réduit à 5s (au lieu de 60s) pour que les rapports
    soient disponibles dès le démarrage et que les boutons Appliquer/Rejeter
    fonctionnent immédiatement sans attendre 2h.
    """
    global _last_learning_report_ts
    await asyncio.sleep(5)   # ← 60s → 5s : rapports disponibles dès le démarrage
    while True:
        try:
            now = datetime.now(timezone.utc).timestamp()
            if now - float(_last_learning_report_ts or 0.0) >= LEARNING_REPORT_EVERY:
                _last_learning_report_ts = now
                for sym in SYMBOLS:
                    report = _compute_learning_report(sym)
                    _learning_pending_confirm[sym] = report
                    if report.get("status") != "ready_for_confirm":
                        continue
                    logger.info("[LEARNING] Rapport %s: %s signaux, win_rate=%.1f%%",
                                sym, report.get("total", 0), float(report.get("win_rate", 0)) * 100)
                    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS:
                        crit_lines = []
                        for c, st in (report.get("criteria") or {}).items():
                            if st.get("count", 0) > 0:
                                arrow = "▲" if float(st.get("suggested_weight", 1.0)) > float(st.get("current_weight", 1.0)) else "▼"
                                crit_lines.append(
                                    "• {}: win={:.0f}% {} w:{:.2f}→{:.2f}".format(
                                        c, float(st.get("win_rate", 0) or 0) * 100,
                                        arrow,
                                        float(st.get("current_weight", 1.0)),
                                        float(st.get("suggested_weight", 1.0)),
                                    )
                                )
                        msg = (
                            "<b>📊 Rapport apprentissage — {sym}</b>\n"
                            "Signaux analysés: {total} | Wins: {wins} | Win rate: {wr:.0f}%\n\n"
                            "<b>Critères (poids suggérés)</b>\n{crits}\n\n"
                            "→ Confirmez l'adaptation via /api/learning/confirm/{sym}"
                        ).format(
                            sym=sym, total=report.get("total", 0), wins=report.get("wins", 0),
                            wr=float(report.get("win_rate", 0)) * 100,
                            crits="\n".join(crit_lines) if crit_lines else "—",
                        )
                        await telegram_send(session, msg, symbol=sym, force=True)
                _save_learning_state()
        except Exception as e:
            logger.warning("[LEARNING] report loop error: %s", e)
        await asyncio.sleep(60)

# Watchdog: timestamp de début de job par symbole (pour détecter les jobs bloqués)
_strict_job_start_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS}
STRICT_JOB_TIMEOUT_MIN = int(os.getenv("STRICT_JOB_TIMEOUT_MIN", "15"))  # max 15 min par job (300 iters ~8-12min)

# STRICT params store (par symbole)
# {sym: {params: {...}, sharpe: float, trained_until: iso, next_recalib_ts: float}}
_strict_store: Dict[str, dict] = {}
_strict_job_started_ts: Dict[str, float] = {}
_strict_progress: Dict[str, dict] = {}  # phase/iters timestamps (mode-pro progress events)
_strict_heatmaps: Dict[str, dict] = {}  # heavy grids served via endpoint
_strict_running: Dict[str, bool] = {s: False for s in SYMBOLS}  # compute in progress
_strict_pending: Dict[str, bool] = {s: False for s in SYMBOLS}  # job en file
_strict_job_id: Dict[str, str] = {s: "" for s in SYMBOLS}

ws_clients: Dict[str, Set[WebSocket]] = {s: set() for s in SYMBOLS}

# Verrou léger par symbole pour resample_and_push (évite appels concurrents)
_resample_lock: Dict[str, bool] = {s: False for s in SYMBOLS}

# metrics
_ws_sent_msgs = 0
_ws_sent_bytes = 0
_ws_last_broadcast_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS}


# ---------------------------------------------------------------------------
# VISION (Ollama local)
# ---------------------------------------------------------------------------
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_CHAT_URL = f"{OLLAMA_BASE_URL}/api/chat"

# Qwen2.5VL 3B quantifié est plus rapide et précis que LLaVA sur CPU
VISION_MODEL_PRIMARY = os.getenv("VISION_MODEL_PRIMARY", "qwen2.5vl:3b-q4_K_M")
VISION_MODEL_FALLBACK = os.getenv("VISION_MODEL_FALLBACK", "llava")

VISION_NUM_CTX = int(os.getenv("VISION_NUM_CTX", "2048"))
VISION_KEEP_ALIVE = os.getenv("VISION_KEEP_ALIVE", "300s")
# Ollama exige une unité de durée — on normalise pour éviter l'erreur HTTP 400
if VISION_KEEP_ALIVE and VISION_KEEP_ALIVE.strip() not in ("0", "-1"):
    _kav = VISION_KEEP_ALIVE.strip()
    if _kav.lstrip("-").isdigit() and int(_kav) > 0:
        VISION_KEEP_ALIVE = _kav + "s"
# Timeouts augmentés : LLaVA sur CPU peut prendre 4-5min
VISION_TIMEOUT_PRIMARY  = int(os.getenv("VISION_TIMEOUT_PRIMARY",  "420"))  # 7min (était 3min)
VISION_TIMEOUT_FALLBACK = int(os.getenv("VISION_TIMEOUT_FALLBACK", "120"))  # 2min (était 1min)
# Mode texte-only : si True, n'envoie jamais l'image → analyse contexte algo uniquement (plus rapide)
VISION_TEXT_ONLY = os.getenv("VISION_TEXT_ONLY", "0").strip().lower() in ("1", "true", "yes", "on")

VISION_CACHE_TTL = int(os.getenv("VISION_CACHE_TTL", "300"))
VISION_CACHE_SIZE = int(os.getenv("VISION_CACHE_SIZE", "32"))

SMC_SYSTEM_PROMPT = """Tu es un expert trader SMC (Smart Money Concepts) et analyste technique.
Tu analyses des graphiques en chandeliers japonais ET des données algorithmiques SMC.

RÈGLES ABSOLUES :
1. Réponds UNIQUEMENT avec le JSON brut — pas de texte avant, pas de ```json, pas d'explication.
2. Tous les champs sont obligatoires, même si vide (utilise [] ou null).
3. Prix en float. Confiance entre 0 et 100 (integer).
4. Si aucune image n'est fournie, base ton analyse UNIQUEMENT sur le contexte algo fourni.
5. Ne génère jamais de texte en dehors du JSON.

FORMAT JSON EXACT (respecte l'ordre des clés) :
{"trend":"BULLISH","structure":"...","bias":"LONG","confidence":75,"order_blocks":[{"type":"BULL","top":0.0,"bot":0.0}],"fvg":[{"type":"BULL","top":0.0,"bot":0.0}],"liquidity_zones":[{"price":0.0,"label":"..."}],"candle_patterns":[{"pattern":"...","type":"BULL","price":0.0}],"volume_poc":{"price":0.0,"strength":"MEDIUM","label":"POC"},"supertrend":{"direction":"BULL","value":0.0},"setup":{"valid":false,"reason":"...","entry":null,"sl":null,"tp1":null,"tp2":null,"tp3":null,"rr_ratio":0.0,"entry_type":null},"drawing_instructions":[],"narrative":"..."}
"""

def _strip_data_url(b64: str) -> str:
    if not b64:
        return ""
    b64 = b64.strip()
    if "base64," in b64[:120]:
        return b64.split("base64,", 1)[1].strip()
    return b64

def _extract_json_block(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        parts = t.split("```")
        if len(parts) >= 2:
            t = parts[1].lstrip()
            if t.startswith("json"):
                t = t[4:]
    t = t.strip().rstrip("```").strip()
    i = t.find("{")
    j = t.rfind("}")
    if i >= 0 and j > i:
        return t[i:j+1]
    return t

def _empty_vision(reason: str) -> Dict[str, Any]:
    return {
        "trend": "RANGING",
        "bias": "NEUTRE",
        "confidence": 0,
        "structure": reason,
        "order_blocks": [],
        "fvg": [],
        "liquidity_zones": [],
        "candle_patterns": [],
        "volume_poc": {"price": 0.0, "strength": "MEDIUM", "label": "N/A"},
        "supertrend": {"direction": "BEAR", "value": 0.0},
        "setup": {"valid": False, "reason": reason, "entry": None, "sl": None, "tp1": None, "tp2": None, "tp3": None, "rr_ratio": 0.0, "entry_type": None},
        "drawing_instructions": [],
        "narrative": reason,
        "_source": "ollama_local",
        "_error": reason,
    }

class VisionCache:
    def __init__(self, max_size: int, ttl_s: int):
        self.max_size = max_size
        self.ttl_s = ttl_s
        self._store: Dict[str, Dict[str, Any]] = {}
        self._order: List[str] = []
        self.hits = 0
        self.misses = 0

    @staticmethod
    def make_key(image_b64: str, symbol: str, timeframe: str) -> str:
        raw = f"{image_b64[:4096]}|{symbol.upper()}|{timeframe}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        if self.ttl_s <= 0:
            return None
        v = self._store.get(key)
        if not v:
            self.misses += 1
            return None
        age = datetime.now(timezone.utc).timestamp() - v["_cache_ts"]
        if age > self.ttl_s:
            self._store.pop(key, None)
            if key in self._order:
                self._order.remove(key)
            self.misses += 1
            return None
        if key in self._order:
            self._order.remove(key)
        self._order.append(key)
        self.hits += 1
        out = dict(v)
        out["_cache_hit"] = True
        out["_cache_age_s"] = round(age, 1)
        return out

    def set(self, key: str, value: Dict[str, Any]) -> None:
        if self.ttl_s <= 0:
            return
        if key in self._store and key in self._order:
            self._order.remove(key)
        self._store[key] = dict(value, _cache_ts=datetime.now(timezone.utc).timestamp(), _cache_hit=False)
        self._order.append(key)
        while len(self._order) > self.max_size:
            oldest = self._order.pop(0)
            self._store.pop(oldest, None)

    def stats(self) -> Dict[str, Any]:
        total = self.hits + self.misses
        return {
            "size": len(self._store),
            "max_size": self.max_size,
            "ttl_s": self.ttl_s,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round((self.hits / total * 100.0), 1) if total else 0.0,
        }

_vision_cache = VisionCache(VISION_CACHE_SIZE, VISION_CACHE_TTL)

async def _ollama_chat(session: aiohttp.ClientSession, model: str, user_text: str, image_b64: str, timeout_s: int) -> str:
    # Construction du message user — images optionnelles si image_b64 non vide
    user_msg: dict = {"role": "user", "content": user_text}
    if image_b64 and len(image_b64) > 100:
        user_msg["images"] = [image_b64]

    payload = {
        "model": model,
        "stream": False,
        "keep_alive": VISION_KEEP_ALIVE,
        "options": {"num_ctx": VISION_NUM_CTX},
        "messages": [
            {"role": "system", "content": SMC_SYSTEM_PROMPT},
            user_msg,
        ],
    }
    async with session.post(
        OLLAMA_CHAT_URL,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=timeout_s),
    ) as r:
        if r.status >= 400:
            body_err = await r.text()
            raise RuntimeError(f"Ollama HTTP {r.status}: {body_err[:200]}")
        data = await r.json(content_type=None)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data["error"]))
    content = ((data.get("message") or {}).get("content")) or data.get("response") or ""
    if not content:
        # Log le payload complet pour debug (tronqué à 300 chars)
        logger.warning("Ollama response vide (model=%s) — payload reçu: %s", model, str(data)[:300])
        raise RuntimeError(f"Ollama response vide (model={model}) — vérifie les logs du serveur Ollama")
    return content

async def ollama_vision_analyze(session: aiohttp.ClientSession, image_b64: str, symbol: str, timeframe: str, price: float, side_hint: Optional[str], algo_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    t0 = datetime.now(timezone.utc).timestamp()
    image_b64 = _strip_data_url(image_b64)

    # Mode texte-only : ignore l'image si VISION_TEXT_ONLY=1 ou si image vide
    if VISION_TEXT_ONLY:
        image_b64 = ""

    key = VisionCache.make_key(image_b64, symbol, timeframe)
    cached = _vision_cache.get(key)
    if cached:
        return cached

    algo_context = algo_context or {}
    has_image = bool(image_b64 and len(image_b64) > 100)
    # Fix: score_max /9 (v7) — valeur par défaut corrigée de 7 → 9
    score_max = int(algo_context.get("score_max", 9))
    score_val = algo_context.get("score", "N/A")

    if has_image:
        img_note = "Analyse le graphique en chandeliers japonais fourni."
    else:
        img_note = "Aucune image — analyse UNIQUEMENT via le contexte algorithmique SMC ci-dessous."

    # Contexte algo compact (évite de dépasser num_ctx)
    ctx_lines = [
        f"Actif: {symbol} | TF: {timeframe} | Prix: {price}",
        f"Direction hint: {side_hint or 'N/A'} | Score SMC: {score_val}/{score_max}",
        f"Structure H2: {algo_context.get('struct_h2','N/A')} | H1: {algo_context.get('struct_h1','N/A')}",
        f"OB 30m: {algo_context.get('ob_status_30m','N/A')} qualité={algo_context.get('ob_quality_30m','N/A')}",
        f"FVG⊂OB: {algo_context.get('ob_fvg_15m_ok', False)} | RSI 5m: {algo_context.get('rsi_5m','N/A')}",
        f"Range 30m: {algo_context.get('range_30m', False)} | Biais 1D: {algo_context.get('d1_bias_ok', False)}",
        f"Confluences: {', '.join(algo_context.get('confs', []) or [])}",
    ]
    if algo_context.get("entry") and algo_context.get("sl"):
        ctx_lines.append(f"Entrée={algo_context['entry']:.4f} | SL={algo_context['sl']:.4f}")
    ctx_str = "\n".join(ctx_lines)

    user_prompt = f"""{img_note}

=== CONTEXTE SMC ALGORITHMIQUE ===
{ctx_str}
UTC: {datetime.now(timezone.utc).strftime("%H:%M")}

Réponds UNIQUEMENT avec le JSON brut — aucun texte avant ou après les accolades.
"""

    async def _try(model_name: str, timeout_s: int, with_image: bool = True) -> Dict[str, Any]:
        img = image_b64 if (with_image and has_image) else ""
        raw = await _ollama_chat(session, model_name, user_prompt, img, timeout_s=timeout_s)
        raw_json = _extract_json_block(raw)
        out = json.loads(raw_json)
        out["_source"] = "ollama_local"
        out["_model"] = model_name
        out["_symbol"] = symbol
        out["_timeframe"] = timeframe
        out["_ts"] = datetime.now(timezone.utc).isoformat()
        out["_elapsed_s"] = round(datetime.now(timezone.utc).timestamp() - t0, 2)
        out["_text_only"] = not (with_image and has_image)
        return out

    result = None

    # ── Tentative 1 : modèle primaire avec image ──────────────────
    try:
        result = await _try(VISION_MODEL_PRIMARY, VISION_TIMEOUT_PRIMARY, with_image=True)
        logger.info("Vision OK (primary=%s) en %.1fs", VISION_MODEL_PRIMARY, result.get("_elapsed_s", 0))
    except Exception as e1:
        elapsed1 = round(datetime.now(timezone.utc).timestamp() - t0, 1)
        logger.warning("Vision primary failed (%s) après %.1fs: %s", VISION_MODEL_PRIMARY, elapsed1, str(e1)[:120])

        # ── Tentative 2 : modèle fallback avec image ──────────────
        try:
            result = await _try(VISION_MODEL_FALLBACK, VISION_TIMEOUT_FALLBACK, with_image=True)
            logger.info("Vision OK (fallback=%s) en %.1fs", VISION_MODEL_FALLBACK, result.get("_elapsed_s", 0))
        except Exception as e2:
            elapsed2 = round(datetime.now(timezone.utc).timestamp() - t0, 1)
            logger.warning("Vision fallback failed (%s) après %.1fs: %s", VISION_MODEL_FALLBACK, elapsed2, str(e2)[:120])

            # ── Tentative 3 : mode texte-only (sans image) ────────
            # Beaucoup plus rapide — contourne le timeout image
            if has_image:
                try:
                    logger.info("Vision: essai mode texte-only (sans image)...")
                    result = await _try(VISION_MODEL_FALLBACK, VISION_TIMEOUT_FALLBACK, with_image=False)
                    logger.info("Vision OK (text-only fallback=%s) en %.1fs",
                                VISION_MODEL_FALLBACK, result.get("_elapsed_s", 0))
                except Exception as e3:
                    elapsed3 = round(datetime.now(timezone.utc).timestamp() - t0, 1)
                    logger.warning("Vision text-only failed après %.1fs: %s", elapsed3, str(e3)[:120])

            if result is None:
                # Construire un message d'erreur informatif
                err_msg = str(e2)[:200]
                is_timeout = "timeout" in err_msg.lower() or elapsed2 > VISION_TIMEOUT_FALLBACK - 5
                if is_timeout:
                    hint = (
                        f"Timeout ({elapsed2:.0f}s) — LLaVA trop lent sur CPU ({elapsed1:.0f}s). "
                        f"Solutions : (1) VISION_TEXT_ONLY=1 dans .env pour analyse sans image, "
                        f"(2) VISION_MODEL_PRIMARY=qwen2.5vl:3b-q4_K_M, "
                        f"(3) VISION_TIMEOUT_PRIMARY={max(VISION_TIMEOUT_PRIMARY, int(elapsed1*1.5))} dans .env."
                    )
                else:
                    hint = f"Erreur Vision: {err_msg}. Vérifiez qu'Ollama est démarré (ollama serve) et qu'un modèle est installé (ollama pull llava)."
                result = _empty_vision(hint)

    if result is None:
        result = _empty_vision("Vision: résultat vide inattendu")

    result["_symbol"] = symbol
    _vision_cache.set(key, result)
    return result


# ---------------------------------------------------------------------------
# SCORING MTF SMC (H4 → H2 → H1 → 30m → 15m → 5m)
# ---------------------------------------------------------------------------
def compute_ema200(series: pd.Series) -> float:
    return float(series.ewm(span=200, adjust=False).mean().iloc[-1])

# ---------------------------------------------------------------------------
# Helpers MTF SMC
# ---------------------------------------------------------------------------

def _detect_market_structure(df: pd.DataFrame, n: int = 20) -> str:
    """Détecte la structure de marché (BOS haussier/baissier) sur les N dernières bougies.

    v7 — CHoCH/BOS amélioré :
      - Validation par CLOSE (pas seulement les wicks) pour éviter les faux signaux.
      - Displacement minimum : le BOS doit être confirmé par un corps > 0.5 × ATR.
      - Algorithme en 3 passes : pivots → BOS/CHoCH → structure finale.
    """
    if len(df) < n:
        return "RANGING"
    tail = df.tail(n).copy()
    highs  = tail["high"].values.astype(float)
    lows   = tail["low"].values.astype(float)
    closes = tail["close"].values.astype(float)
    opens  = tail["open"].values.astype(float)

    # Calcul ATR pour le filtre displacement
    atr_val = 0.0
    try:
        atr_s = ta.atr(tail["high"], tail["low"], tail["close"], 14)
        if atr_s is not None and not atr_s.empty and not pd.isna(atr_s.iloc[-1]):
            atr_val = float(atr_s.iloc[-1])
    except Exception:
        pass

    # ── Détection des pivots (swing high / swing low) ───────────────────────
    pivot_highs: List[Tuple[int, float]] = []   # (index, valeur)
    pivot_lows:  List[Tuple[int, float]] = []

    for i in range(1, len(highs) - 1):
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            pivot_highs.append((i, highs[i]))
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            pivot_lows.append((i, lows[i]))

    if len(pivot_highs) < 2 or len(pivot_lows) < 2:
        return "RANGING"

    ph = [v for _, v in pivot_highs]
    pl = [v for _, v in pivot_lows]

    hh = ph[-1] > ph[-2]
    hl = pl[-1] > pl[-2]
    lh = ph[-1] < ph[-2]
    ll = pl[-1] < pl[-2]

    # ── Validation BOS par close (pas seulement par wick) ───────────────────
    # Un BOS haussier valide = le prix a clôturé AU-DESSUS du dernier pivot high
    last_ph_val = ph[-1]  # niveau du dernier pivot high
    last_pl_val = pl[-1]  # niveau du dernier pivot low

    bos_bull_by_close = any(c > last_ph_val for c in closes[-5:])  # close > dernier PH
    bos_bear_by_close = any(c < last_pl_val for c in closes[-5:])  # close < dernier PL

    # ── Filtre displacement — corps de bougie > 0.5 ATR ─────────────────────
    displacement_bull = False
    displacement_bear = False
    if atr_val > 0:
        for i in range(max(0, len(closes) - 5), len(closes)):
            body = abs(closes[i] - opens[i])
            if body >= 0.5 * atr_val:
                if closes[i] > opens[i]:
                    displacement_bull = True
                else:
                    displacement_bear = True

    # ── Structure finale ─────────────────────────────────────────────────────
    bull_structure = hh and hl and bos_bull_by_close and displacement_bull
    bear_structure = lh and ll and bos_bear_by_close and displacement_bear

    # Fallback souple (sans displacement) si structure confirmée par pivots + close
    if not bull_structure and not bear_structure:
        bull_structure = hh and hl and bos_bull_by_close
        bear_structure = lh and ll and bos_bear_by_close

    if bull_structure and not bear_structure:
        return "BULLISH"
    if bear_structure and not bull_structure:
        return "BEARISH"
    return "RANGING"

def _detect_rejection_candle(df: pd.DataFrame, side: str) -> bool:
    """Détecte une bougie de retournement/confirmation (engulf, pin bar, msb) sur le TF donné."""
    if len(df) < 3:
        return False
    a = df.iloc[-2]
    b = df.iloc[-1]
    a_o, a_c = float(a.open), float(a.close)
    b_o, b_c, b_h, b_l = float(b.open), float(b.close), float(b.high), float(b.low)
    b_range = b_h - b_l
    if b_range < 1e-12:
        return False
    body = abs(b_c - b_o)
    lower_wick = (min(b_o, b_c) - b_l)
    upper_wick = (b_h - max(b_o, b_c))

    if "ACHAT" in side:
        # Pin bar / hammer haussier
        if lower_wick > 2.0 * body and b_c > b_o:
            return True
        # Engulfing haussier
        if b_c > b_o and a_c < a_o and b_c >= a_o and b_o <= a_c:
            return True
        # Bougie forte haussière (corps > 60% de la range)
        if b_c > b_o and body > 0.6 * b_range:
            return True
    else:
        # Shooting star / pin bar baissier
        if upper_wick > 2.0 * body and b_c < b_o:
            return True
        # Engulfing baissier
        if b_c < b_o and a_c > a_o and b_o >= a_c and b_c <= a_o:
            return True
        # Bougie forte baissière
        if b_c < b_o and body > 0.6 * b_range:
            return True
    return False

def _compute_fib_tolerance(df: pd.DataFrame, price: float, atr_val: float) -> float:
    """Calcule la tolérance dynamique basée sur le retracement Fibonacci [0.618–0.786].

    Logique :
      1. Détecte le dernier swing haut/bas sur FIB_SWING_LOOKBACK bougies.
      2. Calcule les niveaux Fib 0.618 et 0.786 de ce swing.
      3. Tolérance = 50% de la largeur de la zone [Fib618, Fib786].
      4. Fallback : ATR × OB_FVG_ATR_MULT si swing trop petit ou indéfini.
    """
    fallback_tol = max(abs(price) * float(OB_FVG_PCT_FALLBACK), atr_val * float(OB_FVG_ATR_MULT))
    if not OB_FVG_FIB_DYNAMIC or len(df) < 10:
        return fallback_tol
    try:
        tail = df.tail(int(FIB_SWING_LOOKBACK))
        swing_high = float(tail["high"].max())
        swing_low  = float(tail["low"].min())
        swing_range = swing_high - swing_low
        if swing_range < 1e-9:
            return fallback_tol
        # Zone Fibonacci [0.618, 0.786] depuis le bas du swing
        fib_618 = swing_low + FIB_LEVEL_LOW  * swing_range
        fib_786 = swing_low + FIB_LEVEL_HIGH * swing_range
        zone_width = abs(fib_786 - fib_618)
        # Tolérance = 50% de la zone Fib, bornée à [fallback, swing_range * 0.15]
        tol = zone_width * 0.5
        return max(fallback_tol, min(tol, swing_range * 0.15))
    except Exception:
        return fallback_tol


def _ob_status(df: pd.DataFrame, ob_top: float, ob_bot: float, side: str,
               lookback_after: int = 20) -> str:
    """Analyse le statut d'un Order Block : intact / tested / broken.

    - intact  : prix n'a jamais pénétré la zone depuis sa formation.
    - tested  : prix a touché la zone au moins une fois mais sans clore dedans/au-delà.
    - broken  : une bougie a clôturé au-delà de la zone (OB épuisé / invalidé).

    Retourne : 'intact' | 'tested' | 'broken'
    """
    if df is None or df.empty or len(df) < 3:
        return "intact"
    tail = df.tail(int(lookback_after))
    for i in range(len(tail)):
        c = float(tail.iloc[i]["close"])
        lo = float(tail.iloc[i]["low"])
        hi = float(tail.iloc[i]["high"])
        if "ACHAT" in side:
            # OB bull : cassé si close < ob_bot
            if c < ob_bot:
                return "broken"
            if lo <= ob_top and hi >= ob_bot:
                return "tested"
        else:
            # OB bear : cassé si close > ob_top
            if c > ob_top:
                return "broken"
            if hi >= ob_bot and lo <= ob_top:
                return "tested"
    return "intact"


def _detect_range_30m(df_m30: pd.DataFrame, lookback: int = 20) -> bool:
    """Détecte si le marché est en range (consolidation) sur le 30m.

    Critère : ATR < 30% de l'ATR moyen sur la fenêtre → range confirmé.
    Signal supplémentaire fourni dans le contexte algo (non bloquant pour le score).
    """
    if df_m30 is None or len(df_m30) < lookback:
        return False
    try:
        atr_s = ta.atr(df_m30["high"], df_m30["low"], df_m30["close"], 14)
        if atr_s is None or atr_s.empty:
            return False
        recent = float(atr_s.iloc[-1])
        avg = float(atr_s.tail(lookback).mean())
        return avg > 0 and (recent / avg) < 0.30
    except Exception:
        return False


def _has_ob_or_fvg_alignment(df: pd.DataFrame, side: str, price: float, lookback: int = 50) -> Tuple[bool, str, float]:
    """Vérifie si le prix actuel est dans/proche d'un OB ou FVG.

    Retourne (aligned: bool, ob_status: str, ob_quality: float 0-1).

    Améliorations v4 :
      - Tolérance dynamique Fibonacci [0.618–0.786].
      - Analyse du statut de chaque OB (intact / tested / broken).
      - Ne valide PAS un OB broken — bascule sur l'OB suivant.
      - Score de qualité de l'OB (1.0=intact, 0.6=tested, 0.0=broken).
      - Double confirmation : FVG présent dans l'OB = qualité ×1.2 (max 1.0).
    """
    if len(df) < 5 or price <= 0:
        return False, "none", 0.0

    tail = df.tail(int(lookback)).copy()

    # Calcul ATR pour la tolérance
    atr_val = abs(price) * float(OB_FVG_PCT_FALLBACK)
    try:
        atr_s = ta.atr(tail["high"], tail["low"], tail["close"], 14)
        if atr_s is not None and (not atr_s.empty) and (not pd.isna(atr_s.iloc[-1])):
            atr_val = float(atr_s.iloc[-1])
    except Exception:
        pass

    tol = _compute_fib_tolerance(tail, price, atr_val)

    # --- Détection FVGs (toutes, pour double confirmation) ---
    fvg_zones: List[Tuple[float, float]] = []
    for i in range(2, len(tail)):
        h0 = float(tail.iloc[i - 2]["high"])
        l0 = float(tail.iloc[i - 2]["low"])
        l2 = float(tail.iloc[i]["low"])
        h2 = float(tail.iloc[i]["high"])
        if "ACHAT" in side and h0 < l2:
            fvg_zones.append((h0, l2))   # FVG bull : [h0, l2]
        elif "VENTE" in side and l0 > h2:
            fvg_zones.append((h2, l0))   # FVG bear : [h2, l0]

    # Vérifier si price est dans un FVG
    for fvg_bot, fvg_top in fvg_zones:
        mid = (fvg_bot + fvg_top) / 2.0
        if abs(price - mid) <= tol:
            return True, "fvg", 0.85

    # --- OB scan (du plus récent au plus ancien) ---
    obs_found: List[Tuple[float, float, int]] = []  # (top, bot, bar_index)
    for i in range(len(tail) - 2, 0, -1):
        c = float(tail.iloc[i]["close"])
        o = float(tail.iloc[i]["open"])
        h = float(tail.iloc[i]["high"])
        low_ = float(tail.iloc[i]["low"])
        if "ACHAT" in side and c < o:     # bougie baissière avant move haussier = OB bull
            obs_found.append((o, low_, i))
        elif "VENTE" in side and c > o:   # bougie haussière avant move baissier = OB bear
            obs_found.append((h, c, i))

    for ob_top, ob_bot, bar_i in obs_found:
        # Vérifier si price est dans/proche de cet OB
        in_zone = (ob_bot - tol) <= price <= (ob_top + tol)
        if not in_zone:
            continue

        # Analyser le statut de l'OB (bougies APRÈS sa formation)
        df_after = tail.iloc[bar_i + 1:].copy() if bar_i + 1 < len(tail) else pd.DataFrame()
        status = _ob_status(df_after, ob_top, ob_bot, side)

        if status == "broken":
            # OB cassé : on continue vers le prochain OB
            continue

        # Qualité de base selon statut
        quality = 1.0 if status == "intact" else 0.65

        # Double confirmation : FVG contenu dans l'OB → bonus qualité
        fvg_in_ob = any(
            ob_bot <= fvg_bot and fvg_top <= ob_top
            for fvg_bot, fvg_top in fvg_zones
        )
        if fvg_in_ob:
            quality = min(1.0, quality * 1.2)

        return True, status, round(quality, 3)

    return False, "none", 0.0


def score_setup(
    df_h4: pd.DataFrame,
    df_1m: pd.DataFrame,
    df30: pd.DataFrame,
    df_h2: Optional[pd.DataFrame] = None,
    df_h1: Optional[pd.DataFrame] = None,
    df_m30: Optional[pd.DataFrame] = None,
    df_m15: Optional[pd.DataFrame] = None,
    df_m5: Optional[pd.DataFrame] = None,
    strict_params: Optional[dict] = None,
    scoring_w: Optional[Dict[str, float]] = None,
    rsi_long_override:  Optional[float] = None,
    rsi_short_override: Optional[float] = None,
    # v7 — nouveaux paramètres
    df_1d: Optional[pd.DataFrame] = None,
    delta_vol_state: Optional[dict] = None,
    futures_data: Optional[dict] = None,
    # v8 — TF actif (pour adapter les lookbacks et la TF de confirmation)
    active_tf: Optional[str] = None,
) -> Tuple[int, str, List[str], Dict[str, Any]]:
    """Scoring multi-timeframe SMC — score /9 (v7/v8).

    Critères (9 points max) :
      1. EMA200(H4) — Biais directeur H4                               [+1]
      2. Structure H2/H1 — BOS aligné avec le biais                   [+1]
      2b. BONUS : H2 ET H1 tous deux alignés avec le biais            [+1]
      3. OB/FVG sur 30m — Zone institutionnelle proche du prix        [+1]
      3b. BONUS : double confirmation OB/FVG 30m ET 15m               [+1]
      4. Bougie de confirmation — TF adapté selon active_tf           [+1]
      5. Signal Trix Strict 5m — Entrée optimale (ou RSI fallback)    [+1]
      6. EMA200(1D) — Biais macro journalier aligné              [NEW +1]
      7. Delta Volume — Pression acheteur/vendeur aggTrade       [NEW +1]

    Poids adaptatifs (scoring_w) : chaque critère peut être pondéré
    différemment selon l'historique d'apprentissage (Module 4).
    Le score pondéré est arrondi à l'entier le plus proche, max 9.

    v8 — Adaptation par TF (active_tf) :
      Pour limiter les faux signaux, les lookbacks OB/FVG et la TF de
      confirmation du rejet sont recalibrés selon le TF actif :

      TF actif  | TF confirmation | OB lookback 30m | OB lookback 15m | Attente equiv.
      ----------|-----------------|-----------------|-----------------|---------------
      1m        | 5m              | 30              | 20              | 5m
      3m        | 15m             | 40              | 25              | 15m
      5m        | 45m (resample)  | 50              | 30              | 45m
      15m       | 1h  (resample)  | 60              | 40              | 1h
      30m       | 2h  (resample)  | 80              | 50              | 2h
      1h        | 4h              | 100             | 60              | 4h
      4h        | 1d              | 120             | 80              | 1d
      (défaut)  | 15m             | 60              | 40              |
    """
    # ── Mapping TF actif → (ob_lookback_30m, ob_lookback_15m, confirm_resample) ─
    # confirm_resample = None → utiliser df_m15 directement
    # confirm_resample = "Xmin" → resample df_m5 vers cette resolution
    TF_SCORING_MAP: Dict[str, Tuple[int, int, Optional[str]]] = {
        "1m":  (30,  20,  None),       # confirmation sur 5m (df_m5 direct)
        "3m":  (40,  25,  None),       # confirmation sur 15m (df_m15 direct)
        "5m":  (50,  30,  "45min"),    # confirmation resamplee 45m depuis df_m5
        "15m": (60,  40,  "60min"),    # confirmation resamplee 1h  depuis df_m5
        "30m": (80,  50,  "120min"),   # confirmation resamplee 2h  depuis df_m5
        "1h":  (100, 60,  None),       # confirmation sur H4 (df_h4 direct)
        "4h":  (120, 80,  None),       # confirmation sur 1D (df_1d direct)
    }
    _tf = (active_tf or ACTIVE_TF).lower().strip()
    _ob_lb_30, _ob_lb_15, _confirm_resample = TF_SCORING_MAP.get(_tf, (60, 40, None))

    # ── DataFrame de confirmation adapte au TF actif ───────────────────────────
    # Pour les TF courts (1m/3m/15m) : df_m15 ou df_m5 direct
    # Pour les TF intermediaires (5m/15m/30m) : resample df_m5
    # Pour H1 : df_h4 ; pour H4 : df_1d
    def _get_confirm_df() -> Optional[pd.DataFrame]:
        if _confirm_resample and df_m5 is not None and len(df_m5) >= 20:
            try:
                df_res = df_m5.resample(_confirm_resample).agg({
                    "open": "first", "high": "max", "low": "min",
                    "close": "last", "v": "sum"
                }).dropna()
                return df_res if len(df_res) >= 3 else None
            except Exception:
                return None
        if _tf == "1m":
            return df_m5   # confirmation sur 5m
        if _tf == "1h":
            return df_h4   # confirmation sur H4
        if _tf == "4h":
            return df_1d   # confirmation sur 1D
        return df_m15      # défaut : 15m

    df_confirm = _get_confirm_df()
    _confirm_label = {
        "1m": "5m", "3m": "15m", "5m": "45m", "15m": "1h",
        "30m": "2h", "1h": "4h", "4h": "1d",
    }.get(_tf, "15m")

    confs: List[str] = []
    score_raw = 0
    w = scoring_w or {}

    if len(df_h4) < 5:
        return 0, "N/A", [], {}

    # ── Critère 1 : Tendance H4 (EMA200) ─────────────────────────────────────
    h4_close = float(df_h4["close"].iloc[-1])
    ema200_h4 = compute_ema200(df_h4["close"])
    side = "ACHAT [LONG]" if h4_close > ema200_h4 else "VENTE [SHORT]"
    score_raw += float(w.get("EMA200_H4", 1.0))
    confs.append("EMA200(H4)")

    # ── Critère 2 : Structure HTF (H2 + H1) ──────────────────────────────────
    struct_confirmed = False
    struct_h2 = "RANGING"
    struct_h1 = "RANGING"
    if df_h2 is not None and len(df_h2) >= 10:
        struct_h2 = _detect_market_structure(df_h2, n=min(20, len(df_h2) - 1))
    if df_h1 is not None and len(df_h1) >= 10:
        struct_h1 = _detect_market_structure(df_h1, n=min(20, len(df_h1) - 1))

    bull_ok = "ACHAT" in side
    bear_ok = "VENTE" in side
    if (bull_ok and struct_h2 == "BULLISH") or (bear_ok and struct_h2 == "BEARISH"):
        struct_confirmed = True
        confs.append("STRUCT(H2)")
    elif (bull_ok and struct_h1 == "BULLISH") or (bear_ok and struct_h1 == "BEARISH"):
        struct_confirmed = True
        confs.append("STRUCT(H1)")

    if struct_confirmed:
        score_raw += float(w.get("STRUCT_H2H1", 1.0))

    # Critère 2b : BONUS H2+H1 alignés
    align_bonus = False
    if (bull_ok and struct_h2 == "BULLISH" and struct_h1 == "BULLISH") or \
       (bear_ok and struct_h2 == "BEARISH" and struct_h1 == "BEARISH"):
        align_bonus = True
        score_raw += float(w.get("ALIGN_H2H1", 1.0))
        confs.append("ALIGN(H2+H1)")

    # ── Critère 3 : OB / FVG — lookback adapte au TF actif ───────────────────
    price_now = h4_close
    if df30 is not None and len(df30) >= 10:
        price_now = float(df30["close"].iloc[-1])

    ob_fvg_ok = False
    ob_status_30m = "none"
    ob_quality_30m = 0.0

    if df_m30 is not None and len(df_m30) >= 5:
        ob_fvg_ok, ob_status_30m, ob_quality_30m = _has_ob_or_fvg_alignment(
            df_m30, side, price_now, lookback=_ob_lb_30)
        if ob_fvg_ok:
            confs.append("OB/FVG(30m)[{}]".format(ob_status_30m))
    if not ob_fvg_ok and df30 is not None and len(df30) >= 5:
        ob_fvg_ok, ob_status_30m, ob_quality_30m = _has_ob_or_fvg_alignment(
            df30, side, price_now, lookback=_ob_lb_30 * 2)
        if ob_fvg_ok:
            confs.append("OB/FVG(30s)[{}]".format(ob_status_30m))

    if ob_fvg_ok:
        score_raw += float(w.get("OB_FVG_30M", 1.0)) * max(0.5, ob_quality_30m)

    # Critère 3b : BONUS double confirmation OB/FVG — lookback adapte
    ob_fvg_15m_ok = False
    ob_quality_15m = 0.0
    if ob_fvg_ok and df_m15 is not None and len(df_m15) >= 5:
        ob_fvg_15m_ok, ob_status_15m, ob_quality_15m = _has_ob_or_fvg_alignment(
            df_m15, side, price_now, lookback=_ob_lb_15)
        if ob_fvg_15m_ok:
            score_raw += float(w.get("OB_FVG_15M_CONFIRM", 1.0)) * max(0.5, ob_quality_15m)
            confs.append("OB/FVG(15m)[{}]".format(ob_status_15m))

    # ── Critère 4 : Bougie de confirmation — TF adapte au TF actif ───────────
    rejection_ok = False
    if df_confirm is not None and len(df_confirm) >= 3:
        rejection_ok = _detect_rejection_candle(df_confirm, side)
        if rejection_ok:
            confs.append("REJET({})".format(_confirm_label))
    # Fallback : df_m15 si le TF confirme n'a pas donne de rejet
    if not rejection_ok and df_m15 is not None and len(df_m15) >= 3 and df_confirm is not df_m15:
        rejection_ok = _detect_rejection_candle(df_m15, side)
        if rejection_ok:
            confs.append("REJET(15m)")
    if not rejection_ok and df_m30 is not None and len(df_m30) >= 3:
        rejection_ok = _detect_rejection_candle(df_m30, side)
        if rejection_ok:
            confs.append("REJET(30m)")
    if not rejection_ok and df30 is not None and len(df30) >= 3:
        last_o = float(df30["open"].iloc[-1])
        last_c = float(df30["close"].iloc[-1])
        rng = max(float(df30["high"].iloc[-1]) - float(df30["low"].iloc[-1]), 1e-12)
        strong = abs(last_c - last_o) > 0.55 * rng
        ema20 = ta.ema(df30["close"], length=20)
        ema20_val = float(ema20.iloc[-1]) if ema20 is not None and not ema20.empty else None
        if strong and ema20_val:
            if ("ACHAT" in side and last_c > ema20_val) or ("VENTE" in side and last_c < ema20_val):
                rejection_ok = True
                confs.append("MOM+EMA20(30s)")
    if rejection_ok:
        score_raw += float(w.get("REJET_15M", 1.0))

    # ── Critère 5 : Signal TRIX Strict 5m (ou RSI 5m+10m fallback) ───────────
    entry_signal = False
    if df_m5 is not None and len(df_m5) >= 50 and isinstance(strict_params, dict):
        try:
            d5 = strict_trix_apply(df_m5.tail(500), strict_params)
            if not d5.empty:
                last5 = d5.iloc[-1]
                in_trend = bool(float(last5.get("close", 0.0) or 0.0) > float(last5.get("trend_ma", 1e18) or 1e18))
                entry_signal = bool(last5.get("entry_long", False)) and in_trend and "ACHAT" in side
                if not entry_signal:
                    entry_signal = bool(last5.get("exit_long", False)) and not in_trend and "VENTE" in side
                if entry_signal:
                    confs.append("TRIX(5m)")
        except Exception:
            pass

    # Fallback RSI 5m + confirmation 10m (resample depuis 5m)
    if not entry_signal and df_m5 is not None and len(df_m5) >= 20:
        poi_ok = bool(ob_fvg_ok or rejection_ok)
        if poi_ok:
            rsi5 = ta.rsi(df_m5["close"], 14)
            rsi5_val = float(rsi5.iloc[-1]) if rsi5 is not None and not rsi5.empty else 50.0
            # RSI 10m : resample 5m → 10m
            rsi10_val = rsi5_val
            try:
                df10 = df_m5.resample("10min").agg({"open": "first", "high": "max", "low": "min", "close": "last", "v": "sum"}).dropna()
                if len(df10) >= 14:
                    rsi10_s = ta.rsi(df10["close"], 14)
                    if rsi10_s is not None and not rsi10_s.empty:
                        rsi10_val = float(rsi10_s.iloc[-1])
            except Exception:
                pass

            if "ACHAT" in side and rsi5_val <= (rsi_long_override or RSI_ENTRY_LONG) and rsi10_val <= (rsi_long_override or RSI_ENTRY_LONG) + 5:
                entry_signal = True
                confs.append("RSI-OK(5m+10m)")
            elif "VENTE" in side and rsi5_val >= (rsi_short_override or RSI_ENTRY_SHORT) and rsi10_val >= (rsi_short_override or RSI_ENTRY_SHORT) - 5:
                entry_signal = True
                confs.append("RSI-OK(5m+10m)")

    if entry_signal:
        score_raw += float(w.get("TRIX_5M", 1.0))

    # ── Critère 6 : Biais macro 1D (EMA200 journalier) ─────────── [NEW v7] ──
    ema200_1d      = None
    d1_bias_ok     = False
    struct_1d      = "RANGING"
    if USE_1D_BIAS and df_1d is not None and len(df_1d) >= 50:
        try:
            ema200_1d = float(df_1d["close"].ewm(span=200, adjust=False).mean().iloc[-1])
            d1_close  = float(df_1d["close"].iloc[-1])
            if ("ACHAT" in side and d1_close > ema200_1d) or \
               ("VENTE" in side and d1_close < ema200_1d):
                d1_bias_ok = True
                score_raw += float(w.get("EMA200_1D", 1.0))
                confs.append("EMA200(1D)")
            # Structure 1D pour information contextuelle
            struct_1d = _detect_market_structure(df_1d, n=min(30, len(df_1d) - 1))
        except Exception:
            pass

    # ── Critère 7 : Delta Volume (pression acheteur/vendeur aggTrade) [NEW v7] ─
    delta_vol_ok     = False
    delta_vol_pct    = 0.5
    delta_vol_signal = "NEUTRAL"
    if DELTA_VOL_ENABLED and delta_vol_state is not None:
        dv_ts = float(delta_vol_state.get("ts", 0.0) or 0.0)
        dv_fresh = (datetime.now(timezone.utc).timestamp() - dv_ts) < 120  # données < 2min
        if dv_fresh:
            delta_vol_pct = float(delta_vol_state.get("delta_pct", 0.5) or 0.5)
            dv_bull = bool(delta_vol_state.get("bullish", False))
            dv_bear = bool(delta_vol_state.get("bearish", False))
            if ("ACHAT" in side and dv_bull) or ("VENTE" in side and dv_bear):
                delta_vol_ok = True
                score_raw += float(w.get("DELTA_VOL", 1.0))
                confs.append("DELTA_VOL({:.0f}%)".format(delta_vol_pct * 100))
                delta_vol_signal = "BULLISH" if dv_bull else "BEARISH"

    # ── Finalisation score ────────────────────────────────────────────────────
    # v7: score /9 (2 critères supplémentaires vs /7 avant)
    score = min(9, int(round(score_raw)))

    # Signal range 30m (informatif, non bloquant)
    range_30m = _detect_range_30m(df_m30) if df_m30 is not None else False
    if range_30m:
        confs.append("RANGE(30m)")

    # ── Contexte algo complet ─────────────────────────────────────────────────
    ema20_series = ta.ema(df30["close"], length=20) if df30 is not None and len(df30) >= 20 else None
    algo_context = {
        "h4_close":          h4_close,
        "ema200_h4":         ema200_h4,
        "struct_h2":         struct_h2,
        "struct_h1":         struct_h1,
        "align_h2_h1":       align_bonus,
        "ob_fvg_ok":         ob_fvg_ok,
        "ob_status_30m":     ob_status_30m,
        "ob_quality_30m":    ob_quality_30m,
        "ob_fvg_15m_ok":     ob_fvg_15m_ok,
        "ob_quality_15m":    ob_quality_15m,
        "rejection_ok":      rejection_ok,
        "entry_signal":      entry_signal,
        "range_30m":         range_30m,
        "rsi_5m":            None,
        "momentum_30s":      False,
        "ema20_30s":         float(ema20_series.iloc[-1]) if ema20_series is not None and not ema20_series.empty else None,
        # v7 new fields
        "score_max":         9,
        "ema200_1d":         ema200_1d,
        "d1_bias_ok":        d1_bias_ok,
        "struct_1d":         struct_1d,
        "delta_vol_ok":      delta_vol_ok,
        "delta_vol_pct":     delta_vol_pct,
        "delta_vol_signal":  delta_vol_signal,
        "futures": {
            "oi":               (futures_data or {}).get("oi"),
            "oi_change_pct":    (futures_data or {}).get("oi_change_pct"),
            "funding":          (futures_data or {}).get("funding"),
            "mark_price":       (futures_data or {}).get("mark_price"),
            "long_short_ratio": (futures_data or {}).get("long_short_ratio"),
        } if futures_data and futures_data.get("ok") else None,
    }
    if df_m5 is not None and len(df_m5) >= 14:
        rsi_s5 = ta.rsi(df_m5["close"], 14)
        algo_context["rsi_5m"] = float(rsi_s5.iloc[-1]) if rsi_s5 is not None and not rsi_s5.empty else None

    return score, side, confs, algo_context

# ---------------------------------------------------------------------------
# MODULE 18 — Optimisation annuelle multi-actifs (SL/TP Adaptatif)
# ---------------------------------------------------------------------------
# Architecture :
#   filter_year()             → filtre un DataFrame sur l'année OPT_YEAR
#   _backtest_strategy_real() → simulation vectorisée sur df filtré avec config
#   _opt_score()              → critère de sélection configurable
#   _run_optimisation_sync()  → boucle CPU-intensive (appelée via to_thread)
#   optimise_strategies_year()→ orchestre multi-actifs + stocke _best_results_2025
#   optimisation_loop()       → tâche asyncio périodique
# ---------------------------------------------------------------------------

def filter_year(df: Optional[pd.DataFrame], year: int = OPT_YEAR) -> Optional[pd.DataFrame]:
    """Filtre un DataFrame sur une année calendaire (index = pd.DatetimeIndex UTC)."""
    if df is None or df.empty:
        return None
    try:
        df_year = df[df.index.year == year]
        return df_year if not df_year.empty else None
    except Exception:
        return None


def _backtest_strategy_real(
    df: pd.DataFrame,
    config: Dict[str, Any],
    fee_bps: float = OPT_FEE_BPS,
) -> Dict[str, Any]:
    """Backtest SL/TP adaptatif sur un DataFrame OHLCV filtré.

    Logique :
      - Signal d'entrée LONG  : close[t] > EMA200(close)[t]
      - Signal d'entrée SHORT : close[t] < EMA200(close)[t]
      - SL et TPs calculés via compute_adaptive_levels() avec la config testée
      - Sortie : premier TP touché (TP1) ou SL atteint sur les bougies suivantes
      - Frais inclus (fee_bps) dans le calcul des niveaux

    Métriques retournées :
      sharpe, expectancy, winrate, max_drawdown, trades, return, sample_size

    Note : la logique EMA200 est cohérente avec le critère 1 de score_setup().
    """
    _NULL = {
        "sharpe": -1e9, "expectancy": -1e9, "winrate": 0.0,
        "max_drawdown": -1.0, "trades": 0, "return": 0.0,
        "sample_size": len(df) if df is not None else 0,
        "atr_mult":  config.get("atr_mult",  1.0),
        "tp_ratios": config.get("tp_ratios", (1.2, 1.8, 2.4)),
        "trailing":  config.get("trailing",  False),
    }
    if df is None or len(df) < OPT_MIN_CANDLES:
        return _NULL

    try:
        df = df.copy()
        close_arr = df["close"].astype(float).values
        high_arr  = df["high"].astype(float).values
        low_arr   = df["low"].astype(float).values
        n = len(close_arr)

        # ── EMA200 + ATR14 vectorisés ─────────────────────────────────────
        ema200_s = df["close"].ewm(span=200, adjust=False).mean()
        atr_s    = ta.atr(df["high"], df["low"], df["close"], 14)
        ema200   = ema200_s.values.astype(float)
        atr_v    = (atr_s.bfill().fillna(0.0)).values.astype(float)

        # Prépare colonne ATR pour compute_adaptive_levels (clamping p20-p80)
        df["atr"] = atr_v

        atr_mult  = float(config.get("atr_mult",  1.0))
        tp_ratios = tuple(config.get("tp_ratios", (1.2, 1.8, 2.4)))
        fee_pct   = fee_bps / 10_000.0

        trades_pnl: List[float] = []
        equity     = 1.0
        eq_curve:  List[float] = [1.0]

        i = 200  # skip EMA warmup
        while i < n - 1:
            # ── Détermination du biais via EMA200 ─────────────────────────
            side = "long" if close_arr[i] > ema200[i] else "short"

            entry_price = close_arr[i]
            atr_current = float(atr_v[i]) if atr_v[i] > 0 else abs(entry_price) * 0.002

            # lookback pour clamp ATR percentile
            lb_start = max(0, i - 50)
            lookback_df = df.iloc[lb_start : i + 1]

            sl, tps = compute_adaptive_levels(
                entry_price = entry_price,
                atr         = atr_current,
                side        = side,
                lookback_df = lookback_df,
                atr_mult    = atr_mult,
                tp_ratios   = tp_ratios,
                fee_bps     = int(fee_bps),
            )
            tp1 = tps[0] if tps else None
            if tp1 is None:
                i += 1
                continue

            # ── Simulation bougie par bougie jusqu'au SL ou TP1 ──────────
            max_hold = min(i + 50, n)  # max 50 bougies de détention
            outcome  = 0.0
            hit      = False
            for j in range(i + 1, max_hold):
                h_j = float(high_arr[j])
                l_j = float(low_arr[j])
                if side == "long":
                    if l_j <= sl:
                        # SL touché
                        pnl = (sl - entry_price) / entry_price - fee_pct
                        outcome = pnl
                        hit = True
                        i = j
                        break
                    if h_j >= tp1:
                        # TP1 touché
                        pnl = (tp1 - entry_price) / entry_price - fee_pct
                        outcome = pnl
                        hit = True
                        i = j
                        break
                else:  # short
                    if h_j >= sl:
                        pnl = (entry_price - sl) / entry_price - fee_pct
                        outcome = -abs(pnl)
                        hit = True
                        i = j
                        break
                    if l_j <= tp1:
                        pnl = (entry_price - tp1) / entry_price - fee_pct
                        outcome = abs(pnl)
                        hit = True
                        i = j
                        break

            if not hit:
                # Expiration : sortie au close de la dernière bougie
                exit_px = float(close_arr[max_hold - 1])
                if side == "long":
                    outcome = (exit_px - entry_price) / entry_price - fee_pct
                else:
                    outcome = (entry_price - exit_px) / entry_price - fee_pct
                i = max_hold

            trades_pnl.append(outcome)
            equity *= (1.0 + outcome)
            eq_curve.append(equity)

            # Évite les entrées consécutives — on attend 3 bougies de repos
            i += 3

        if not trades_pnl:
            return _NULL

        arr_pnl = np.array(trades_pnl, dtype=float)
        n_trades = len(arr_pnl)
        mean_pnl = float(arr_pnl.mean())
        std_pnl  = float(arr_pnl.std(ddof=1)) if n_trades > 1 else 1e-12
        sharpe   = mean_pnl / (std_pnl + 1e-12) * (n_trades ** 0.5)
        wins     = int(np.sum(arr_pnl > 0))
        losses   = int(np.sum(arr_pnl <= 0))
        winrate  = wins / n_trades
        avg_win  = float(arr_pnl[arr_pnl > 0].mean()) if wins > 0 else 0.0
        avg_loss = float(abs(arr_pnl[arr_pnl <= 0].mean())) if losses > 0 else 1e-9
        # Expectancy = WR×AvgWin − (1−WR)×AvgLoss  (en % par trade)
        expectancy = winrate * avg_win - (1.0 - winrate) * avg_loss

        # Max drawdown sur la courbe d'équité
        eq_arr  = np.array(eq_curve, dtype=float)
        peak    = np.maximum.accumulate(eq_arr)
        dd      = (eq_arr / (peak + 1e-12)) - 1.0
        max_dd  = float(dd.min())

        return {
            "sharpe":       float(sharpe),
            "expectancy":   float(expectancy),
            "winrate":      float(winrate),
            "max_drawdown": float(max_dd),
            "trades":       int(n_trades),
            "return":       float(equity - 1.0),
            "sample_size":  int(n),
            "atr_mult":     atr_mult,
            "tp_ratios":    list(tp_ratios),
            "trailing":     bool(config.get("trailing", False)),
        }

    except Exception as e:
        logger.debug("[OPT] _backtest_strategy_real error: %s", e)
        return _NULL


def _opt_score(res: Dict[str, Any], criteria: str = OPT_SCORE_CRITERIA) -> float:
    """Score de sélection — retourne -1e18 si 0 trades ou valeurs sentinelles."""
    trades = int(res.get("trades", 0))
    if trades == 0:
        return -1e18
    sharpe     = float(res.get("sharpe",     -1e9))
    expectancy = float(res.get("expectancy", -1e9))
    max_dd     = float(res.get("max_drawdown", -1.0))
    if sharpe <= -1e8 or expectancy <= -1e8:
        return -1e18
    if criteria == "sharpe":
        return sharpe
    if criteria == "expectancy":
        return expectancy
    return 0.5 * sharpe + 0.3 * expectancy + 0.2 * (1.0 + max_dd)


def _run_optimisation_sync(
    symbols:        List[str],
    candle_store_snap: Dict[str, Optional[pd.DataFrame]],
    configurations: List[Dict[str, Any]],
    year:           int,
) -> Dict[str, Dict[str, Any]]:
    """Version synchrone CPU-intensive de l'optimisation (exécutée via to_thread).

    Pour chaque symbole :
      1. Si year > 0 : filtre les bougies sur cette année
         Si year == 0 : utilise toutes les données disponibles (mode fetch historique)
      2. Teste toutes les configurations
      3. Conserve la meilleure selon OPT_SCORE_CRITERIA
      4. Stocke la liste complète des résultats triés (pour analyse)
    """
    results: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        df_full  = candle_store_snap.get(sym)
        # year=0 signifie "toutes les données" (fetch via fetch_klines_history)
        df_year  = filter_year(df_full, year) if year > 0 else df_full

        if df_year is None or len(df_year) < OPT_MIN_CANDLES:
            logger.warning(
                "[OPT] %s — données insuffisantes (%s bougies, min=%s)",
                sym,
                len(df_year) if df_year is not None else 0,
                OPT_MIN_CANDLES,
            )
            results[sym] = {
                "error":        "insufficient_data",
                "sample_size":  len(df_year) if df_year is not None else 0,
                "year":         year,
                "computed_at":  datetime.now(timezone.utc).isoformat(),
            }
            continue

        all_results: List[Dict[str, Any]] = []
        best_result: Optional[Dict[str, Any]] = None
        best_score  = -1e18

        for cfg in configurations:
            res = _backtest_strategy_real(df_year, cfg)
            res["_score"] = _opt_score(res)
            all_results.append(res)
            if res["_score"] > best_score:
                best_score  = res["_score"]
                best_result = res

        # Tri décroissant par score pour analyse frontend
        all_results.sort(key=lambda x: float(x.get("_score", -1e18)), reverse=True)

        if best_result is None:
            results[sym] = {
                "error":       "no_result",
                "year":        year,
                "computed_at": datetime.now(timezone.utc).isoformat(),
            }
            continue

        results[sym] = {
            **best_result,
            "year":           year,
            "score_criteria": OPT_SCORE_CRITERIA,
            "computed_at":    datetime.now(timezone.utc).isoformat(),
            "all_results":    all_results[:6],   # top-6 pour ne pas surcharger /api/state
        }
        logger.info(
            "[OPT] %s %s → best: atr_mult=%.1f tp_ratios=%s | sharpe=%.2f exp=%.4f wr=%.0f%% dd=%.1f%%",
            sym, year,
            float(best_result.get("atr_mult", 0)),
            best_result.get("tp_ratios"),
            float(best_result.get("sharpe", 0)),
            float(best_result.get("expectancy", 0)),
            float(best_result.get("winrate", 0)) * 100,
            float(best_result.get("max_drawdown", 0)) * 100,
        )

    return results


async def optimise_strategies_year(
    symbols:        List[str],
    configurations: List[Dict[str, Any]],
    session:        Any = None,
    year:           int = None,
) -> Dict[str, Dict[str, Any]]:
    """Lance l'optimisation SL/TP multi-actifs de manière non-bloquante.

    Correctifs v8 :
      - fetch réseau + fallback candle_store
      - OPT_IN_SAMPLE_DAYS=60j (17280 bougies)
      - fillna().bfill() compatible Pandas 2.x
      - _opt_score : garde-fou trades==0 → -1e18
    """
    global _best_results_2025, _opt_running, _opt_last_run_ts

    if _opt_running:
        logger.info("[OPT] Optimisation déjà en cours — skip")
        return _best_results_2025

    _opt_running = True
    try:
        logger.info("[OPT] Démarrage optimisation — %s actifs, %s configs, in_sample=%sd, tf=%s",
                    len(symbols), len(configurations), OPT_IN_SAMPLE_DAYS, OPT_TF)

        # ── Fetch historique pour chaque actif (fetch réseau + fallback candle_store) ──
        snap: Dict[str, Optional[pd.DataFrame]] = {}
        _sess = session
        try:
            _sess = _sess or app.state.http
        except Exception:
            pass

        for sym in symbols:
            df_ok = None

            # Tentative 1 : fetch réseau
            if _sess is not None:
                try:
                    df_f = await fetch_klines_history(_sess, sym, OPT_TF, OPT_IN_SAMPLE_DAYS)
                    if df_f is not None and not df_f.empty and len(df_f) >= OPT_MIN_CANDLES:
                        df_ok = df_f
                        logger.info("[OPT] fetch OK %s: %s bougies %s", sym, len(df_f), OPT_TF)
                    else:
                        logger.warning("[OPT] fetch %s: %s bougies < min=%s — fallback candle_store",
                                       sym, len(df_f) if df_f is not None else 0, OPT_MIN_CANDLES)
                except Exception as fe:
                    logger.warning("[OPT] fetch %s échoué: %s — fallback candle_store", sym, fe)

            # Tentative 2 : fallback candle_store
            if df_ok is None:
                df_cs = candle_store.get(sym)
                if df_cs is not None and not df_cs.empty and len(df_cs) >= OPT_MIN_CANDLES:
                    df_ok = df_cs
                    logger.info("[OPT] fallback candle_store %s: %s bougies", sym, len(df_ok))
                else:
                    logger.warning("[OPT] %s: aucune donnée (fetch KO + candle_store=%s bougies)",
                                   sym, len(df_cs) if df_cs is not None else 0)

            snap[sym] = df_ok

        # ── Optimisation par actif ────────────────────────────────────────────
        results: Dict[str, Dict[str, Any]] = {}
        for sym in symbols:
            sym_configs = OPT_CONFIGURATIONS_PAXG if sym in SYM_OVERRIDES else configurations
            sym_snap    = {sym: snap.get(sym)}
            sym_result  = await asyncio.to_thread(
                _run_optimisation_sync, [sym], sym_snap, sym_configs, 0
            )
            results.update(sym_result)

        _best_results_2025 = results
        _opt_last_run_ts   = datetime.now(timezone.utc).timestamp()

        # ── Injection dans signals ────────────────────────────────────────────
        for sym, res in results.items():
            if sym not in signals:
                signals[sym] = {}
            signals[sym]["best_config_2025"] = {
                k: v for k, v in res.items() if k != "all_results"
            }

        ok_syms  = [s for s, r in results.items() if not r.get("error") and int(r.get("trades", 0)) > 0]
        err_syms = [s for s, r in results.items() if r.get("error") or int(r.get("trades", 0)) == 0]
        logger.info("[OPT] Terminé — OK: %s | Erreur/0trades: %s", ok_syms, err_syms)
        return results

    except Exception as e:
        logger.error("[OPT] Erreur optimisation: %s", e, exc_info=True)
        return _best_results_2025
    finally:
        _opt_running = False


async def optimisation_loop() -> None:
    """Tâche asyncio : lance l'optimisation au démarrage puis périodiquement."""
    await asyncio.sleep(90)   # attendre que les WS soient stables
    logger.info("[OPT] Premier run d'optimisation dans 5s…")
    await asyncio.sleep(5)

    refresh_s = int(OPT_REFRESH_HOURS) * 3600

    while True:
        try:
            await optimise_strategies_year(
                symbols        = SYMBOLS,
                configurations = OPT_CONFIGURATIONS,
                session        = app.state.http,
            )
        except Exception as e:
            logger.warning("[OPT] optimisation_loop exception: %s", e)

        await asyncio.sleep(refresh_s)


# ---------------------------------------------------------------------------
# PATCH SL/TP ADAPTATIF — Titanium Dashboard Autonome v3
# Calcul SL/TP optimisé : ATR clampé (p20–p80), frais Binance inclus,
# ratios calibrés (1.2 / 1.8 / 2.4 RR).
# ---------------------------------------------------------------------------
def compute_adaptive_levels(
    entry_price: float,
    atr: float,
    side: str,
    lookback_df: pd.DataFrame,
    atr_mult: float = 1.0,
    tp_ratios: Tuple[float, ...] = (1.2, 1.8, 2.4),
    fee_bps: int = 4,
) -> Tuple[float, List[float]]:
    """Calcule SL/TP optimisés pour Titanium Dashboard Autonome v3.

    Utilise ATR clampé entre le 20e et 80e percentile historique (rolling 30)
    pour éviter les valeurs extrêmes. Intègre les frais Binance (maker/taker)
    dans le prix d'entrée ajusté. Les ratios TP (1.2 / 1.8 / 2.4) sont calibrés
    sur backtests 3m BTC/ETH/SOL.

    Args:
        entry_price: Prix d'entrée brut (dernière clôture 3m).
        atr:         Valeur ATR courante sur le TF 3m.
        side:        'long' ou 'short'.
        lookback_df: DataFrame avec colonne 'atr' si disponible (ex: df_3m.tail(50)).
        atr_mult:    Multiplicateur ATR pour le SL (défaut 1.0).
        tp_ratios:   Ratios RR pour les TPs (défaut 1.2 / 1.8 / 2.4).
        fee_bps:     Commission Binance en bps (défaut 4 = 0.04%).

    Returns:
        (sl, [tp1, tp2, tp3]) — prix arrondis à 4 décimales.
    """
    # ── Clamp ATR sur le 20e–80e percentile (rolling 30) ─────────────────────
    try:
        if "atr" in lookback_df.columns:
            hist_atr = lookback_df["atr"].rolling(30).mean().dropna()
            if len(hist_atr) > 0:
                atr_val = float(np.clip(atr, hist_atr.quantile(0.2), hist_atr.quantile(0.8)))
            else:
                atr_val = float(atr)
        else:
            atr_val = float(atr)
    except Exception:
        atr_val = float(atr)  # fallback sécurisé

    # ── Ajustement frais (commission incluse dans le prix d'entrée) ───────────
    fee_pct = fee_bps / 10_000
    adj_entry = entry_price * (1 - fee_pct) if side == "long" else entry_price * (1 + fee_pct)

    # ── SL et TPs ─────────────────────────────────────────────────────────────
    if side == "long":
        sl  = adj_entry - atr_mult * atr_val
        tps = [adj_entry + r * (adj_entry - sl) for r in tp_ratios]
    else:
        sl  = adj_entry + atr_mult * atr_val
        tps = [adj_entry - r * (sl - adj_entry) for r in tp_ratios]

    return round(sl, 4), [round(tp, 4) for tp in tps]


def compute_atr_levels(
    df30: pd.DataFrame,
    side: str,
    df_m3: Optional[pd.DataFrame] = None,
    fvgs: Optional[List[dict]] = None,
    obs:  Optional[List[dict]] = None,
) -> Tuple[float, float, List[float]]:
    """Calcule entry / SL / TPs.

    Améliorations v4 :
      - Entry = dernière clôture de bougie 3m si df_m3 disponible (sinon 30s).
      - TP1–TP4 calculés en priorité sur les zones de résistance/support
        réelles (OB + pivots) au-dessus/dessous du prix d'entrée.
      - Si un FVG est contenu dans l'OB courant → double confirmation,
        les TPs sont déployés plus agressivement (multipliers ×1.1).
      - SL : swing + 0.35 × ATR (inchangé).
    """
    # ── Entrée : clôture 3m si disponible ────────────────────────────────────
    if df_m3 is not None and len(df_m3) >= 1:
        entry = float(df_m3["close"].iloc[-1])
        atr_src = df_m3
    else:
        entry = float(df30["close"].iloc[-1])
        atr_src = df30

    atr = ta.atr(atr_src["high"], atr_src["low"], atr_src["close"], 14)
    atr_val = max(float(atr.iloc[-1]) if atr is not None and not atr.empty else 0.001, 1e-9)

    # ── SL : swing + ATR buffer ───────────────────────────────────────────────
    if "ACHAT" in side:
        swing = float(df30.tail(60)["low"].min())
        sl = swing - 0.35 * atr_val
    else:
        swing = float(df30.tail(60)["high"].max())
        sl = swing + 0.35 * atr_val

    r = max(abs(entry - sl), atr_val * 0.5)

    # ── Double confirmation FVG-dans-OB ──────────────────────────────────────
    fvg_in_ob = False
    if fvgs and obs:
        for fvg in fvgs:
            for ob in obs:
                fbot = float(fvg.get("bot", 0) or 0)
                ftop = float(fvg.get("top", 0) or 0)
                obot = float(ob.get("bot", 0) or 0)
                otop = float(ob.get("top", 0) or 0)
                if obot > 0 and otop > 0 and fbot >= obot and ftop <= otop:
                    fvg_in_ob = True
                    break
            if fvg_in_ob:
                break

    mult = [1.1, 2.2, 3.3, 4.4] if fvg_in_ob else [1.0, 2.0, 3.0, 4.0]

    # ── TPs : résistances/supports réels (OBs) si disponibles ───────────────
    tp_levels: List[float] = []
    if obs:
        relevant = sorted(
            [float(ob.get("top", 0) or 0) for ob in obs if float(ob.get("top", 0) or 0) > entry]
            if "ACHAT" in side else
            [float(ob.get("bot", 0) or 0) for ob in obs if 0 < float(ob.get("bot", 0) or 0) < entry],
            reverse=("VENTE" in side),
        )
        tp_levels = [p for p in relevant if p > 0][:4]

    # Compléter les TPs manquants avec les niveaux ATR
    for i, m in enumerate(mult):
        if i >= len(tp_levels):
            if "ACHAT" in side:
                tp_levels.append(entry + r * m)
            else:
                tp_levels.append(entry - r * m)

    return entry, sl, tp_levels[:4]

def detect_fvg(df30: pd.DataFrame, n: int = 200) -> List[Dict[str, Any]]:
    """Détecte TOUTES les Fair Value Gaps (FVG) sans restriction de cap.

    Retourne une liste de zones {top, bot, ts, type, width_pct}.
    - type='bull' si gap haussier (h0 < l2)
    - type='bear' si gap baissier (l0 > h2)
    - width_pct : largeur du FVG en % du prix (indicateur de force)

    Note v4 : cap retiré (était -5). Toutes les FVG sont retournées
    pour calcul de scoring et affichage frontend.
    """
    zones: List[Dict[str, Any]] = []
    if df30 is None or df30.empty:
        return zones

    tail = df30.tail(int(n))
    for i in range(2, len(tail)):
        h0 = float(tail.iloc[i - 2]["high"])
        l0 = float(tail.iloc[i - 2]["low"])
        l2 = float(tail.iloc[i]["low"])
        h2 = float(tail.iloc[i]["high"])
        ts = int(tail.index[i].timestamp() * 1000)
        close_ref = float(tail.iloc[i]["close"])

        if h0 < l2:
            width_pct = round((l2 - h0) / close_ref * 100, 4) if close_ref > 0 else 0.0
            zones.append({"top": l2, "bot": h0, "ts": ts, "type": "bull", "width_pct": width_pct})
        elif l0 > h2:
            width_pct = round((l0 - h2) / close_ref * 100, 4) if close_ref > 0 else 0.0
            zones.append({"top": l0, "bot": h2, "ts": ts, "type": "bear", "width_pct": width_pct})

    return zones  # toutes les FVG, sans cap

def detect_ob(df30: pd.DataFrame, side: str, n: int = 60) -> List[Dict[str, Any]]:
    """Détecte les Order Blocks avec statut intact/tested/broken et flag FVG-dans-OB."""
    tail = df30.tail(n)
    obs = []
    for i in range(len(tail) - 2, 0, -1):
        c = float(tail.iloc[i]["close"])
        o = float(tail.iloc[i]["open"])
        h = float(tail.iloc[i]["high"])
        low_ = float(tail.iloc[i]["low"])

        if "ACHAT" in side and c < o:     # OB bull
            ob_top, ob_bot = o, low_
        elif "VENTE" in side and c > o:   # OB bear
            ob_top, ob_bot = h, c
        else:
            continue

        # Statut de l'OB sur les bougies postérieures
        df_after = tail.iloc[i + 1:].copy() if i + 1 < len(tail) else pd.DataFrame()
        status = _ob_status(df_after, ob_top, ob_bot, side)

        obs.append({
            "top":    ob_top,
            "bot":    ob_bot,
            "ts":     int(tail.index[i].timestamp() * 1000),
            "type":   "bull_ob" if "ACHAT" in side else "bear_ob",
            "status": status,    # intact / tested / broken
        })
        if len(obs) >= 5:   # retourner jusqu'à 5 OBs (dont les cassés pour info)
            break
    return obs

def detect_candle_patterns(df30: pd.DataFrame, n: int = 40) -> List[Dict[str, Any]]:
    """Bibliothèque de patterns de bougies enrichie v4 — 17 patterns (sans TA-Lib).

    Patterns ajoutés v4 :
      - Shooting Star          (BEAR, HIGH)   — mèche haute > 2×corps, corps baissier
      - Spinning Top           (NEUTRAL, LOW) — corps très petit, mèches équilibrées
      - Three White Soldiers   (BULL, HIGH)   — 3 bougies haussières consécutives solides
      - Three Black Crows      (BEAR, HIGH)   — 3 bougies baissières consécutives solides
      - Tweezer Top            (BEAR, HIGH)   — 2 hauts identiques après tendance haussière
      - Tweezer Bottom         (BULL, HIGH)   — 2 bas identiques après tendance baissière
      - Piercing Line          (BULL, HIGH)   — bougie haussière pénètre > 50% du corps préc.
      - Dark Cloud Cover       (BEAR, HIGH)   — bougie baissière pénètre > 50% du corps préc.
      - Bull Kicker            (BULL, HIGH)   — gap haussier violent, corps solide
      - Bear Kicker            (BEAR, HIGH)   — gap baissier violent, corps solide
      - Three Inside Up        (BULL, MEDIUM) — Harami + confirmation haussière
      - Three Inside Down      (BEAR, MEDIUM) — Harami + confirmation baissière

    Format de sortie : {pattern, type, significance, label, time, ts_ms, price}
    """
    out: List[Dict[str, Any]] = []
    if df30 is None or df30.empty:
        return out

    tail = df30.tail(int(n)).copy()
    if len(tail) < 3:
        return out

    def _add(ts, price, pattern, ptype, sig, label=None):
        t_sec = int(pd.Timestamp(ts).timestamp())
        out.append({
            "pattern":     pattern,
            "type":        ptype,
            "price":       round(float(price), 6),
            "significance": sig,
            "label":       label or pattern,
            "time":        t_sec,
            "ts_ms":       int(t_sec * 1000),
        })

    for i in range(2, len(tail)):
        c0 = tail.iloc[i - 2]
        c1 = tail.iloc[i - 1]
        c  = tail.iloc[i]
        ts = tail.index[i]

        o   = float(c["open"]);   h   = float(c["high"])
        low_ = float(c["low"]);   cl  = float(c["close"])
        o1  = float(c1["open"]);  h1  = float(c1["high"])
        l1  = float(c1["low"]);   cl1 = float(c1["close"])
        o0  = float(c0["open"]);  h0  = float(c0["high"])
        l0  = float(c0["low"]);   cl0 = float(c0["close"])

        rng   = max(h - low_, 1e-10)
        body  = abs(cl - o)
        upper = h - max(o, cl)
        lower = min(o, cl) - low_

        body1  = abs(cl1 - o1)
        rng1   = max(h1 - l1, 1e-10)
        body0  = abs(cl0 - o0)

        # ── PATTERNS EXISTANTS (conservés) ────────────────────────────────

        # Doji
        if body / rng < 0.06:
            _add(ts, cl, "Doji", "NEUTRAL", "MEDIUM", "Doji")

        # Spinning Top (corps petit, mèches équilibrées)
        elif body / rng < 0.25 and upper > 0.2 * rng and lower > 0.2 * rng:
            _add(ts, cl, "SpinningTop", "NEUTRAL", "LOW", "Spinning Top")

        # Marubozu
        if cl > o and body / rng > 0.90:
            _add(ts, cl, "BullMarubozu", "BULL", "HIGH", "Marubozu haussier")
        elif cl < o and body / rng > 0.90:
            _add(ts, cl, "BearMarubozu", "BEAR", "HIGH", "Marubozu baissier")

        # Hammer
        if lower > 2.0 * body and upper < 0.30 * rng and body / rng > 0.05:
            _add(ts, cl, "Hammer", "BULL", "HIGH", "Marteau")

        # Shooting Star (mèche haute, corps baissier ou petit)
        if upper > 2.0 * body and lower < 0.30 * rng and body / rng > 0.05:
            if cl <= o:
                _add(ts, cl, "ShootingStar", "BEAR", "HIGH", "Shooting Star")
            else:
                _add(ts, cl, "InvHammer", "BULL", "MEDIUM", "Marteau inversé")

        # Engulfing
        if (cl1 < o1) and (cl > o) and (o <= cl1) and (cl >= o1):
            _add(ts, cl, "BullEngulfing", "BULL", "HIGH", "Engulfing haussier")
        if (cl1 > o1) and (cl < o) and (o >= cl1) and (cl <= o1):
            _add(ts, cl, "BearEngulfing", "BEAR", "HIGH", "Engulfing baissier")

        # Harami
        if (cl1 < o1) and (cl > o) and (o >= cl1) and (cl <= o1) and (body < 0.55 * max(body1, 1e-10)):
            _add(ts, cl, "BullHarami", "BULL", "MEDIUM", "Harami haussier")
        if (cl1 > o1) and (cl < o) and (o <= cl1) and (cl >= o1) and (body < 0.55 * max(body1, 1e-10)):
            _add(ts, cl, "BearHarami", "BEAR", "MEDIUM", "Harami baissier")

        # Morning / Evening Star
        if (cl0 < o0) and (body1 / rng1 < 0.35) and (cl > o) and (cl > (o0 + cl0) / 2.0):
            _add(ts, cl, "MorningStar", "BULL", "HIGH", "Morning Star")
        if (cl0 > o0) and (body1 / rng1 < 0.35) and (cl < o) and (cl < (o0 + cl0) / 2.0):
            _add(ts, cl, "EveningStar", "BEAR", "HIGH", "Evening Star")

        # ── NOUVEAUX PATTERNS v4 ──────────────────────────────────────────

        # Three White Soldiers : 3 bougies haussières solides consécutives
        # Corps > 60% range, chaque ouverture dans le corps précédent
        if (cl > o and cl1 > o1 and cl0 > o0
                and body / rng > 0.60 and body1 / rng1 > 0.60 and body0 / max(h0-l0, 1e-10) > 0.60
                and o >= min(o1, cl1) * 0.999 and o1 >= min(o0, cl0) * 0.999):
            _add(ts, cl, "ThreeWhiteSoldiers", "BULL", "HIGH", "Three White Soldiers")

        # Three Black Crows : 3 bougies baissières solides consécutives
        if (cl < o and cl1 < o1 and cl0 < o0
                and body / rng > 0.60 and body1 / rng1 > 0.60 and body0 / max(h0-l0, 1e-10) > 0.60
                and o <= max(o1, cl1) * 1.001 and o1 <= max(o0, cl0) * 1.001):
            _add(ts, cl, "ThreeBlackCrows", "BEAR", "HIGH", "Three Black Crows")

        # Tweezer Top : deux hauts quasi-identiques (±0.1% range), contexte haussier avant
        tol_tweezer = rng1 * 0.10
        if abs(h - h1) <= tol_tweezer and cl1 > o1 and cl < o and body / rng > 0.30:
            _add(ts, cl, "TweezerTop", "BEAR", "HIGH", "Tweezer Top")

        # Tweezer Bottom : deux bas quasi-identiques, contexte baissier avant
        if abs(low_ - l1) <= tol_tweezer and cl1 < o1 and cl > o and body / rng > 0.30:
            _add(ts, cl, "TweezerBottom", "BULL", "HIGH", "Tweezer Bottom")

        # Piercing Line : bougie baissière C1, C2 haussière ouvre sous low1 et ferme > 50% corps C1
        mid1 = (o1 + cl1) / 2.0
        if cl1 < o1 and cl > o and o < l1 and cl > mid1 and cl < o1:
            _add(ts, cl, "PiercingLine", "BULL", "HIGH", "Piercing Line")

        # Dark Cloud Cover : bougie haussière C1, C2 baissière ouvre au-dessus high1 et ferme < 50% corps C1
        if cl1 > o1 and cl < o and o > h1 and cl < (o1 + cl1) / 2.0 and cl > o1:
            _add(ts, cl, "DarkCloudCover", "BEAR", "HIGH", "Dark Cloud Cover")

        # Bull Kicker : gap haussier violent (o > cl1), corps haussier solide
        if o > cl1 and cl > o and body / rng > 0.65:
            _add(ts, cl, "BullKicker", "BULL", "HIGH", "Bull Kicker")

        # Bear Kicker : gap baissier violent (o < cl1), corps baissier solide
        if o < cl1 and cl < o and body / rng > 0.65:
            _add(ts, cl, "BearKicker", "BEAR", "HIGH", "Bear Kicker")

        # Three Inside Up : Harami haussier (C0 bear, C1 bull inside) + C2 confirme au-dessus C0
        if (cl0 < o0 and cl1 > o1
                and o1 >= min(cl0, o0) and cl1 <= max(cl0, o0)
                and cl > cl0):
            _add(ts, cl, "ThreeInsideUp", "BULL", "MEDIUM", "Three Inside Up")

        # Three Inside Down : Harami baissier + C2 confirme en dessous C0
        if (cl0 > o0 and cl1 < o1
                and o1 <= max(cl0, o0) and cl1 >= min(cl0, o0)
                and cl < cl0):
            _add(ts, cl, "ThreeInsideDown", "BEAR", "MEDIUM", "Three Inside Down")

    # Retourne les 20 plus récents (contre 12 avant — bibliothèque plus riche)
    return out[-20:]


def detect_reversal_candles(df30: pd.DataFrame, lookback: int = 300) -> List[Dict[str, Any]]:
    """Scan de patterns de retournement sur lookback bougies — version enrichie v4.

    Utilisé pour le surlignage jaune sur le graphique.
    Patterns : Engulfing, Doji, Hammer, Shooting Star, Three Soldiers/Crows,
               Tweezer Top/Bottom, Bull/Bear Kicker.
    """
    out: List[Dict[str, Any]] = []
    if df30 is None or df30.empty or len(df30) < 3:
        return out

    tail = df30.tail(int(lookback)).copy()
    rows = list(tail.itertuples())

    def _add(ts, pat, typ, sig):
        out.append({"time": int(pd.Timestamp(ts).timestamp()), "pattern": pat, "type": typ, "significance": sig})

    for i in range(1, len(rows)):
        a = rows[i - 1]
        b = rows[i]
        a_o, a_c = float(a.open), float(a.close)
        b_o, b_c, b_h, b_l = float(b.open), float(b.close), float(b.high), float(b.low)
        b_rng  = max(b_h - b_l, 1e-9)
        b_body = abs(b_c - b_o)
        b_upper = b_h - max(b_o, b_c)
        b_lower = min(b_o, b_c) - b_l

        # Engulfing
        if b_c > b_o and a_c < a_o and b_c >= a_o and b_o <= a_c:
            _add(b.Index, 'BULL_ENGULF', 'BULL', 'HIGH')
        if b_c < b_o and a_c > a_o and b_o >= a_c and b_c <= a_o:
            _add(b.Index, 'BEAR_ENGULF', 'BEAR', 'HIGH')

        # Doji
        if (b_body / b_rng) <= 0.06:
            _add(b.Index, 'DOJI', 'NEUTRAL', 'MEDIUM')

        # Hammer
        if b_lower >= 2.0 * max(b_body, 1e-9) and b_upper <= 0.5 * max(b_body, 1e-9):
            _add(b.Index, 'HAMMER', 'BULL', 'MEDIUM')

        # Shooting Star
        if b_upper >= 2.0 * max(b_body, 1e-9) and b_lower <= 0.5 * max(b_body, 1e-9) and b_c <= b_o:
            _add(b.Index, 'SHOOTING_STAR', 'BEAR', 'HIGH')

        # Bull Kicker / Bear Kicker
        if b_o > a_c and b_c > b_o and b_body / b_rng > 0.65:
            _add(b.Index, 'BULL_KICKER', 'BULL', 'HIGH')
        if b_o < a_c and b_c < b_o and b_body / b_rng > 0.65:
            _add(b.Index, 'BEAR_KICKER', 'BEAR', 'HIGH')

        # Tweezer (requiert bougie précédente)
        tol = b_rng * 0.10
        if abs(b_h - float(a.high)) <= tol and a_c > a_o and b_c < b_o and b_body / b_rng > 0.30:
            _add(b.Index, 'TWEEZER_TOP', 'BEAR', 'HIGH')
        if abs(b_l - float(a.low)) <= tol and a_c < a_o and b_c > b_o and b_body / b_rng > 0.30:
            _add(b.Index, 'TWEEZER_BOTTOM', 'BULL', 'HIGH')

        # Three White Soldiers / Three Black Crows (requiert i >= 2)
        if i >= 2:
            p = rows[i - 2]
            p_o, p_c = float(p.open), float(p.close)
            p_rng = max(float(p.high) - float(p.low), 1e-9)
            a_rng = max(float(a.high) - float(a.low), 1e-9)
            if (b_c > b_o and a_c > a_o and p_c > p_o
                    and b_body / b_rng > 0.55 and abs(a_c - a_o) / a_rng > 0.55
                    and abs(p_c - p_o) / p_rng > 0.55
                    and b_o >= min(a_o, a_c) and a_o >= min(p_o, p_c)):
                _add(b.Index, 'THREE_WHITE_SOLDIERS', 'BULL', 'HIGH')
            if (b_c < b_o and a_c < a_o and p_c < p_o
                    and b_body / b_rng > 0.55 and abs(a_c - a_o) / a_rng > 0.55
                    and abs(p_c - p_o) / p_rng > 0.55
                    and b_o <= max(a_o, a_c) and a_o <= max(p_o, p_c)):
                _add(b.Index, 'THREE_BLACK_CROWS', 'BEAR', 'HIGH')

    return out[-100:]  # étendu à 100 (contre 80) pour la bibliothèque enrichie

def compute_volume_poc(df30: pd.DataFrame, bins: int = 24, lookback: int = 120) -> Dict[str, Any]:
    tail = df30.tail(lookback)
    if len(tail) < 10:
        return {"price": 0.0, "strength": "MEDIUM", "label": "N/A"}
    prices = tail["close"].values.astype(float)
    vols = tail["v"].values.astype(float)
    pmin, pmax = float(np.min(prices)), float(np.max(prices))
    if pmax <= pmin:
        return {"price": float(prices[-1]), "strength": "MEDIUM", "label": "POC zone"}
    edges = np.linspace(pmin, pmax, bins + 1)
    idx = np.clip(np.digitize(prices, edges) - 1, 0, bins - 1)
    agg = np.zeros(bins, dtype=float)
    for i, v in zip(idx, vols):
        agg[i] += v
    best = int(np.argmax(agg))
    poc = float((edges[best] + edges[best+1]) / 2.0)
    strength = "STRONG" if agg[best] >= np.mean(agg) + 2*np.std(agg) else "MEDIUM"
    return {"price": poc, "strength": strength, "label": "POC zone"}

def compute_supertrend(df30: pd.DataFrame) -> Dict[str, Any]:
    if len(df30) < 50:
        return {"direction": "BEAR", "value": 0.0}
    st = ta.supertrend(df30["high"], df30["low"], df30["close"], length=10, multiplier=3.0)
    if st is None or st.empty:
        return {"direction": "BEAR", "value": 0.0}
    cols = list(st.columns)
    vcol = next((c for c in cols if c.startswith("SUPERT_")), None)
    dcol = next((c for c in cols if c.startswith("SUPERTd_")), None)
    v = float(st[vcol].iloc[-1]) if vcol else 0.0
    d = int(st[dcol].iloc[-1]) if dcol else -1
    return {"direction": "BULL" if d > 0 else "BEAR", "value": v}


# ---------------------------------------------------------------------------
# Binance WS (trade) -> 1s OHLCV -> 30s
# ---------------------------------------------------------------------------
async def ws_binance(session: aiohttp.ClientSession):
    """Flux live robuste — v7 : aggTrade stream pour delta volume + trade stream pour OHLCV.

    Optimisations v7 :
      - Stream aggTrade au lieu de @trade pour avoir is_buyer_maker.
      - Delta volume rolling mis à jour en temps réel.
      - Authentification via header X-MBX-APIKEY (rate-limit levé).
      - Logique OHLCV 1s conservée identiquement.
    """
    # v7: aggTrade stream inclut is_buyer_maker ('m') pour le delta volume
    streams = "/".join([f"{ws_sym(s)}@aggTrade" for s in SYMBOLS])

    # Headers pour WS authentifié (optionnel mais lève le rate-limit)
    ws_headers = {}
    if BINANCE_KEY:
        ws_headers["X-MBX-APIKEY"] = BINANCE_KEY

    while True:
        connected_any = False
        for base in WS_BASES:
            url = f"{base}/stream?streams={streams}"
            try:
                async with session.ws_connect(
                    url, heartbeat=15, timeout=20, headers=ws_headers
                ) as ws:
                    connected_any = True
                    logger.info("Binance WS connecté (aggTrade) via %s", base)
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        data = json.loads(msg.data)
                        payload = data.get("data", {})
                        # aggTrade event
                        if payload.get("e") != "aggTrade":
                            continue

                        sym_raw = str(payload.get("s", "")).upper()
                        sym = next((s for s in SYMBOLS if s.replace("/", "") == sym_raw), None)
                        if not sym:
                            continue

                        now_ts = datetime.now(timezone.utc).timestamp()
                        _last_tick_ts[sym] = now_ts

                        try:
                            price = float(payload.get("p"))
                            qty   = float(payload.get("q", 0.0) or 0.0)
                            t_ms  = int(payload.get("T") or payload.get("E") or 0)
                            # is_buyer_maker: True = seller agressif / False = buyer agressif
                            is_buyer_maker = bool(payload.get("m", False))
                        except Exception:
                            continue

                        # ── Delta Volume (v7 new) ──────────────────────────
                        _update_delta_vol(sym, price, qty, is_buyer_maker)

                        # ── OHLCV 1s (logique inchangée) ──────────────────
                        sec = int(t_ms // 1000) if t_ms else int(now_ts)

                        b = _trade_bucket.get(sym)
                        if not b:
                            _trade_bucket[sym] = {"sec": sec, "o": price, "h": price,
                                                   "l": price, "c": price, "v": qty}
                            continue

                        prev_sec = int(b.get("sec", sec))

                        if prev_sec == sec:
                            if price > float(b.get("h", price)): b["h"] = price
                            if price < float(b.get("l", price)): b["l"] = price
                            b["c"] = price
                            b["v"] = float(b.get("v", 0.0) or 0.0) + qty
                            continue

                        prev_close = float(b.get("c", price))
                        lst = raw_1s.setdefault(sym, [])
                        lst.append({
                            "ts":    pd.to_datetime(prev_sec, unit="s", utc=True),
                            "open":  float(b["o"]), "high": float(b["h"]),
                            "low":   float(b["l"]), "close": float(b["c"]),
                            "v":     float(b.get("v", 0.0) or 0.0),
                        })

                        gap = sec - prev_sec
                        if 1 < gap <= 30:
                            for sec_i in range(prev_sec + 1, sec):
                                lst.append({
                                    "ts":    pd.to_datetime(sec_i, unit="s", utc=True),
                                    "open":  prev_close, "high": prev_close,
                                    "low":   prev_close, "close": prev_close, "v": 0.0,
                                })

                        if len(lst) > MAX_1S:
                            raw_1s[sym] = lst[-MAX_1S:]

                        _trade_bucket[sym] = {"sec": sec, "o": price, "h": price,
                                               "l": price, "c": price, "v": qty}

                        if not _resample_lock.get(sym, False):
                            _resample_lock[sym] = True
                            try:
                                await resample_and_push(sym)
                            finally:
                                _resample_lock[sym] = False

            except Exception as e:
                logger.warning("WS reconnect (%s): %s", base, e)
                await asyncio.sleep(2)

        if not connected_any:
            await asyncio.sleep(3)

async def resample_and_push(sym: str):
    lst = raw_1s.get(sym, [])
    if len(lst) < 30:
        return
    df = pd.DataFrame(lst).set_index("ts").sort_index()
    o = df["open"].resample("30s").first()
    h = df["high"].resample("30s").max()
    low_s = df["low"].resample("30s").min()
    c = df["close"].resample("30s").last()
    v = df["v"].resample("30s").sum()
    df30 = pd.concat([o, h, low_s, c, v], axis=1).dropna()
    df30.columns = ["open", "high", "low", "close", "v"]
    if len(df30) > MAX_CANDLES_30S:
        df30 = df30.tail(MAX_CANDLES_30S)
    candle_store[sym] = df30
    try:
        _last_bar_ts[sym] = float(df30.index[-1].timestamp())
    except Exception:
        pass
    await broadcast(sym, {"type":"candle","symbol":sym,"data":{
        "time": int(df30.index[-1].timestamp()),
        "open": round(float(df30["open"].iloc[-1]), 6),
        "high": round(float(df30["high"].iloc[-1]), 6),
        "low":  round(float(df30["low"].iloc[-1]), 6),
        "close":round(float(df30["close"].iloc[-1]), 6),
    }})

# ---------------------------------------------------------------------------
# Binance REST (cache TTL)
# ---------------------------------------------------------------------------
async def fetch_klines(session: aiohttp.ClientSession, sym: str, interval: str, limit: int) -> pd.DataFrame:
    params = {"symbol": rest_sym(sym), "interval": interval, "limit": str(limit)}
    # v7 : injecte la clé API read-only → rate-limit plus généreux (1200 req/min vs 100)
    headers: dict = {}
    if BINANCE_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_KEY
    for base in (REST_BASE, REST_FALLBACK):
        try:
            async with session.get(
                f"{base}/api/v3/klines", params=params, headers=headers, timeout=20
            ) as r:
                if r.status != 200:
                    continue
                data = await r.json(content_type=None)
                rows = [{"ts": pd.to_datetime(int(k[0]), unit="ms", utc=True),
                         "open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                         "close": float(k[4]), "v": float(k[5])} for k in data]
                return pd.DataFrame(rows).set_index("ts").sort_index()
        except Exception:
            continue
    return pd.DataFrame()


# =============================================================================
# [OPT-3] FONCTION CRÉATION SESSION HTTP OPTIMISÉE [v7]
# =============================================================================

async def create_optimized_session() -> aiohttp.ClientSession:
    """Crée une session aiohttp optimisée pour le pooling de connexions [v7]."""
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_TOTAL)

    connector = aiohttp.TCPConnector(
        limit=HTTP_POOL_SIZE,
        limit_per_host=HTTP_CONNECT_LIMIT,
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True,
    )

    return aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"User-Agent": "TitaniumDashboard/7.0"},
    )

# ---------------------------------------------------------------------------
# Binance REST — helper unifié (consolidation doublons M11)
# ---------------------------------------------------------------------------
# Toutes les fonctions get_*_cached partagent la même logique TTL.
# Avant : 8 fonctions quasi-identiques de 5 lignes chacune.
# Après : 1 helper + 8 wrappers d'1 ligne.

# Registre des caches TTL : {interval: (cache_dict, ttl_s, limit)}
# v7 : limites augmentées pour plus d'historique → meilleure détection SMC
_CACHE_REGISTRY: Dict[str, Tuple[Dict, int, int]] = {
    "1d":  (_1d_cache,   D1_CACHE_TTL, D1_LIMIT),   # ← NEW v7 : bougies daily
    "4h":  (_h4_cache,   H4_CACHE_TTL, 500),         # ← 210 → 500 (~83 jours)
    "2h":  (_h2_cache,   H4_CACHE_TTL, 400),         # ← 200 → 400 (~33 jours)
    "1h":  (_h1_cache,   120,          500),          # ← 300 → 500 (~20 jours)
    "30m": (_m30_cache,  90,           500),          # ← 300 → 500 (~10 jours)
    "15m": (_m15_cache,  60,           500),          # ← 300 → 500 (~5 jours)
    "5m":  (_m5_cache,   M5_CACHE_TTL, 1000),         # ← 600 → 1000 (~3.5 jours)
    "3m":  (_m3_cache,   20,           150),          # ← 100 → 150
    "1m":  (_m1_cache,   M1_CACHE_TTL, 500),          # ← 260 → 500 (~8h)
}

async def _get_cached(session: aiohttp.ClientSession, sym: str, interval: str) -> pd.DataFrame:
    """Helper unifié : retourne les klines depuis le cache ou via REST."""
    cache_dict, ttl, limit = _CACHE_REGISTRY[interval]
    now = datetime.now(timezone.utc).timestamp()
    cached = cache_dict.get(sym)
    if cached and (now - cached[0]) < ttl:
        return cached[1]
    df = await fetch_klines(session, sym, interval, limit)
    cache_dict[sym] = (now, df)
    return df

async def get_h4_cached(session, sym):  return await _get_cached(session, sym, "4h")
async def get_h2_cached(session, sym):  return await _get_cached(session, sym, "2h")
async def get_h1_cached(session, sym):  return await _get_cached(session, sym, "1h")
async def get_m30_cached(session, sym): return await _get_cached(session, sym, "30m")
async def get_m15_cached(session, sym): return await _get_cached(session, sym, "15m")
async def get_m5_cached(session, sym):  return await _get_cached(session, sym, "5m")
async def get_m3_cached(session, sym):  return await _get_cached(session, sym, "3m")
async def get_m1_cached(session, sym):  return await _get_cached(session, sym, "1m")
async def get_1d_cached(session, sym):  return await _get_cached(session, sym, "1d")  # ← NEW v7


# ---------------------------------------------------------------------------
# v7 — FETCH FUTURES METRICS (Open Interest + Funding Rate + Mark Price)
# ---------------------------------------------------------------------------
async def fetch_futures_metrics(session: aiohttp.ClientSession, sym: str) -> dict:
    """Fetche OI + funding rate + mark price via Binance Futures REST (read-only).

    Retourne un dict :
      {"oi": float, "oi_change_pct": float, "funding": float,
       "mark_price": float, "long_short_ratio": float|None, "ok": bool}

    Utilise le cache FUTURES_CACHE_TTL secondes (défaut 60s).
    """
    if not FUTURES_ENABLED:
        return {"ok": False, "reason": "futures_disabled"}

    fsym = FUTURES_SYMBOLS_MAP.get(sym)
    if not fsym:
        return {"ok": False, "reason": "not_a_futures_symbol"}

    now = datetime.now(timezone.utc).timestamp()
    cached = _futures_cache.get(sym)
    if cached and (now - cached[0]) < FUTURES_CACHE_TTL:
        return cached[1]

    headers = {"X-MBX-APIKEY": BINANCE_KEY} if BINANCE_KEY else {}
    result: dict = {"ok": False, "sym": sym, "fsym": fsym}

    try:
        # 1. Open Interest
        async with session.get(
            f"{FUTURES_BASE}/fapi/v1/openInterest",
            params={"symbol": fsym},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                result["oi"] = float(d.get("openInterest", 0.0) or 0.0)

        # 2. Open Interest history (pour calculer la variation)
        async with session.get(
            f"{FUTURES_BASE}/futures/data/openInterestHist",
            params={"symbol": fsym, "period": "5m", "limit": "2"},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                hist = await r.json(content_type=None)
                if isinstance(hist, list) and len(hist) >= 2:
                    oi_prev = float(hist[-2].get("sumOpenInterest", 0.0) or 0.0)
                    oi_cur  = float(hist[-1].get("sumOpenInterest", 0.0) or 0.0)
                    result["oi_change_pct"] = round(
                        (oi_cur - oi_prev) / max(oi_prev, 1e-9) * 100.0, 3
                    )

        # 3. Premium index (funding rate + mark price)
        async with session.get(
            f"{FUTURES_BASE}/fapi/v1/premiumIndex",
            params={"symbol": fsym},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                result["funding"]    = float(d.get("lastFundingRate", 0.0) or 0.0)
                result["mark_price"] = float(d.get("markPrice", 0.0) or 0.0)

        # 4. Long/Short ratio (proxy de sentiment)
        async with session.get(
            f"{FUTURES_BASE}/futures/data/globalLongShortAccountRatio",
            params={"symbol": fsym, "period": "5m", "limit": "1"},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                ls = await r.json(content_type=None)
                if isinstance(ls, list) and ls:
                    result["long_short_ratio"] = float(ls[0].get("longShortRatio", 1.0) or 1.0)

        result["ok"] = True
        logger.debug("[FUTURES] %s OI=%.0f funding=%.6f mark=%.2f", sym, result.get("oi", 0), result.get("funding", 0), result.get("mark_price", 0))

    except Exception as e:
        logger.debug("[FUTURES] fetch error %s: %s", sym, e)
        result["ok"] = False
        result["error"] = str(e)[:80]

    _futures_cache[sym] = (now, result)
    return result


# ---------------------------------------------------------------------------
# v7 — XAU/USD Gold réel — Dual provider : Twelve Data (primary) + Yahoo Finance (fallback)
# ---------------------------------------------------------------------------

def _parse_twelvedata_df(data: dict) -> pd.DataFrame:
    """Parse la réponse JSON de Twelve Data en DataFrame OHLCV."""
    values = data.get("values", [])
    if not values:
        return pd.DataFrame()
    rows = []
    for v in values:
        try:
            rows.append({
                "ts":    pd.to_datetime(v["datetime"], utc=True),
                "open":  float(v["open"]),
                "high":  float(v["high"]),
                "low":   float(v["low"]),
                "close": float(v["close"]),
                "v":     float(v.get("volume", 0) or 0),
            })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).set_index("ts").sort_index()
    return df[~df.index.duplicated(keep="last")]


async def _fetch_twelvedata(
    session: aiohttp.ClientSession,
    symbol: str = "XAU/USD",
    interval: str = "4h",
    outputsize: int = 500,
) -> pd.DataFrame:
    """Fetch bougies XAU/USD depuis Twelve Data REST API.

    Gratuit : 800 crédits/jour (1 bougie = 1 crédit).
    Clé gratuite sur https://twelvedata.com (inscription 30 secondes).
    """
    if not TWELVEDATA_API_KEY:
        return pd.DataFrame()
    td_interval = _TD_TF_MAP.get(interval, interval)
    params = {
        "symbol":     symbol,
        "interval":   td_interval,
        "outputsize": str(min(5000, max(1, outputsize))),
        "apikey":     TWELVEDATA_API_KEY,
        "timezone":   "UTC",
        "format":     "JSON",
    }
    try:
        async with session.get(
            f"{TWELVEDATA_BASE_URL}/time_series",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                body = await r.text()
                logger.warning("[TWELVEDATA] HTTP %s %s/%s: %s", r.status, symbol, interval, body[:120])
                return pd.DataFrame()
            data = await r.json(content_type=None)

        # Twelve Data retourne {"code":400,"message":"..."} en cas d'erreur
        if isinstance(data, dict) and data.get("code"):
            logger.warning("[TWELVEDATA] Erreur API %s: %s", data.get("code"), data.get("message", "")[:100])
            return pd.DataFrame()

        df = _parse_twelvedata_df(data)
        if not df.empty:
            logger.info("[TWELVEDATA] %s/%s → %s bougies", symbol, interval, len(df))
        return df

    except Exception as e:
        logger.warning("[TWELVEDATA] fetch error %s/%s: %s", symbol, interval, e)
        return pd.DataFrame()


async def _fetch_yahoo_gold(
    session: aiohttp.ClientSession,
    interval: str = "4h",
    count: int = 500,
) -> pd.DataFrame:
    """Fetch bougies Gold depuis Yahoo Finance (aucune clé requise).

    Symbole : GC=F (Gold Futures) — très proche du spot XAU/USD.
    Fallback automatique si Twelve Data est indisponible ou quota épuisé.
    Limites Yahoo : intervalles 1h–1d pour >7 jours d'historique.
    """
    # Mapping interval → (yf_interval, period)
    _yf_cfg = {
        "4h":  ("1h",  "60d"),
        "2h":  ("1h",  "60d"),
        "1h":  ("1h",  "60d"),
        "30m": ("30m", "30d"),
        "15m": ("15m", "10d"),
        "5m":  ("5m",  "5d"),
        "3m":  ("5m",  "5d"),
        "1m":  ("1m",  "7d"),
        "1d":  ("1d",  "730d"),
    }
    yf_interval, period = _yf_cfg.get(interval, ("1h", "60d"))

    # Yahoo Finance query API (endpoint non documenté mais stable)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/GC%3DF"
    params = {
        "interval":     yf_interval,
        "range":        period,
        "includePrePost": "false",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept":     "application/json",
    }

    try:
        async with session.get(
            url, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                logger.warning("[YF_GOLD] HTTP %s GC=F/%s", r.status, yf_interval)
                return pd.DataFrame()
            data = await r.json(content_type=None)

        result = (data.get("chart") or {}).get("result") or []
        if not result:
            return pd.DataFrame()

        chart  = result[0]
        ts_arr = chart.get("timestamp", [])
        quote  = (chart.get("indicators") or {}).get("quote", [{}])[0]

        opens  = quote.get("open",  [])
        highs  = quote.get("high",  [])
        lows   = quote.get("low",   [])
        closes = quote.get("close", [])
        vols   = quote.get("volume",[])

        rows = []
        for i, ts in enumerate(ts_arr):
            try:
                o = opens[i]; h = highs[i]; l = lows[i]; c = closes[i]
                if any(x is None for x in (o, h, l, c)):
                    continue
                rows.append({
                    "ts":    pd.to_datetime(ts, unit="s", utc=True),
                    "open":  float(o), "high": float(h),
                    "low":   float(l), "close": float(c),
                    "v":     float(vols[i] or 0) if i < len(vols) else 0.0,
                })
            except Exception:
                continue

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows).set_index("ts").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        # Resample si nécessaire (ex: 4h demandé → on a du 1h)
        if interval in ("4h", "2h") and yf_interval == "1h":
            rule = "4h" if interval == "4h" else "2h"
            df = df.resample(rule).agg({
                "open": "first", "high": "max", "low": "min",
                "close": "last", "v": "sum"
            }).dropna()
        logger.info("[YF_GOLD] GC=F/%s → %s bougies (fallback Yahoo)", interval, len(df))
        return df

    except Exception as e:
        logger.warning("[YF_GOLD] fetch error %s: %s", interval, e)
        return pd.DataFrame()


async def fetch_gold_candles(
    session: aiohttp.ClientSession,
    interval: str = "4h",
    count: int = 500,
) -> pd.DataFrame:
    """Fetch XAU/USD — dual provider avec cache partagé.

    Ordre de priorité :
      1. Twelve Data (si TWELVEDATA_API_KEY configuré) → données XAU/USD réelles
      2. Yahoo Finance GC=F (aucune clé) → Gold Futures (~XAU/USD spot)
      3. Retour DataFrame vide → le bot utilise PAXG/USDT comme fallback

    Cache : GOLD_CACHE_TTL secondes (défaut 180s).
    """
    if not GOLD_REAL_ENABLED:
        return pd.DataFrame()

    cache_key = f"xauusd_{interval}"
    now = datetime.now(timezone.utc).timestamp()
    cached = _gold_cache.get(cache_key)
    if cached and (now - cached[0]) < GOLD_CACHE_TTL:
        return cached[1]

    df = pd.DataFrame()

    # 1. Twelve Data (primary)
    if TWELVEDATA_API_KEY:
        df = await _fetch_twelvedata(session, "XAU/USD", interval, count)

    # 2. Yahoo Finance (fallback automatique)
    if df.empty:
        df = await _fetch_yahoo_gold(session, interval, count)

    if not df.empty:
        _gold_cache[cache_key] = (now, df)
        logger.debug("[GOLD] %s → %s bougies (cache mis à jour)", interval, len(df))

    return df


# ---------------------------------------------------------------------------
# v7 — Mise à jour Delta Volume depuis un trade aggTrade
# ---------------------------------------------------------------------------
def _update_delta_vol(sym: str, price: float, qty: float, is_buyer_maker: bool) -> None:
    """Met à jour le delta volume rolling pour un symbole.

    is_buyer_maker=True  → c'est une VENTE agressive (seller hit bid)
    is_buyer_maker=False → c'est un ACHAT agressif (buyer lifted ask)
    """
    if not DELTA_VOL_ENABLED:
        return
    dv = _delta_vol.get(sym)
    if dv is None:
        return

    trades: _deque = dv["trades"]
    # trade = (qty, is_buy: bool)
    is_buy = not is_buyer_maker  # achat agressif si l'acheteur n'est pas maker
    trades.append((qty, is_buy))

    buy_vol  = sum(q for q, b in trades if b)
    sell_vol = sum(q for q, b in trades if not b)
    total    = buy_vol + sell_vol

    delta     = buy_vol - sell_vol
    delta_pct = (buy_vol / max(total, 1e-12)) if total > 0 else 0.5

    dv["buy_vol"]   = buy_vol
    dv["sell_vol"]  = sell_vol
    dv["delta"]     = delta
    dv["delta_pct"] = round(delta_pct, 4)
    dv["bullish"]   = delta_pct >= DELTA_VOL_SIGNAL_PCT       # ≥60% achat
    dv["bearish"]   = delta_pct <= (1.0 - DELTA_VOL_SIGNAL_PCT)  # ≤40% achat = ≥60% vente
    dv["ts"]        = datetime.now(timezone.utc).timestamp()

# ---------------------------------------------------------------------------
# STRICT TRIX — calcul + backtest (Sharpe) + recalibrage 90j
# ---------------------------------------------------------------------------

def _ma(s: pd.Series, length: int, ma_type: str) -> pd.Series:
    ma_type = (ma_type or "sma").lower()
    if ma_type == "ema":
        return s.ewm(span=int(length), adjust=False).mean()
    return s.rolling(int(length)).mean()


def _trix_line(close: pd.Series, length: int) -> pd.Series:
    """TRIX = ROC(EMA(EMA(EMA(close)))) en %."""
    length = int(length)
    e1 = close.ewm(span=length, adjust=False).mean()
    e2 = e1.ewm(span=length, adjust=False).mean()
    e3 = e2.ewm(span=length, adjust=False).mean()
    return 100.0 * (e3 / e3.shift(1) - 1.0)


def strict_trix_apply(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """Ajoute trix/signal/hist/trend_ma + entry/exit (strict)."""
    out = df.copy()
    if out.empty:
        return out

    close = out["close"].astype(float)
    trix_len = int(params.get("trix_len", 18))
    sig_len = int(params.get("signal_len", 9))
    sig_ma = str(params.get("signal_ma", "sma")).lower()
    trend_len = int(params.get("trend_ma_len", 300))
    trend_ma = str(params.get("trend_ma", "sma")).lower()

    out["trix"] = _trix_line(close, trix_len)
    out["trix_signal"] = _ma(out["trix"], sig_len, sig_ma)
    out["trix_hist"] = out["trix"] - out["trix_signal"]

    out["trend_ma"] = _ma(close, trend_len, trend_ma)
    in_trend = close > out["trend_ma"]

    hist = out["trix_hist"]
    cross_up = (hist > 0) & (hist.shift(1) <= 0)
    cross_dn = (hist < 0) & (hist.shift(1) >= 0)

    out["entry_long"] = (cross_up & in_trend).fillna(False)
    out["exit_long"] = cross_dn.fillna(False)
    return out


def strict_trix_backtest_sharpe(df: pd.DataFrame, params: dict, fee_bps: float = 4.0) -> dict:
    """Backtest long-only (entrée/sortie à la clôture), Sharpe sur trades (simple & rapide)."""
    d = strict_trix_apply(df, params)
    if d.empty or len(d) < 200:
        return {"trades": 0, "return": 0.0, "sharpe": -1e9, "max_dd": 0.0}

    close = d["close"].astype(float).to_numpy()
    fee = float(fee_bps) / 10000.0

    pos = 0
    entry_px = 0.0
    equity = 1.0
    eq_curve = [equity]
    rets = []

    for i in range(1, len(d)):
        if pos == 0 and bool(d["entry_long"].iloc[i]):
            pos = 1
            entry_px = close[i] * (1.0 + fee)
        elif pos == 1 and bool(d["exit_long"].iloc[i]):
            exit_px = close[i] * (1.0 - fee)
            r = (exit_px / entry_px) - 1.0
            rets.append(float(r))
            equity *= (1.0 + r)
            eq_curve.append(equity)
            pos = 0

    if not rets:
        return {"trades": 0, "return": 0.0, "sharpe": -1e9, "max_dd": 0.0}

    r = np.array(rets, dtype=float)
    mean = float(r.mean())
    std = float(r.std(ddof=1)) if len(r) > 1 else 0.0
    sharpe = mean / (std + 1e-12) * (len(r) ** 0.5)

    eq = np.array(eq_curve, dtype=float)
    peak = np.maximum.accumulate(eq)
    dd = (eq / peak) - 1.0
    max_dd = float(dd.min())

    return {"trades": int(len(r)), "return": float(equity - 1.0), "sharpe": float(sharpe), "max_dd": max_dd}


def _rand_params(rng: np.random.Generator) -> dict:
    # Plages inspirées de la vidéo (TRIX len 5-51, signal 5-51, trend 100-800)
    trix_len = int(rng.integers(5, 52))
    signal_len = int(rng.integers(5, 52))
    trend_ma_len = int(rng.choice([100, 200, 300, 400, 500, 600, 700, 800]))
    signal_ma = "ema" if rng.random() < 0.5 else "sma"
    return {
        "trix_len": trix_len,
        "signal_len": signal_len,
        "signal_ma": signal_ma,
        "trend_ma_len": trend_ma_len,
        "trend_ma": "sma",
    }

def _split_periods(df: pd.DataFrame, subperiod_days: int) -> list[pd.DataFrame]:
    """Découpe df en sous-périodes temporelles (ex: 90j) sur la fin."""
    if df.empty:
        return []
    subperiod_days = max(7, int(subperiod_days))
    end = df.index.max()
    start = df.index.min()
    # on construit des fenêtres [end-90j, end], puis on remonte
    periods = []
    cur_end = end
    while True:
        cur_start = cur_end - pd.Timedelta(days=subperiod_days)
        if cur_start <= start:
            chunk = df.loc[start:cur_end]
            if len(chunk) > 50:
                periods.append(chunk)
            break
        chunk = df.loc[cur_start:cur_end]
        if len(chunk) > 50:
            periods.append(chunk)
        cur_end = cur_start
        if len(periods) >= 12:  # garde-fou
            break
    periods.reverse()
    return periods


def _robust_score(sharpes: list[float], floor: float, min_periods: int) -> float:
    """Score de robustesse: privilégie (1) nb de périodes > seuil, (2) moyenne, (3) faible variance."""
    if not sharpes:
        return -1e9
    ok = [s for s in sharpes if s >= floor]
    if len(ok) < int(min_periods):
        return -1e9
    arr = np.array(sharpes, dtype=float)
    # moyenne - pénalité variance + bonus pass ratio
    mean = float(arr.mean())
    std = float(arr.std(ddof=0))
    pass_ratio = float(len(ok) / len(sharpes))
    return mean - 0.35 * std + 0.25 * pass_ratio


def _gaussian_blur2d(mat: np.ndarray) -> np.ndarray:
    """Petit flou gaussien 5x5 (sans scipy)."""
    # kernel gaussien 5x5 approx
    k = np.array([
        [1,  4,  7,  4, 1],
        [4, 16, 26, 16, 4],
        [7, 26, 41, 26, 7],
        [4, 16, 26, 16, 4],
        [1,  4,  7,  4, 1],
    ], dtype=float)
    k /= k.sum()

    h, w = mat.shape
    out = np.full_like(mat, -1e9, dtype=float)

    # padding par réplication des bords
    pad = 2
    padded = np.pad(mat, ((pad, pad), (pad, pad)), mode='edge')

    for i in range(h):
        for j in range(w):
            window = padded[i:i+5, j:j+5]
            # ignore -inf cells by treating them as very low
            out[i, j] = float((window * k).sum())
    return out


def _pick_zone_centers(score_smoothed: np.ndarray, topk: int, min_dist: int) -> list[tuple[int,int,float]]:
    """Retourne centres (i,j,score) des zones chaudes, avec suppression locale."""
    topk = max(1, int(topk))
    min_dist = max(1, int(min_dist))
    work = score_smoothed.copy()
    picks = []
    for _ in range(topk):
        idx = np.unravel_index(int(np.argmax(work)), work.shape)
        best = float(work[idx])
        if best <= -1e8:
            break
        i, j = int(idx[0]), int(idx[1])
        picks.append((i, j, best))
        # supprime une zone autour
        i0 = max(0, i - min_dist)
        i1 = min(work.shape[0], i + min_dist + 1)
        j0 = max(0, j - min_dist)
        j1 = min(work.shape[1], j + min_dist + 1)
        work[i0:i1, j0:j1] = -1e9
    return picks



def _fetch_start_ms(days: int) -> int:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return now_ms - int(days) * 24 * 3600 * 1000


async def fetch_klines_history(session: aiohttp.ClientSession, sym: str, interval: str, days: int) -> pd.DataFrame:
    """Récupère un historique sur N jours — correctifs v4.1.

    - Timeout 30s/page + retry 3x (réseau lent, PAXG peu liquide)
    - Avance par durée de bougie exacte (évite doublons aux jonctions)
    - Déduplication finale par index
    - Log détaillé pour debug insufficient_history
    - Stop propre si page incomplète (fin des données disponibles)
    """
    start_ms = _fetch_start_ms(days)
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    limit = 1000

    def _interval_minutes(iv: str) -> float:
        iv = (iv or "").strip().lower()
        try:
            if iv.endswith("m"): return float(iv[:-1])
            if iv.endswith("h"): return float(iv[:-1]) * 60.0
            if iv.endswith("d"): return float(iv[:-1]) * 1440.0
        except Exception:
            pass
        return 5.0

    iv_min = max(1.0, _interval_minutes(interval))
    iv_ms  = int(iv_min * 60 * 1000)
    days_per_page = (limit * iv_min) / 1440.0
    pages_needed  = int((max(1, int(days)) / max(1e-9, days_per_page)) + 6)
    max_pages     = max(60, min(800, pages_needed))

    for base in (REST_BASE, REST_FALLBACK):
        cur = start_ms
        all_rows: list = []
        pages = 0
        consecutive_empty = 0
        try:
            while cur < end_ms and pages < max_pages:
                pages += 1
                params = {
                    "symbol":    rest_sym(sym),
                    "interval":  interval,
                    "limit":     str(limit),
                    "startTime": str(cur),
                }
                # Retry 3x par page
                data = None
                for attempt in range(3):
                    try:
                        async with session.get(
                            f"{base}/api/v3/klines", params=params,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as r:
                            if r.status == 429:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            if r.status != 200:
                                raise RuntimeError(f"HTTP {r.status}")
                            data = await r.json(content_type=None)
                            break
                    except Exception as page_err:
                        if attempt == 2:
                            raise page_err
                        await asyncio.sleep(1)

                if not data:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    continue
                consecutive_empty = 0

                all_rows.extend(data)
                last_open = int(data[-1][0])
                nxt = last_open + iv_ms
                if nxt <= cur:
                    break
                cur = nxt
                if len(data) < limit:
                    break  # page incomplète = fin des données disponibles

            if not all_rows:
                logger.warning("[STRICT] fetch_klines_history %s/%s: 0 bougies via %s", sym, interval, base)
                continue

            rows = [{
                "ts":    pd.to_datetime(int(k[0]), unit="ms", utc=True),
                "open":  float(k[1]), "high": float(k[2]),
                "low":   float(k[3]), "close": float(k[4]), "v": float(k[5]),
            } for k in all_rows]

            df = pd.DataFrame(rows).set_index("ts").sort_index()
            df = df[~df.index.duplicated(keep="last")]

            logger.info(
                "[STRICT] fetch_klines_history %s/%s/%sd → %s bougies (pages=%s) via %s",
                sym, interval, days, len(df), pages, base
            )
            return df

        except Exception as e:
            logger.warning("[STRICT] fetch_klines_history %s (%s): %s", sym, base, e)
            continue

    return pd.DataFrame()


def _compute_strict_zones_for_symbol(df: pd.DataFrame, periods: list[pd.DataFrame], progress_hook=None) -> dict:
    """CPU heavy part (runs in thread): build robust heatmap, blur, pick zones.

    Mode STRICT:
      - robust_score computed with (STRICT_SHARPE_FLOOR, STRICT_ROBUST_MIN_PERIODS)

    Mode RELAXED fallback (si aucune cellule en strict):
      - floor_relaxed = STRICT_SHARPE_FLOOR - 0.4 (min 0.3)
      - min_ok_relaxed = STRICT_ROBUST_MIN_PERIODS - 1 (min 2)

    Le but est d'obtenir AU MOINS une heatmap exploitable en debug, même si les
    contraintes strictes sont trop agressives pour la période.
    """
    rng = np.random.default_rng(42)

    x_vals = list(range(5, 52))
    y_vals = list(range(5, 52))
    x_to_i = {v: idx for idx, v in enumerate(x_vals)}
    y_to_j = {v: idx for idx, v in enumerate(y_vals)}

    # STRICT
    score_best = np.full((len(x_vals), len(y_vals)), -1e9, dtype=float)
    best_params_cell: dict[tuple[int,int], dict] = {}
    best_detail_cell: dict[tuple[int,int], dict] = {}

    # RELAXED (floor - 0.2, min_ok - 1)
    floor_relaxed = max(0.1, float(STRICT_SHARPE_FLOOR) - 0.2)
    min_ok_relaxed = max(1, int(STRICT_ROBUST_MIN_PERIODS) - 1)
    score_best_rel = np.full((len(x_vals), len(y_vals)), -1e9, dtype=float)
    best_params_cell_rel: dict[tuple[int,int], dict] = {}
    best_detail_cell_rel: dict[tuple[int,int], dict] = {}

    # ULTRA-RELAXED : accepte tout Sharpe > 0, 1 seule période, 1 seul trade
    # Fallback de dernier recours pour toujours produire un résultat
    floor_ultra = 0.0
    min_ok_ultra = 1
    score_best_ultra = np.full((len(x_vals), len(y_vals)), -1e9, dtype=float)
    best_params_cell_ultra: dict[tuple[int,int], dict] = {}
    best_detail_cell_ultra: dict[tuple[int,int], dict] = {}

    # BEST-EFFORT : meilleure config par Sharpe moyen brut (sans critère de robustesse)
    # Utilisé en dernier recours absolu si même ultra-relaxed échoue
    score_best_effort = np.full((len(x_vals), len(y_vals)), -1e9, dtype=float)
    best_params_effort: dict[tuple[int,int], dict] = {}
    best_detail_effort: dict[tuple[int,int], dict] = {}

    iters = max(200, int(STRICT_RANDOM_ITERS))
    if progress_hook:
        try:
            progress_hook(0, iters, phase='compute_heatmap')
        except Exception:
            pass
    for i in range(iters):
        if progress_hook and (i % 25 == 0):
            try:
                progress_hook(i, iters, phase='compute_heatmap')
            except Exception:
                pass
        p = _rand_params(rng)

        sharpes_strict = []   # périodes avec >= STRICT_MIN_TRADES trades
        sharpes_any    = []   # toutes les périodes avec >= 1 trade (pour best_effort)

        for chunk in periods:
            m = strict_trix_backtest_sharpe(chunk, p, fee_bps=STRICT_FEE_BPS)
            if m["trades"] >= 1:
                sharpes_any.append(float(m["sharpe"]))
            if m["trades"] >= max(1, int(STRICT_MIN_TRADES)):
                sharpes_strict.append(float(m["sharpe"]))

        # Utiliser sharpes_strict pour strict/relaxed/ultra, sharpes_any pour best_effort
        sharpes = sharpes_strict

        if not sharpes_any:
            continue  # aucun trade dans aucune période — skip complet

        xi = x_to_i.get(int(p["trix_len"]))
        yj = y_to_j.get(int(p["signal_len"]))
        if xi is None or yj is None:
            continue

        # Modes stricts (utilisent sharpes_strict)
        if sharpes:
            robust       = _robust_score(sharpes, STRICT_SHARPE_FLOOR, STRICT_ROBUST_MIN_PERIODS)
            robust_rel   = _robust_score(sharpes, floor_relaxed,       min_ok_relaxed)
            robust_ultra = _robust_score(sharpes, floor_ultra,         min_ok_ultra)

            if robust > -1e8 and robust > float(score_best[xi, yj]):
                score_best[xi, yj] = float(robust)
                best_params_cell[(xi, yj)] = p
                best_detail_cell[(xi, yj)] = {
                    "robust": float(robust), "sharpes": [float(s) for s in sharpes],
                    "floor": float(STRICT_SHARPE_FLOOR), "min_ok": int(STRICT_ROBUST_MIN_PERIODS),
                    "subperiod_days": int(STRICT_SUBPERIOD_DAYS),
                }

            if robust_rel > -1e8 and robust_rel > float(score_best_rel[xi, yj]):
                score_best_rel[xi, yj] = float(robust_rel)
                best_params_cell_rel[(xi, yj)] = p
                best_detail_cell_rel[(xi, yj)] = {
                    "robust": float(robust_rel), "sharpes": [float(s) for s in sharpes],
                    "floor": float(floor_relaxed), "min_ok": int(min_ok_relaxed),
                    "subperiod_days": int(STRICT_SUBPERIOD_DAYS),
                }

            if robust_ultra > -1e8 and robust_ultra > float(score_best_ultra[xi, yj]):
                score_best_ultra[xi, yj] = float(robust_ultra)
                best_params_cell_ultra[(xi, yj)] = p
                best_detail_cell_ultra[(xi, yj)] = {
                    "robust": float(robust_ultra), "sharpes": [float(s) for s in sharpes],
                    "floor": float(floor_ultra), "min_ok": int(min_ok_ultra),
                    "subperiod_days": int(STRICT_SUBPERIOD_DAYS),
                }

        # Best-effort : toujours calculé avec sharpes_any (>= 1 trade par période)
        mean_sharpe = float(np.mean(sharpes_any))
        if mean_sharpe > float(score_best_effort[xi, yj]):
            score_best_effort[xi, yj] = mean_sharpe
            best_params_effort[(xi, yj)] = p
            best_detail_effort[(xi, yj)] = {
                "robust": mean_sharpe, "sharpes": [float(s) for s in sharpes_any],
                "floor": 0.0, "min_ok": 1,
                "subperiod_days": int(STRICT_SUBPERIOD_DAYS),
            }

    # ── Sélection du meilleur mode disponible ─────────────────────
    mode = "strict"
    if best_params_cell:
        mode = "strict"
    elif best_params_cell_rel:
        mode = "relaxed"
        score_best      = score_best_rel
        best_params_cell = best_params_cell_rel
        best_detail_cell = best_detail_cell_rel
    elif best_params_cell_ultra:
        mode = "ultra_relaxed"
        score_best      = score_best_ultra
        best_params_cell = best_params_cell_ultra
        best_detail_cell = best_detail_cell_ultra
    elif best_params_effort:
        mode = "best_effort"
        score_best      = score_best_effort
        best_params_cell = best_params_effort
        best_detail_cell = best_detail_effort
    else:
        return {"ok": False, "mode": "no_robust_cells"}

    smoothed = _gaussian_blur2d(score_best)
    picks = _pick_zone_centers(smoothed, topk=STRICT_TOPK_ZONES, min_dist=STRICT_ZONE_MIN_DIST)
    if not picks:
        return {"ok": False, "mode": "no_picks"}

    (i0, j0, _) = picks[0]
    p0 = best_params_cell.get((i0, j0))
    d0 = best_detail_cell.get((i0, j0), {})

    p1 = None
    d1 = None
    if len(picks) >= 2:
        (i1, j1, _) = picks[1]
        p1 = best_params_cell.get((i1, j1))
        d1 = best_detail_cell.get((i1, j1), {})

    top_cells = [{"trix_len": x_vals[ii], "signal_len": y_vals[jj], "score": float(sc)} for (ii, jj, sc) in picks]

    # store scaled arrays to keep JSON small
    scaled = np.where(score_best <= -1e8, -9999, np.round(score_best * 100).astype(int))
    scaled_sm = np.where(smoothed <= -1e8, -9999, np.round(smoothed * 100).astype(int))

    # metrics on full df for reporting
    m0 = strict_trix_backtest_sharpe(df, p0, fee_bps=STRICT_FEE_BPS) if p0 else None
    m1 = strict_trix_backtest_sharpe(df, p1, fee_bps=STRICT_FEE_BPS) if p1 else None

    return {
        "ok": True,
        "mode": mode,
        "params": p0,
        "params_alt": p1,
        "metrics": m0,
        "metrics_alt": m1,
        "zones": top_cells,
        "zone_detail": d0,
        "zone_detail_alt": d1,
        "heatmap": {
            "tf": STRICT_TF,
            "x": x_vals,
            "y": y_vals,
            "score": scaled.tolist(),
            "score_smoothed": scaled_sm.tolist(),
            "top": top_cells,
            "floor": float(d0.get("floor", STRICT_SHARPE_FLOOR)),
            "subperiod_days": int(STRICT_SUBPERIOD_DAYS),
            "min_ok": int(d0.get("min_ok", STRICT_ROBUST_MIN_PERIODS)),
        },
    }


# ---------------------------------------------------------------------------
# STRICT — MODE PRO: worker process + queue (zéro blocage event-loop)
# ---------------------------------------------------------------------------

def _normalize_sym(sym: str) -> str:
    return (sym or '').upper().replace('_','/').strip()

async def _strict_enqueue(sym: str, job_q) -> dict:
    sym = _normalize_sym(sym)
    if sym not in SYMBOLS:
        return {"ok": False, "error": "unknown_symbol", "symbol": sym}

    if _strict_pending.get(sym) or _strict_running.get(sym):
        return {"ok": True, "started": False, "symbol": sym, "job_id": _strict_job_id.get(sym, "")}

    job_id = str(uuid.uuid4())
    payload = {"job_id": job_id, "symbol": sym}
    try:
        job_q.put_nowait(payload)
    except Exception:
        return {"ok": False, "error": "queue_full", "symbol": sym}

    _strict_pending[sym] = True
    _strict_running[sym] = False  # ← FIX: ne pas marquer running ici, seulement pending
    _strict_job_id[sym] = job_id
    _strict_job_start_ts[sym] = datetime.now(timezone.utc).timestamp()
    _strict_job_started_ts[sym] = datetime.now(timezone.utc).timestamp()  # ← FIX: init aussi started_ts
    _strict_store[sym] = {
        "status": "queued",
        "job_id": job_id,
        "queued_at": datetime.now(timezone.utc).isoformat(),
        "next_recalib_ts": float((_strict_store.get(sym, {}) or {}).get("next_recalib_ts", 0.0) or 0.0),
    }
    return {"ok": True, "started": True, "symbol": sym, "job_id": job_id}

async def strict_scheduler_loop(job_q):
    """Planifie automatiquement le recalibrage (sans calcul CPU dans FastAPI)."""
    await asyncio.sleep(5)
    i = 0
    while True:
        try:
            if not SYMBOLS:
                await asyncio.sleep(60)
                continue

            sym = SYMBOLS[i % len(SYMBOLS)]
            i += 1
            now = datetime.now(timezone.utc).timestamp()
            st = _strict_store.get(sym, {}) or {}
            next_ts = float(st.get("next_recalib_ts", 0.0) or 0.0)

            if next_ts and now < next_ts:
                await asyncio.sleep(2)
                continue

            # ── Watchdog: reset jobs bloqués depuis trop longtemps ─────────
            job_start = float(_strict_job_start_ts.get(sym, 0.0) or 0.0)
            timeout_s = STRICT_JOB_TIMEOUT_MIN * 60
            if job_start > 0 and (now - job_start) > timeout_s:
                if _strict_pending.get(sym) or _strict_running.get(sym):
                    logger.warning("[STRICT] watchdog: job %s bloqué depuis >%dm — reset", sym, STRICT_JOB_TIMEOUT_MIN)
                    _strict_pending[sym] = False
                    _strict_running[sym] = False
                    _strict_job_start_ts[sym] = 0.0
                    _strict_store[sym] = {"error": "watchdog_reset", "next_recalib_ts": now + 300}
                    await asyncio.sleep(5)
                    continue

            if _strict_pending.get(sym) or _strict_running.get(sym):
                await asyncio.sleep(2)
                continue

            logger.info("[STRICT] (mode-pro) enqueue %s | tf=%s | in_sample=%sd | iters=%s", sym, STRICT_TF, STRICT_IN_SAMPLE_DAYS, STRICT_RANDOM_ITERS)
            await _strict_enqueue(sym, job_q)

        except Exception as e:
            logger.warning("[STRICT] scheduler error: %s", e)

        await asyncio.sleep(10)

async def strict_result_listener(res_q):
    """Récupère les résultats du worker process et met à jour les stores."""
    while True:
        try:
            res = await asyncio.to_thread(res_q.get)
            if not isinstance(res, dict):
                continue

            sym = _normalize_sym(res.get("symbol", ""))
            if not sym:
                continue

            # Signal "started" envoyé par le worker quand il commence (optionnel)
            if res.get("_event") == "started":
                _strict_running[sym] = True
                _strict_pending[sym] = False
                try:
                    st0 = _strict_store.get(sym, {}) or {}
                    _strict_store[sym] = {**st0, 'status': 'running', 'job_id': res.get('job_id'), 'started_at': datetime.now(timezone.utc).isoformat()}
                except Exception:
                    pass
                continue

            # Signal "progress" envoyé par le worker (phase/iters)
            if res.get("_event") == "progress":
                _strict_running[sym] = True
                _strict_pending[sym] = False
                _strict_job_started_ts.setdefault(sym, datetime.now(timezone.utc).timestamp())
                st0 = _strict_store.get(sym, {}) or {}
                if not isinstance(st0, dict):
                    st0 = {}
                phase = res.get('phase') or st0.get('phase') or 'compute'
                it_done = res.get('iters_done')
                it_tot = res.get('iters_total')
                st0.update({
                    'status': 'running',
                    'job_id': res.get('job_id') or st0.get('job_id'),
                    'phase': phase,
                    'iters_done': int(it_done) if isinstance(it_done, (int,float)) else st0.get('iters_done'),
                    'iters_total': int(it_tot) if isinstance(it_tot, (int,float)) else st0.get('iters_total'),
                    'updated_at': datetime.now(timezone.utc).isoformat(),
                })
                _strict_store[sym] = st0
                continue

            _strict_pending[sym] = False
            _strict_running[sym] = False
            _strict_job_start_ts[sym] = 0.0

            if not res.get("ok"):
                _strict_store[sym] = {
                    "error": res.get("error", "worker_failed"),
                    "job_id": res.get("job_id"),
                    "next_recalib_ts": datetime.now(timezone.utc).timestamp() + 3600,
                }
                continue

            heatmap = res.get("heatmap")
            if isinstance(heatmap, dict):
                _strict_heatmaps[sym] = heatmap

            st = res.get("strict_store")
            _strict_store[sym] = st if isinstance(st, dict) else {}

        except Exception as e:
            logger.warning("[STRICT] result listener error: %s", e)
            await asyncio.sleep(1)


def strict_worker_main(job_q, res_q):
    """Entrypoint du worker (process séparé)."""
    async def _run():
        timeout = aiohttp.ClientTimeout(total=45)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                job = await asyncio.to_thread(job_q.get)
                if not isinstance(job, dict):
                    continue

                job_id = job.get("job_id")
                sym = _normalize_sym(job.get("symbol", ""))
                if not sym:
                    continue

                # Signale au listener que le job démarre réellement
                res_q.put({"_event": "started", "symbol": sym, "job_id": job_id})

                try:
                    now = datetime.now(timezone.utc).timestamp()
                    res_q.put({"_event": "progress", "symbol": sym, "job_id": job_id, "phase": "fetch_history", "iters_done": 0, "iters_total": max(200, int(STRICT_RANDOM_ITERS))})
                    df = await fetch_klines_history(session, sym, STRICT_TF, STRICT_IN_SAMPLE_DAYS)
                    if STRICT_MAX_BARS and len(df) > int(STRICT_MAX_BARS):
                        df = df.tail(int(STRICT_MAX_BARS))

                    if df.empty or len(df) < 800:
                        res_q.put({"ok": False, "error": "insufficient_history", "symbol": sym, "job_id": job_id})
                        continue

                    res_q.put({"_event": "progress", "symbol": sym, "job_id": job_id, "phase": "split_periods", "iters_done": 0, "iters_total": max(200, int(STRICT_RANDOM_ITERS))})
                    periods = _split_periods(df, STRICT_SUBPERIOD_DAYS)
                    if len(periods) < int(STRICT_ROBUST_MIN_PERIODS):
                        res_q.put({"ok": False, "error": "not_enough_periods", "periods": len(periods), "symbol": sym, "job_id": job_id})
                        continue

                    # CPU heavy mais ici c'est OK: on est dans un process dédié
                    def _progress_hook(done, total, phase='compute_heatmap'):
                        try:
                            res_q.put({"_event": "progress", "symbol": sym, "job_id": job_id, "phase": phase, "iters_done": int(done), "iters_total": int(total)})
                        except Exception:
                            pass

                    out = _compute_strict_zones_for_symbol(df, periods, progress_hook=_progress_hook)
                    if not out.get("ok"):
                        res_q.put({"ok": False, "error": out.get("mode", "no_robust"), "symbol": sym, "job_id": job_id})
                        continue

                    m0 = out.get("metrics") or {}
                    strict_store = {
                        'params': out.get('params'),
                        'params_alt': out.get('params_alt'),
                        'trained_until': datetime.now(timezone.utc).isoformat(),
                        'sharpe': float(m0.get('sharpe', 0.0) if isinstance(m0, dict) else 0.0),
                        'metrics': out.get('metrics'),
                        'metrics_alt': out.get('metrics_alt'),
                        'next_recalib_ts': now + STRICT_RECALIB_DAYS * 24 * 3600,
                        'mode': 'stable_zones_worker_process',
                        'zones': out.get('zones'),
                        'zones_count': len(out.get('zones') or []) if isinstance(out.get('zones'), list) else 0,
                        'zone_detail': out.get('zone_detail'),
                        'zone_detail_alt': out.get('zone_detail_alt'),
                        'status': 'ok',
                        'job_id': job_id,
                    }

                    heatmap = {**out['heatmap'], 'generated_at': datetime.now(timezone.utc).isoformat()}

                    res_q.put({"ok": True, "symbol": sym, "job_id": job_id, "strict_store": strict_store, "heatmap": heatmap})

                except Exception as e:
                    res_q.put({"ok": False, "error": str(e), "symbol": sym, "job_id": job_id})

    asyncio.run(_run())

async def strict_recalibrate_symbol(session: aiohttp.ClientSession, sym: str) -> dict:
    """Recalibre un symbole strict (zones stables) une seule fois, sans bloquer l'event-loop."""
    sym = (sym or '').upper().replace('_','/')
    now = datetime.now(timezone.utc).timestamp()

    # Fetch réseau avec fallback candle_store
    df = None
    try:
        df_fetched = await fetch_klines_history(session, sym, STRICT_TF, STRICT_IN_SAMPLE_DAYS)
        if df_fetched is not None and not df_fetched.empty and len(df_fetched) >= 800:
            df = df_fetched
    except Exception as fe:
        logger.warning("[STRICT] recalibrate_symbol fetch %s échoué: %s", sym, fe)

    if df is None:
        df_cs = candle_store.get(sym)
        if df_cs is not None and not df_cs.empty and len(df_cs) >= 800:
            df = df_cs
            logger.info("[STRICT] recalibrate_symbol fallback candle_store %s: %s bougies", sym, len(df))
        else:
            n = len(df_cs) if df_cs is not None else 0
            _strict_store[sym] = {"error": f"insufficient_history ({n} bars)", "next_recalib_ts": now + 3600}
            return {"ok": False, "error": "insufficient_history", "bars": n}

    if STRICT_MAX_BARS and len(df) > int(STRICT_MAX_BARS):
        df = df.tail(int(STRICT_MAX_BARS))
    if len(df) < 800:
        _strict_store[sym] = {"error": f"insufficient_history ({len(df)} bars)", "next_recalib_ts": now + 3600}
        return {"ok": False, "error": "insufficient_history", "bars": len(df)}

    periods = _split_periods(df, STRICT_SUBPERIOD_DAYS)
    if len(periods) < int(STRICT_ROBUST_MIN_PERIODS):
        logger.warning("[STRICT] %s: %s périodes < %s requis (STRICT_SUBPERIOD_DAYS=%s, STRICT_ROBUST_MIN=%s)",
                       sym, len(periods), STRICT_ROBUST_MIN_PERIODS, STRICT_SUBPERIOD_DAYS, STRICT_ROBUST_MIN_PERIODS)
        _strict_store[sym] = {
            "error": f"not_enough_periods ({len(periods)}/{STRICT_ROBUST_MIN_PERIODS})",
            "next_recalib_ts": now + 3600
        }
        return {"ok": False, "error": "not_enough_periods", "periods": len(periods)}

    _strict_running[sym] = True
    try:
        res = await asyncio.to_thread(_compute_strict_zones_for_symbol, df, periods)
    finally:
        _strict_running[sym] = False

    if not res.get('ok'):
        _strict_store[sym] = {"error": res.get("mode", "no_robust"), "next_recalib_ts": now + 3600}
        return {"ok": False, "error": res.get('mode', 'no_robust')}

    _strict_heatmaps[sym] = {**res['heatmap'], 'generated_at': datetime.now(timezone.utc).isoformat()}

    m0 = res.get('metrics') or {}
    _strict_store[sym] = {
        'params': res.get('params'),
        'params_alt': res.get('params_alt'),
        'trained_until': datetime.now(timezone.utc).isoformat(),
        'sharpe': float(m0.get('sharpe', 0.0) if isinstance(m0, dict) else 0.0),
        'metrics': res.get('metrics'),
        'metrics_alt': res.get('metrics_alt'),
        'next_recalib_ts': now + STRICT_RECALIB_DAYS * 24 * 3600,
        'mode': 'stable_zones_heatmap_to_thread',
        'zones': res.get('zones'),
        'zone_detail': res.get('zone_detail'),
        'zone_detail_alt': res.get('zone_detail_alt'),
    }

    return {"ok": True, "symbol": sym, "sharpe": _strict_store[sym].get('sharpe', 0.0)}


async def strict_recalib_loop(session: aiohttp.ClientSession):
    """Recalibrage périodique (tous les STRICT_RECALIB_DAYS) des params STRICT TRIX par symbole.

    IMPORTANT: cette version offload la partie CPU-heavy (heatmap + backtests) via asyncio.to_thread
    pour ne PAS bloquer l'event-loop FastAPI (sinon /api/* peut "charger en boucle").

    Inspiré de l'approche expliquée dans la vidéo (robustesse > meilleur point).
    [Source] https://www.youtube.com/watch?v=SCT7ATLVMGg
    """
    i = 0

    await asyncio.sleep(3)

    while True:
        try:
            if not SYMBOLS:
                await asyncio.sleep(60)
                continue

            sym = SYMBOLS[i % len(SYMBOLS)]
            i += 1

            now = datetime.now(timezone.utc).timestamp()
            st = _strict_store.get(sym, {})
            next_ts = float(st.get("next_recalib_ts", 0.0) or 0.0)

            if next_ts and now < next_ts:
                await asyncio.sleep(5)
                continue

            logger.info(
                "[STRICT] Recalibrage %s | tf=%s | in_sample=%sd | iters=%s | floor=%.2f | subperiod=%sd | min_ok=%s",
                sym, STRICT_TF, STRICT_IN_SAMPLE_DAYS, STRICT_RANDOM_ITERS,
                STRICT_SHARPE_FLOOR, STRICT_SUBPERIOD_DAYS, STRICT_ROBUST_MIN_PERIODS,
            )

            # Marquer le début du job pour la durée (utilisé par l'UI via /api/strict/status)
            _strict_job_start_ts[sym] = now
            _strict_store[sym] = {
                **(_strict_store.get(sym) or {}),
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
            }

            df = None
            fetch_ok = False

            # ── Tentative 1 : fetch réseau ─────────────────────────
            try:
                df_fetched = await fetch_klines_history(session, sym, STRICT_TF, STRICT_IN_SAMPLE_DAYS)
                if df_fetched is not None and not df_fetched.empty and len(df_fetched) >= 800:
                    df = df_fetched
                    fetch_ok = True
                    logger.info("[STRICT] fetch OK %s: %s bougies", sym, len(df))
                else:
                    logger.warning("[STRICT] fetch %s: données insuffisantes (%s bougies) — essai fallback",
                                   sym, len(df_fetched) if df_fetched is not None else 0)
            except Exception as fetch_err:
                logger.warning("[STRICT] fetch %s échoué: %s — essai fallback", sym, fetch_err)

            # ── Tentative 2 : fallback candle_store ────────────────
            if df is None:
                df_cs = candle_store.get(sym)
                if df_cs is not None and not df_cs.empty and len(df_cs) >= 800:
                    df = df_cs
                    logger.info("[STRICT] fallback candle_store %s: %s bougies", sym, len(df))
                else:
                    n_cs = len(df_cs) if df_cs is not None else 0
                    logger.warning("[STRICT] Aucune donnée utilisable %s (fetch KO + candle_store=%s bougies)", sym, n_cs)
                    # Retry après délai exponentiel (max 30min)
                    retry_count = int((_strict_store.get(sym) or {}).get("_retry_count", 0))
                    retry_delay = min(1800, 300 * (2 ** retry_count))
                    _strict_store[sym] = {
                        "error": f"no_data (fetch KO + candle_store={n_cs})",
                        "next_recalib_ts": now + retry_delay,
                        "_retry_count": retry_count + 1,
                    }
                    await asyncio.sleep(15)
                    continue

            if STRICT_MAX_BARS and len(df) > int(STRICT_MAX_BARS):
                df = df.tail(int(STRICT_MAX_BARS))

            periods = _split_periods(df, STRICT_SUBPERIOD_DAYS)
            if len(periods) < int(STRICT_ROBUST_MIN_PERIODS):
                logger.warning("[STRICT] Pas assez de sous-périodes (%s) pour %s", len(periods), sym)
                _strict_store[sym] = {"error": "not_enough_periods", "next_recalib_ts": now + 3600}
                await asyncio.sleep(10)
                continue

            _strict_running[sym] = True
            try:
                # CPU heavy: run in thread
                res = await asyncio.to_thread(_compute_strict_zones_for_symbol, df, periods)
            finally:
                _strict_running[sym] = False

            if not res.get("ok"):
                logger.warning("[STRICT] zones stables indisponibles %s | mode=%s", sym, res.get("mode"))
                _strict_store[sym] = {"error": res.get("mode", "no_robust"), "next_recalib_ts": now + 3600}
                await asyncio.sleep(10)
                continue

            # publish heatmap for endpoint
            _strict_heatmaps[sym] = {
                **res["heatmap"],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            m0 = res.get("metrics") or {}
            _strict_store[sym] = {
                "params": res.get("params"),
                "params_alt": res.get("params_alt"),
                "trained_until": datetime.now(timezone.utc).isoformat(),
                "sharpe": float(m0.get("sharpe", 0.0) if isinstance(m0, dict) else 0.0),
                "metrics": res.get("metrics"),
                "metrics_alt": res.get("metrics_alt"),
                "next_recalib_ts": now + STRICT_RECALIB_DAYS * 24 * 3600,
                "mode": "stable_zones_heatmap_to_thread",
                "zones": res.get("zones"),
                "zone_detail": res.get("zone_detail"),
                "zone_detail_alt": res.get("zone_detail_alt"),
            }

            logger.info("[STRICT] OK %s | sharpe=%.3f | params=%s", sym, float(_strict_store[sym].get("sharpe", 0.0)), res.get("params"))

        except Exception as e:
            logger.warning("[STRICT] recalib loop error: %s", e)

        await asyncio.sleep(15)

# ---------------------------------------------------------------------------
# scan_loop
# ---------------------------------------------------------------------------
async def scan_loop(session: aiohttp.ClientSession):
    logger.info("[SCAN] scan_loop démarré")
    while True:
        for sym in SYMBOLS:
            try:
                df30 = candle_store.get(sym)
                if df30 is None or len(df30) < MIN_DF30_FOR_SCAN:
                    logger.debug("[SCAN] %s: pas assez de bougies (%s/%s)", sym, len(df30) if df30 is not None else 0, MIN_DF30_FOR_SCAN)
                    continue

                try:
                    await _persist_candle_store_if_needed()
                except Exception:
                    pass

                # ── Fetch tous les timeframes MTF ─────────────────────────
                df_h4, df_h2, df_h1, df_m30, df_m15, df_1m, df_5m, df_3m, df_1d = await asyncio.gather(
                    get_h4_cached(session, sym),
                    get_h2_cached(session, sym),
                    get_h1_cached(session, sym),
                    get_m30_cached(session, sym),
                    get_m15_cached(session, sym),
                    get_m1_cached(session, sym),
                    get_m5_cached(session, sym),
                    get_m3_cached(session, sym),
                    get_1d_cached(session, sym),  # ← NEW v7
                )
                h4_store[sym]  = df_h4
                h2_store[sym]  = df_h2
                h1_store[sym]  = df_h1
                m30_store[sym] = df_m30
                m15_store[sym] = df_m15
                m1_store[sym]  = df_1m
                m5_store[sym]  = df_5m
                m3_store[sym]  = df_3m
                d1_store[sym]  = df_1d  # ← NEW v7

                # ── Fetch Futures metrics (OI + funding rate) [v7 new] ────
                futures_data = {}
                if sym in FUTURES_SYMBOLS_MAP:
                    try:
                        futures_data = await fetch_futures_metrics(session, sym)
                        if futures_data.get("ok"):
                            futures_store[sym] = futures_data
                    except Exception as fe:
                        logger.debug("[FUTURES] %s error: %s", sym, fe)

                # ── Fetch Gold XAU/USD réel si applicable [v7] ───────────
                # Remplace les données PAXG/USDT par le Gold réel (Twelve Data ou Yahoo)
                gold_sym = GOLD_SYMBOL_MAP.get(sym)
                if gold_sym and GOLD_REAL_ENABLED:
                    try:
                        df_gold_h4 = await fetch_gold_candles(session, "4h", 500)
                        if not df_gold_h4.empty:
                            gold_store[sym] = df_gold_h4
                            # Override df_h4 pour PAXG → utilise Gold réel
                            df_h4 = df_gold_h4
                            h4_store[sym] = df_h4
                            logger.debug("[GOLD] %s → Gold réel XAU/USD injecté en H4 (%s bougies)", sym, len(df_gold_h4))
                    except Exception as ge:
                        logger.debug("[GOLD] %s error: %s", sym, ge)

                # Delta Volume state courante [v7]
                delta_state = _delta_vol.get(sym)

                # Params STRICT pour le signal 5m
                strict_state = _strict_store.get(sym) or {}
                strict_params = strict_state.get("params") if isinstance(strict_state, dict) else None

                # Poids adaptatifs (Module 4)
                sym_weights = scoring_weights.get(sym, {c: 1.0 for c in SCORE_CRITERIA})

                # ── Score MTF complet /7 ──────────────────────────────────
                # MODULE 19 : overrides RSI par symbole pour PAXG/Gold
                _rsi_long  = get_sym_override(sym, "rsi_long",  RSI_ENTRY_LONG)
                _rsi_short = get_sym_override(sym, "rsi_short", RSI_ENTRY_SHORT)

                score, side, confs, algo_context = score_setup(
                    df_h4, df_1m, df30,
                    df_h2=df_h2, df_h1=df_h1,
                    df_m30=df_m30, df_m15=df_m15,
                    df_m5=df_5m,
                    strict_params=strict_params,
                    scoring_w=sym_weights,
                    rsi_long_override=_rsi_long,
                    rsi_short_override=_rsi_short,
                    # v7 new
                    df_1d=df_1d,
                    delta_vol_state=delta_state,
                    futures_data=futures_data,
                    # v8 : TF actif pour adapter lookbacks et TF de confirmation
                    active_tf=ACTIVE_TF,
                )

                fvgs = detect_fvg(df30)
                obs = detect_ob(df30, side)
                # entry : clôture 3m via compute_atr_levels (logique OB/FVG inchangée)
                entry, _sl_base, _tps_base = compute_atr_levels(df30, side, df_m3=df_3m, fvgs=fvgs, obs=obs)

                # ── SANITY CHECK prix d'entrée ────────────────────────────
                # Protège contre une corruption de cache (ex: données BTC dans cache PAXG)
                # Si l'entry est > 2× ou < 0.5× le close actuel → fallback au close 30s
                _current_close = float(df30["close"].iloc[-1]) if df30 is not None and not df30.empty else 0.0
                if _current_close > 0 and entry > 0:
                    _ratio = entry / _current_close
                    if _ratio > 2.0 or _ratio < 0.5:
                        logger.warning(
                            "[SCAN] %s entrée incohérente (entry=%.2f vs close=%.2f ratio=%.2f) "
                            "→ purge cache 3m + fallback close",
                            sym, entry, _current_close, _ratio
                        )
                        # Purger le cache 3m corrompu pour ce symbole
                        _m3_cache.pop(sym, None)
                        df_3m = None
                        entry = _current_close
                        sl    = _sl_base if _sl_base and abs(_sl_base - entry) / entry < 0.1 else 0.0
                        tps   = list(_tps_base) if _tps_base else []

                # ── MODULE 19 : filtre score minimum par symbole ──────────
                # Pour PAXG/Gold : score ≥ 5/7 requis.
                # IMPORTANT : on ne bloque PAS le traitement complet (le signal
                # doit quand même être mis à jour dans signals[sym] pour que
                # l'UI affiche les niveaux courants). On inhibe uniquement
                # l'émission WS et l'enregistrement dans l'historique.
                _score_min  = get_sym_override(sym, "score_min", 0)
                _below_min  = (_score_min > 0 and score < _score_min)

                # ── PATCH ADAPTATIF SL/TP ─────────────────────────────────
                # Calcul ATR sur 3m si disponible, sinon fallback 30s
                _atr_src = df_3m if (df_3m is not None and len(df_3m) >= 14) else df30
                _atr_series = ta.atr(_atr_src["high"], _atr_src["low"], _atr_src["close"], 14)
                _atr_val = (
                    float(_atr_series.iloc[-1])
                    if _atr_series is not None and not _atr_series.empty and not pd.isna(_atr_series.iloc[-1])
                    else abs(entry) * 0.002
                )
                # Mapping side → 'long' / 'short'
                _side_str = "long" if "ACHAT" in side else "short"
                # lookback_df : 50 dernières bougies 3m (ou 30s) pour le clamp ATR
                _lookback_df = (df_3m.tail(50) if df_3m is not None and len(df_3m) >= 14 else df30.tail(50)).copy()
                # Ajoute colonne 'atr' au lookback si absente (pour le clamping percentile)
                if "atr" not in _lookback_df.columns:
                    _atr_lk = ta.atr(_lookback_df["high"], _lookback_df["low"], _lookback_df["close"], 14)
                    if _atr_lk is not None and not _atr_lk.empty:
                        _lookback_df["atr"] = _atr_lk

                # ── MODULE 19 : overrides par symbole (PAXG Gold calibré) ──
                _atr_mult  = get_sym_override(sym, "atr_mult",  1.0)
                _tp_ratios = get_sym_override(sym, "tp_ratios", (1.2, 1.8, 2.4))

                sl, tps = compute_adaptive_levels(
                    entry_price=entry,
                    atr=_atr_val,
                    side=_side_str,
                    lookback_df=_lookback_df,
                    atr_mult=_atr_mult,
                    tp_ratios=_tp_ratios,
                )

                # Floor SL absolu (sécurité pour l'or — évite les SL micro)
                _sl_floor_pct = get_sym_override(sym, "sl_floor_pct", 0.0)
                if _sl_floor_pct > 0 and entry > 0:
                    _sl_min_dist = abs(entry) * _sl_floor_pct
                    if abs(entry - sl) < _sl_min_dist:
                        sl = round(entry - _sl_min_dist if _side_str == "long" else entry + _sl_min_dist, 4)
                        # Recalculer TPs avec le SL corrigé
                        _risk = abs(entry - sl)
                        tps = [
                            round(entry + r * _risk if _side_str == "long" else entry - r * _risk, 4)
                            for r in _tp_ratios
                        ]
                # Complétion à 4 TPs pour compatibilité avec le reste du code (tp3, tp4, Telegram…)
                while len(tps) < 4:
                    _r = max(abs(entry - sl), _atr_val * 0.5)
                    _n = len(tps) + 1
                    tps.append(round(
                        entry + _r * _n if _side_str == "long" else entry - _r * _n, 4
                    ))
                # ── FIN PATCH ADAPTATIF SL/TP ─────────────────────────────
                patterns = detect_candle_patterns(df30)
                reversal = detect_reversal_candles(df30, lookback=300)
                poc = compute_volume_poc(df30)
                st = compute_supertrend(df30)

                ema200_vals = df_h4["close"].ewm(span=200, adjust=False).mean().tail(1)
                ema200_h4 = float(ema200_vals.iloc[-1]) if not ema200_vals.empty else 0.0

                ema20_series = ta.ema(df30["close"], length=20)
                ema20 = []
                if ema20_series is not None:
                    for t, v in zip(df30.index[-200:], ema20_series.values[-200:]):
                        if not pd.isna(v):
                            ema20.append({"time": int(t.timestamp()), "value": round(float(v), 6)})

                # ── STRICT TRIX (5m) — infos + bougies signal (jaune) ─────
                strict_info = None
                strict_entry_ts: List[int] = []   # timestamps des bougies d'entrée (pour surlignage jaune)
                if isinstance(df_5m, pd.DataFrame) and (not df_5m.empty) and isinstance(strict_params, dict):
                    try:
                        d5 = strict_trix_apply(df_5m.tail(2000), strict_params)
                        if not d5.empty:
                            last = d5.iloc[-1]
                            # Collecte les bougies d'entrée récentes (300 dernières)
                            d5_recent = d5.tail(300)
                            strict_entry_ts = [
                                int(ts.timestamp()) for ts, row in zip(d5_recent.index, d5_recent.to_dict("records"))
                                if bool(row.get("entry_long", False))
                            ]
                            strict_info = {
                                "tf": STRICT_TF,
                                "params": strict_params,
                                "zones": (strict_state.get("zones") if isinstance(strict_state, dict) else []),
                                "params_alt": (strict_state.get("params_alt") if isinstance(strict_state, dict) else None),
                                "sharpe": float(strict_state.get("sharpe", 0.0) or 0.0),
                                "trades": int((strict_state.get("metrics") or {}).get("trades", 0) or 0),
                                "hist": float(last.get("trix_hist", 0.0) or 0.0),
                                "in_trend": bool((last.get("close", 0.0) or 0.0) > (last.get("trend_ma", 1e18) or 1e18)),
                                "entry_long": bool(last.get("entry_long", False)),
                                "exit_long": bool(last.get("exit_long", False)),
                                "trained_until": strict_state.get("trained_until"),
                                "next_recalib_ts": float(strict_state.get("next_recalib_ts", 0.0) or 0.0),
                            }
                    except Exception:
                        strict_info = {"tf": STRICT_TF, "error": "strict_compute_failed"}

                # Contexte OB enrichi (statuts, double confirmation)
                ob_context = {
                    "ob_status_30m":  algo_context.get("ob_status_30m", "none"),
                    "ob_quality_30m": algo_context.get("ob_quality_30m", 0.0),
                    "ob_fvg_15m_ok":  algo_context.get("ob_fvg_15m_ok", False),
                    "ob_quality_15m": algo_context.get("ob_quality_15m", 0.0),
                    "range_30m":      algo_context.get("range_30m", False),
                    "align_h2_h1":    algo_context.get("align_h2_h1", False),
                }

                sig = {
                    "symbol": sym,
                    "strict": strict_info,
                    "strict_entry_ts": strict_entry_ts,
                    "reversal_ts": [int(x.get('time')) for x in (reversal or []) if isinstance(x, dict) and x.get('time') is not None],
                    "reversal_patterns": reversal or [],
                    "score": score,
                    "score_max": 9,           # ← v7: score /9
                    "side": side,
                    "confs": confs,
                    "entry": entry,
                    "sl": sl,
                    "tps": tps,
                    "ema200_h4": ema200_h4,
                    "ema20": ema20,
                    "fvg": fvgs,
                    "ob": obs,
                    "ob_context": ob_context,
                    "candle_patterns": patterns,
                    "volume_poc": poc,
                    "supertrend": st,
                    "tp1": (tps[0] if isinstance(tps, list) and len(tps) > 0 else None),
                    "tp2": (tps[1] if isinstance(tps, list) and len(tps) > 1 else None),
                    "tp3": (tps[2] if isinstance(tps, list) and len(tps) > 2 else None),
                    "tp4": (tps[3] if isinstance(tps, list) and len(tps) > 3 else None),
                    "be": entry,
                    "resistance_zones": (obs[:5] if isinstance(obs, list) else obs),
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "candles": [
                        {"time": int(t.timestamp()),
                         "open":  round(float(r["open"]),  6),
                         "high":  round(float(r["high"]),  6),
                         "low":   round(float(r["low"]),   6),
                         "close": round(float(r["close"]), 6)}
                        for t, r in zip(df30.index[-300:], df30.tail(300).to_dict("records"))
                    ],
                    "struct_h2": algo_context.get("struct_h2", "—"),
                    "struct_h1": algo_context.get("struct_h1", "—"),
                    "rsi_5m":    algo_context.get("rsi_5m"),
                    # ── MODULE 18 : meilleure config annuelle pour ce symbole ──
                    "best_config_2025": _best_results_2025.get(sym, {}) or {},
                    # ── v7 new fields ─────────────────────────────────────────
                    "struct_1d":         algo_context.get("struct_1d",         "—"),
                    "ema200_1d":         algo_context.get("ema200_1d"),
                    "d1_bias_ok":        algo_context.get("d1_bias_ok",         False),
                    "delta_vol": {
                        "delta_pct":  float((delta_state or {}).get("delta_pct", 0.5)),
                        "buy_vol":    float((delta_state or {}).get("buy_vol",   0.0)),
                        "sell_vol":   float((delta_state or {}).get("sell_vol",  0.0)),
                        "bullish":    bool((delta_state or {}).get("bullish",    False)),
                        "bearish":    bool((delta_state or {}).get("bearish",    False)),
                        "signal":     algo_context.get("delta_vol_signal", "NEUTRAL"),
                    } if delta_state else None,
                    "futures": algo_context.get("futures"),
                    # ── v8 : TF actif pour le dashboard ───────────────────────
                    "active_tf": ACTIVE_TF,
                }

                signals[sym] = sig

                # MODULE 19 : n'émettre le signal WS que si score ≥ score_min
                # Si _below_min : mettre à jour uniquement le score/side/confs
                # mais CONSERVER les anciens niveaux SL/TP du signal précédent
                if _below_min:
                    prev = signals.get(sym) or {}
                    if prev and prev.get("entry") and prev.get("sl"):
                        sig["entry"] = prev["entry"]
                        sig["sl"]    = prev["sl"]
                        sig["tps"]   = prev.get("tps", tps)
                        sig["tp1"]   = prev.get("tp1")
                        sig["tp2"]   = prev.get("tp2")
                        sig["tp3"]   = prev.get("tp3")
                        sig["tp4"]   = prev.get("tp4")
                        signals[sym] = sig

                # MODULE 19 : n'émettre le signal WS que si score ≥ score_min
                # (signals[sym] est toujours mis à jour pour que l'UI reste fraîche)
                if not _below_min:
                    await broadcast(sym, {"type": "signal", "symbol": sym, "data": sig})
                    logger.info(
                        "[SIGNAL] %s score=%s/7 %s entry=%.4f sl=%.4f tp1=%.4f",
                        sym, score, side, entry,
                        sl if isinstance(sl, float) else 0.0,
                        tps[0] if tps else 0.0,
                    )
                else:
                    # Signal en dessous du seuil PAXG — log discret, pas d'émission
                    logger.debug("[MOD19] %s score %s/%s < min %s — signal non émis", sym, score, 7, _score_min)

                # ── Module 4 : enregistrement signal en historique ─────────
                try:
                    if score >= 4 and not _below_min:
                        add_signal_to_history(sym, score, side, confs, entry, sl, tps)
                    # Mise à jour des outcomes des signaux passés
                    current_price = float(df30["close"].iloc[-1]) if df30 is not None and not df30.empty else entry
                    record_signal_outcome(sym, entry, sl,
                                         tps[0] if tps else entry,
                                         current_price,
                                         sig["ts"], confs, score, side)
                except Exception:
                    pass

                # ── Module 12 : Alertes Telegram multi-niveaux /9 ─────────
                try:
                    score_int = int(score)
                    score_max_tg = int(sig.get("score_max", 9))
                    if (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS
                            and score_int >= int(TELEGRAM_SCORE_THRESHOLD)
                            and str(side).lower() not in ("neutral", "flat", "none", "")):

                        quality_label = TELEGRAM_SCORE_LABELS.get(score_int, "📊 Signal")
                        is_long = "ACHAT" in side
                        direction_emoji = "🟢" if is_long else "🔴"
                        side_clean = "LONG ▲" if is_long else "SHORT ▼"

                        # RR ratios
                        risk = abs(float(entry) - float(sl)) if entry and sl else 0
                        rr_lines = ""
                        if risk > 0 and isinstance(tps, list):
                            for idx, tp in enumerate(tps[:4], 1):
                                if tp:
                                    rr = abs(float(tp) - float(entry)) / risk
                                    rr_lines += f"TP{idx}: <b>{float(tp):.4f}</b>  <i>({rr:.1f}R)</i>\n"

                        # Confluences groupées
                        confs_display = "\n".join(f"  ✓ {c}" for c in confs) if confs else "  —"

                        # Contexte v7 (1D + Delta Vol)
                        v7_ctx = ""
                        if algo_context.get("d1_bias_ok"):
                            v7_ctx += "\n📆 <b>Biais 1D aligné</b>"
                        dv = sig.get("delta_vol") or {}
                        if dv.get("signal") and dv["signal"] != "NEUTRAL":
                            pct = int((dv.get("delta_pct", 0.5)) * 100)
                            v7_ctx += f"\n📊 Delta Vol: <b>{dv['signal']}</b> ({pct}%)"

                        # Double confirmation FVG⊂OB
                        fvg_ob_txt = ""
                        if ob_context.get("ob_quality_30m", 0) >= 1.0 or ob_context.get("ob_fvg_15m_ok"):
                            fvg_ob_txt = "\n✅ <b>Double confirmation FVG⊂OB</b>  TP ×1.1"

                        # Range warning
                        range_txt = "\n⚠️ <i>Marché en range 30m — prudence</i>" if ob_context.get("range_30m") else ""

                        # STRICT Sharpe
                        strict_txt = ""
                        if isinstance(strict_info, dict) and strict_info.get("sharpe"):
                            strict_txt = "\nTRIX Sharpe: <b>{:.2f}</b>".format(
                                float(strict_info.get("sharpe", 0.0) or 0.0))

                        msg = (
                            f"{quality_label}\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"{direction_emoji} <b>{sym}</b>  —  Score <b>{score_int}/{score_max_tg}</b>\n"
                            f"Direction: <b>{side_clean}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"📌 Entrée:  <b>{float(entry):.4f}</b>\n"
                            f"🛑 SL:     <b>{float(sl):.4f}</b>  <i>(R={risk:.4f})</i>\n"
                            f"{rr_lines}"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"Confluences:\n{confs_display}"
                            f"{fvg_ob_txt}{range_txt}{v7_ctx}{strict_txt}\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"<i>⏰ {datetime.now(timezone.utc).strftime('%H:%M UTC')} — Vérifiez avant exécution.</i>"
                        )

                        await telegram_send(session, msg, symbol=sym)
                except Exception:
                    pass

            except Exception as e:
                logger.error("Scan %s ERREUR: %s\n%s", sym, e, traceback.format_exc())

        await asyncio.sleep(SCAN_INTERVAL)


# ---------------------------------------------------------------------------
# Module 14: broadcast (WS texte pur OU gzip+base64 + métriques)
# ---------------------------------------------------------------------------

def _ws_pack(payload: dict) -> str:
    """Retourne le message à envoyer via WebSocket (toujours en TEXT).

    - WS_COMPRESS=off: JSON texte brut.
    - WS_COMPRESS=gzip_base64: enveloppe compressée si taille >= seuil ET type autorisé.

    Enveloppe (frames texte, compatible navigateur):
      {"_compressed":"gzip+base64","payload_b64":"...","raw_bytes":...,"gz_bytes":...}
    """
    raw = json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8")

    if WS_COMPRESS != "gzip_base64":
        return raw.decode("utf-8")

    ptype = str(payload.get("type", "")).strip().lower()
    if WS_COMPRESS_TYPES and ptype not in WS_COMPRESS_TYPES:
        return raw.decode("utf-8")

    if len(raw) < WS_COMPRESS_MIN_BYTES:
        return raw.decode("utf-8")

    gz = gzip.compress(raw, compresslevel=6)
    b64 = base64.b64encode(gz).decode("ascii")

    envelope = {
        "_compressed": "gzip+base64",
        "content_type": "application/json; charset=utf-8",
        "raw_bytes": len(raw),
        "gz_bytes": len(gz),
        "payload_b64": b64,
    }
    return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))


def _client_counts() -> Dict[str, Any]:
    by_sym = {s: len(ws_clients.get(s, set())) for s in SYMBOLS}
    return {"total": int(sum(by_sym.values())), "by_symbol": by_sym}

async def broadcast(sym: str, payload: dict):
    """Broadcast WS.

    - WS_COMPRESS=off          -> JSON texte brut
    - WS_COMPRESS=gzip_base64  -> enveloppe compressée (selon seuil + types)

    Note perf: par défaut, on compresse seulement les messages type='signal'.
    """
    global _ws_sent_msgs, _ws_sent_bytes
    clients = ws_clients.get(sym, set())
    if not clients:
        return

    msg = _ws_pack(payload)
    out_bytes = len(msg.encode("utf-8"))

    dead = set()
    for ws in list(clients):
        try:
            await ws.send_text(msg)
            _ws_sent_msgs += 1
            _ws_sent_bytes += out_bytes
        except Exception:
            dead.add(ws)

    if dead:
        ws_clients[sym] -= dead

    _ws_last_broadcast_ts[sym] = datetime.now(timezone.utc).timestamp()


# ---------------------------------------------------------------------------
# MODULE 20 — TITANIUM ORDER BOOK ENGINE [v8]
# Carnet d'ordres Niveau 2 en temps réel (intégré — sans port séparé)
# ---------------------------------------------------------------------------

@dataclass
class MarketSignalOB:
    timestamp: float
    symbol: str
    signal_type: str
    price: float
    value: float
    description: str
    strength: float
    smc_context: str = ""


@dataclass
class OrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update: float = 0.0
    imbalance: float = 0.0
    total_bid_vol: float = 0.0
    total_ask_vol: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0


class TitaniumOrderBookEngine:
    """
    Moteur de carnet d'ordres Niveau 2 — intégré dans le backend v8.
    - Connexion WebSocket Binance Futures pour BTC, ETH, SOL
    - Simulation hybride réaliste pour le Gold (PAXG/USDT → XAUUSD)
    - Détection de signaux SMC (Murs, Imbalance, Absorption)
    - Broadcast via le WS existant FastAPI /ws/{symbol} (type: BOOK_UPDATE)
    """

    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.books: Dict[str, OrderBookState] = {s: OrderBookState() for s in symbols}
        self.history: Dict[str, _deque] = {s: _deque(maxlen=OB_HISTORY_MAXLEN) for s in symbols}
        self.signals: _deque = _deque(maxlen=OB_SIGNALS_MAXLEN)
        self.running = False

    async def start(self, session: aiohttp.ClientSession):
        """Démarre toutes les tâches du moteur order book."""
        self.running = True
        tasks = []
        for symbol in self.symbols:
            config = OB_SYMBOL_MAP.get(symbol)
            if not config:
                logger.warning("[OB] Symbole %s non configuré dans OB_SYMBOL_MAP", symbol)
                continue
            if config["exchange"] == "binance":
                tasks.append(asyncio.create_task(
                    self._connect_binance(session, symbol, config["stream"])
                ))
            elif config["exchange"] == "hybrid":
                tasks.append(asyncio.create_task(
                    self._simulate_gold_stream(symbol)
                ))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        self.running = False
        logger.info("[OB] Moteur order book arrêté.")

    def get_book_snapshot(self, symbol: str) -> dict:
        """Retourne un snapshot JSON-sérialisable du carnet pour /api/orderbook/{symbol}."""
        book = self.books.get(symbol)
        if not book:
            return {}
        return {
            "symbol": symbol,
            "bids": [[p, q] for p, q in sorted(book.bids.items(), reverse=True)],
            "asks": [[p, q] for p, q in sorted(book.asks.items())],
            "imbalance": round(book.imbalance, 4),
            "spread": round(book.best_ask - book.best_bid, 4) if book.best_ask and book.best_bid else 0.0,
            "best_bid": book.best_bid,
            "best_ask": book.best_ask,
            "total_bid_vol": round(book.total_bid_vol, 2),
            "total_ask_vol": round(book.total_ask_vol, 2),
            "last_update": book.last_update,
        }

    def get_recent_signals(self, symbol: Optional[str] = None, limit: int = 20) -> List[dict]:
        """Retourne les signaux SMC récents, filtrés par symbole si précisé."""
        sigs = list(self.signals)
        if symbol:
            sigs = [s for s in sigs if s.symbol == symbol]
        return [asdict(s) for s in sigs[-limit:]]

    async def _connect_binance(self, session: aiohttp.ClientSession, symbol: str, stream_name: str):
        """Connexion WebSocket Binance Futures avec reconnexion exponentielle."""
        url = f"{OB_BINANCE_WS_URL}/{stream_name}"
        logger.info("[OB][%s] Connexion Binance Futures WS: %s", symbol, url)
        reconnect_delay = 5
        while self.running:
            try:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    logger.info("[OB][%s] Connecté à Binance Futures", symbol)
                    reconnect_delay = 5
                    async for msg in ws:
                        if not self.running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            await self._process_update(symbol, data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            logger.warning("[OB][%s] WS fermé/erreur, reconnexion...", symbol)
                            break
            except Exception as e:
                logger.error("[OB][%s] Exception: %s — retry dans %ds", symbol, e, reconnect_delay)
                if self.running:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)

    async def _simulate_gold_stream(self, symbol: str):
        """Simulation réaliste du carnet PAXG/USDT (proxy Gold XAU/USD)."""
        logger.info("[OB][%s] Démarrage flux hybride Gold (simulation)", symbol)
        base_price = 2350.00
        while self.running:
            noise = np.random.normal(0, 0.4)
            trend = np.sin(time.time() / 30) * 0.3
            base_price += noise + trend

            bids: Dict[float, float] = {}
            asks: Dict[float, float] = {}
            for i in range(1, 21):
                bid_p = round(base_price - i * 0.1, 1)
                ask_p = round(base_price + i * 0.1, 1)
                base_vol = float(np.random.exponential(30))
                bids[bid_p] = round(base_vol * (1 - i / 40), 2)
                asks[ask_p] = round(base_vol * (1 - i / 40), 2)

            # Murs aléatoires (5% de chance)
            if np.random.random() > 0.95:
                wp = round(base_price - float(np.random.choice([1.0, 1.5, 2.0])), 1)
                bids[wp] = round(np.random.uniform(300, 800), 2)
            if np.random.random() > 0.95:
                wp = round(base_price + float(np.random.choice([1.0, 1.5, 2.0])), 1)
                asks[wp] = round(np.random.uniform(300, 800), 2)

            fake_data = {
                "bids": [[p, q] for p, q in sorted(bids.items(), reverse=True)],
                "asks": [[p, q] for p, q in sorted(asks.items())],
            }
            await self._process_update(symbol, fake_data)
            await asyncio.sleep(0.2)

    async def _process_update(self, symbol: str, data: dict):
        """Traite une mise à jour du carnet et broadcast via le WS FastAPI."""
        if "bids" not in data or "asks" not in data:
            return

        book = self.books[symbol]
        ts = time.time()

        new_bids = {float(p): float(q) for p, q in (data["bids"] or [])[:20]}
        new_asks = {float(p): float(q) for p, q in (data["asks"] or [])[:20]}
        if not new_bids or not new_asks:
            return

        book.total_bid_vol = sum(new_bids.values())
        book.total_ask_vol = sum(new_asks.values())
        total_vol = book.total_bid_vol + book.total_ask_vol
        book.imbalance = book.total_bid_vol / total_vol if total_vol > 0 else 0.5
        book.best_bid = max(new_bids.keys())
        book.best_ask = min(new_asks.keys())
        book.bids = new_bids
        book.asks = new_asks
        book.last_update = ts

        snapshot = {
            "time": ts,
            "price": (book.best_bid + book.best_ask) / 2,
            "imbalance": book.imbalance,
            "bid_vol": book.total_bid_vol,
            "ask_vol": book.total_ask_vol,
        }
        self.history[symbol].append(snapshot)

        ob_signals = await self._analyze_smc(symbol, snapshot)
        for sig in ob_signals:
            self.signals.appendleft(sig)

        # Broadcast via le mécanisme WS existant du dashboard
        payload = {
            "type": "BOOK_UPDATE",
            "symbol": symbol,
            "data": {
                "bids": [[p, q] for p, q in sorted(book.bids.items(), reverse=True)],
                "asks": [[p, q] for p, q in sorted(book.asks.items())],
                "imbalance": book.imbalance,
                "spread": round(book.best_ask - book.best_bid, 4),
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
                "total_bid_vol": round(book.total_bid_vol, 2),
                "total_ask_vol": round(book.total_ask_vol, 2),
                "last_update": ts,
            },
            "signals": [asdict(s) for s in ob_signals],
        }
        # Utilise le broadcast FastAPI existant (clé = rest_sym du symbole)
        await broadcast(rest_sym(symbol), payload)

    async def _analyze_smc(self, symbol: str, current: dict) -> List[MarketSignalOB]:
        """Analyse SMC : détection Murs, Imbalance extrême, Absorption."""
        signals: List[MarketSignalOB] = []
        history = list(self.history[symbol])
        if len(history) < 10:
            return signals

        book = self.books[symbol]
        ts = current["time"]

        # 1. Détection MURS (Wall)
        if book.bids:
            max_bid_qty = max(book.bids.values())
            if max_bid_qty > book.total_bid_vol * OB_WALL_THRESHOLD:
                price = next(p for p, q in book.bids.items() if q == max_bid_qty)
                signals.append(MarketSignalOB(
                    timestamp=ts, symbol=symbol, signal_type="WALL_BUY",
                    price=price, value=max_bid_qty,
                    description=f"Mur d'achat massif détecté à {price}",
                    strength=min(1.0, max_bid_qty / (book.total_bid_vol * 0.1)),
                    smc_context="Support Institutionnel Potentiel",
                ))

        if book.asks:
            max_ask_qty = max(book.asks.values())
            if max_ask_qty > book.total_ask_vol * OB_WALL_THRESHOLD:
                price = next(p for p, q in book.asks.items() if q == max_ask_qty)
                signals.append(MarketSignalOB(
                    timestamp=ts, symbol=symbol, signal_type="WALL_SELL",
                    price=price, value=max_ask_qty,
                    description=f"Mur de vente massif détecté à {price}",
                    strength=min(1.0, max_ask_qty / (book.total_ask_vol * 0.1)),
                    smc_context="Résistance Institutionnelle Potentielle",
                ))

        # 2. Imbalance Extrême
        imb = current["imbalance"]
        if imb > OB_IMBALANCE_THRESH:
            signals.append(MarketSignalOB(
                timestamp=ts, symbol=symbol, signal_type="IMBALANCE_BUY",
                price=book.best_bid, value=imb,
                description=f"Pression acheteuse extrême ({imb:.1%})",
                strength=(imb - 0.5) * 2,
                smc_context="Dislocation du prix imminente",
            ))
        elif imb < (1 - OB_IMBALANCE_THRESH):
            signals.append(MarketSignalOB(
                timestamp=ts, symbol=symbol, signal_type="IMBALANCE_SELL",
                price=book.best_ask, value=1 - imb,
                description=f"Pression vendeuse extrême ({1-imb:.1%})",
                strength=((1 - imb) - 0.5) * 2,
                smc_context="Chute de prix imminente",
            ))

        # 3. Absorption
        recent = history[-OB_ABSORPTION_WINDOW:]
        if len(recent) >= OB_ABSORPTION_WINDOW:
            p_start = recent[0]["price"]
            p_end   = recent[-1]["price"]
            vol_sold   = sum(h["ask_vol"] for h in recent)
            vol_bought = sum(h["bid_vol"] for h in recent)

            if vol_sold > book.total_ask_vol * 3 and p_end >= p_start * 0.9995:
                signals.append(MarketSignalOB(
                    timestamp=ts, symbol=symbol, signal_type="ABSORPTION_BUY",
                    price=book.best_bid, value=vol_sold,
                    description="Absorption : vente massive ignorée par le marché",
                    strength=0.85, smc_context="Accumulation Smart Money",
                ))
            if vol_bought > book.total_bid_vol * 3 and p_end <= p_start * 1.0005:
                signals.append(MarketSignalOB(
                    timestamp=ts, symbol=symbol, signal_type="ABSORPTION_SELL",
                    price=book.best_ask, value=vol_bought,
                    description="Absorption : achat massif absorbé sans hausse",
                    strength=0.85, smc_context="Distribution Smart Money",
                ))

        return signals


# Singleton global du moteur order book (initialisé dans le lifespan)
_ob_engine: Optional[TitaniumOrderBookEngine] = None


# ---------------------------------------------------------------------------
# FastAPI app + lifespan (Module 16)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ob_engine
    # Chargement de l'état d'apprentissage (Module 4)
    _load_learning_state()
    logger.info("[LEARNING] État chargé — %s symboles", len(SYMBOLS))

    # [OPT-3] v7: Session HTTP optimisée avec pooling de connexions
    session = await create_optimized_session()
    app.state.http = session

    tasks = [
        asyncio.create_task(ws_binance(session)),
        asyncio.create_task(scan_loop(session)),
        asyncio.create_task(learning_report_loop(session)),   # ← Module 4
        asyncio.create_task(optimisation_loop()),             # ← Module 18
    ]

    # [v8] MODULE 20 — Démarrage du moteur Order Book
    if OB_ENABLED:
        _ob_engine = TitaniumOrderBookEngine(SYMBOLS)
        app.state.ob_engine = _ob_engine
        tasks.append(asyncio.create_task(_ob_engine.start(session)))
        logger.info("[OB] TitaniumOrderBookEngine démarré — %d symboles", len(SYMBOLS))
    else:
        app.state.ob_engine = None
        logger.info("[OB] Order Book Engine désactivé (OB_ENABLED=0)")

    if STRICT_OFFLOAD_PROCESS:
        # Windows safe: spawn
        ctx = multiprocessing.get_context("spawn")
        job_q = ctx.Queue(maxsize=STRICT_JOB_QUEUE_MAX)
        res_q = ctx.Queue(maxsize=STRICT_JOB_QUEUE_MAX)
        app.state.strict_job_q = job_q
        app.state.strict_res_q = res_q
        app.state.strict_workers = []

        for _ in range(max(1, int(STRICT_WORKERS))):
            pr = ctx.Process(target=strict_worker_main, args=(job_q, res_q), daemon=True)
            pr.start()
            app.state.strict_workers.append(pr)

        tasks.append(asyncio.create_task(strict_scheduler_loop(job_q)))
        tasks.append(asyncio.create_task(strict_result_listener(res_q)))
        logger.info("[STRICT] mode-pro ON | workers=%s", int(STRICT_WORKERS))
    else:
        tasks.append(asyncio.create_task(strict_recalib_loop(session)))
        logger.info("[STRICT] mode-pro OFF (to_thread)")
    app.state.tasks = tasks

    logger.info("Dashboard démarré → http://localhost:8080")
    logger.info("Vision Ollama → %s | primary=%s | fallback=%s", OLLAMA_CHAT_URL, VISION_MODEL_PRIMARY, VISION_MODEL_FALLBACK)
    logger.info("Bridge /api/push enabled: %s", ENABLE_BRIDGE)
    logger.info("WS_COMPRESS=%s | WS_COMPRESS_MIN_BYTES=%s | WS_COMPRESS_TYPES=%s", WS_COMPRESS, WS_COMPRESS_MIN_BYTES, sorted(WS_COMPRESS_TYPES) if WS_COMPRESS_TYPES else [])
    logger.info("STRICT tf=%s | recalib_days=%s | in_sample_days=%s | iters=%s", STRICT_TF, STRICT_RECALIB_DAYS, STRICT_IN_SAMPLE_DAYS, STRICT_RANDOM_ITERS)

    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # stop STRICT workers (mode pro)
        try:
            for pr in getattr(app.state, "strict_workers", []) or []:
                try:
                    pr.terminate()
                except Exception:
                    pass
        except Exception:
            pass

        await session.close()

app = FastAPI(title="Titanium Dashboard Autonome (WS texte pur)", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1500)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).with_name("titanium_dashboard.html")
    if not html_path.exists():
        return "<h1>titanium_dashboard.html introuvable</h1>"
    return html_path.read_text(encoding="utf-8")

@app.get("/api/state")
async def get_state():
    return {
        "symbols": SYMBOLS,
        "signals": signals,
        "clients": _client_counts(),
        "vision_cache": _vision_cache.stats(),
        "strict": _strict_store,
        "strict_heatmaps_ready": sorted(list(_strict_heatmaps.keys())),
        "learning": {
            "weights": scoring_weights,
            "pending_confirm": {s: bool(_learning_pending_confirm.get(s)) for s in SYMBOLS},
        },
        # MODULE 18 — Optimisation annuelle
        "optim_2025": {
            "running":      _opt_running,
            "last_run_ts":  _opt_last_run_ts,
            "year":         OPT_YEAR,
            "criteria":     OPT_SCORE_CRITERIA,
            "configs_count": len(OPT_CONFIGURATIONS),
            "results":      {
                sym: {k: v for k, v in res.items() if k != "all_results"}
                for sym, res in _best_results_2025.items()
            },
        },
        # v8 — TF actif
        "active_tf": ACTIVE_TF,
    }


@app.post("/api/set_tf")
async def set_active_tf(req: FARequest):
    """Change le TF actif a la volee (recalibre lookbacks OB/FVG et TF confirmation).

    Body JSON : {"tf": "5m"}
    TF valides : 1m | 3m | 5m | 15m | 30m | 1h | 4h
    """
    global ACTIVE_TF
    _VALID_TF = {"1m", "3m", "5m", "15m", "30m", "1h", "4h"}
    body = {}
    try:
        body = await req.json()
    except Exception:
        pass
    tf = str(body.get("tf", "")).strip().lower()
    if tf not in _VALID_TF:
        raise HTTPException(status_code=400, detail=f"TF invalide. Valeurs acceptees: {sorted(_VALID_TF)}")
    ACTIVE_TF = tf
    logger.info("[TF] TF actif change -> %s", ACTIVE_TF)
    # Diffuser le changement de TF a tous les clients connectes
    for sym in SYMBOLS:
        try:
            await broadcast(sym, {"type": "tf_change", "active_tf": ACTIVE_TF})
        except Exception:
            pass
    return {"ok": True, "active_tf": ACTIVE_TF}


# ---------------------------------------------------------------------------
# MODULE 4 — Routes apprentissage adaptatif
# ---------------------------------------------------------------------------

@app.get("/api/learning/report")
async def learning_report_all():
    """Retourne les rapports de performance pour tous les symboles."""
    return {
        sym: _learning_pending_confirm.get(sym) or _compute_learning_report(sym)
        for sym in SYMBOLS
    }

@app.get("/api/learning/report/{symbol}")
async def learning_report_symbol(symbol: str):
    """Rapport de performance pour un symbole."""
    sym = _normalize_sym(symbol)
    if sym not in SYMBOLS:
        raise HTTPException(status_code=404, detail="Symbole inconnu")
    return _learning_pending_confirm.get(sym) or _compute_learning_report(sym)

@app.post("/api/learning/confirm/{symbol}")
async def learning_confirm(symbol: str, req: FARequest):
    """Confirme ou rejette l'adaptation des poids de scoring pour un symbole.

    v6 fix :
    - Génère le rapport à la volée si absent de _learning_pending_confirm
      (évite le "no_pending_report" quand l'utilisateur clique avant le cycle 2h)
    - Remet un rapport frais après apply pour que l'UI reflète les nouveaux poids
    """
    sym = _normalize_sym(symbol)
    if sym not in SYMBOLS:
        raise HTTPException(status_code=404, detail="Symbole inconnu")

    body = {}
    try:
        body = await req.json()
    except Exception:
        pass

    apply = bool(body.get("apply", True))

    # v6 fix : générer à la volée si aucun rapport en attente
    report = _learning_pending_confirm.get(sym)
    if not report or report.get("status") not in ("ready_for_confirm",):
        report = _compute_learning_report(sym)
        _learning_pending_confirm[sym] = report

    if report.get("status") != "ready_for_confirm":
        return {
            "ok": False,
            "reason":  "not_enough_data",
            "symbol":  sym,
            "count":   report.get("count", 0),
            "min":     LEARNING_MIN_SIGNALS,
        }

    if apply:
        criteria = report.get("criteria") or {}
        for crit, stats in criteria.items():
            if isinstance(stats, dict) and stats.get("suggested_weight") is not None:
                scoring_weights[sym][crit] = float(stats["suggested_weight"])
        _save_learning_state()
        logger.info("[LEARNING] Poids confirmés pour %s : %s", sym, scoring_weights[sym])
        # v6 fix : marquer comme appliqué (pour l'UI) puis supprimer
        _learning_pending_confirm.pop(sym, None)
        return {
            "ok":          True,
            "applied":     True,
            "symbol":      sym,
            "new_weights": scoring_weights[sym],
        }
    else:
        _learning_pending_confirm.pop(sym, None)
        logger.info("[LEARNING] Adaptation rejetée pour %s", sym)
        return {"ok": True, "applied": False, "symbol": sym}

@app.get("/api/learning/weights")
async def learning_weights():
    """Retourne les poids adaptatifs actuels par symbole et critère."""
    return scoring_weights

@app.post("/api/learning/reset/{symbol}")
async def learning_reset(symbol: str):
    """Remet les poids à 1.0 pour un symbole (reset manuel)."""
    sym = _normalize_sym(symbol)
    if sym not in SYMBOLS:
        raise HTTPException(status_code=404, detail="Symbole inconnu")
    scoring_weights[sym] = {c: 1.0 for c in SCORE_CRITERIA}
    _save_learning_state()
    return {"ok": True, "symbol": sym, "weights": scoring_weights[sym]}

@app.get("/api/learning/history/{symbol}")
async def learning_history(symbol: str):
    """Retourne l'historique des signaux (100 derniers) pour un symbole."""
    sym = _normalize_sym(symbol)
    if sym not in SYMBOLS:
        raise HTTPException(status_code=404, detail="Symbole inconnu")
    return {
        "symbol": sym,
        "count": len(signal_history.get(sym, [])),
        "history": list(reversed(signal_history.get(sym, [])))[:100],
    }


def _sym_ui_to_binance(sym_ui: str) -> str:
    # "BTC_USDT" ou "BTC/USDT" -> "BTCUSDT"
    return (sym_ui or "").replace("/", "").replace("_", "").upper()


def _interval_to_binance(interval: str) -> str:
    allowed = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"}
    if interval not in allowed:
        raise ValueError("interval non supporté")
    return interval


@app.get("/api/klines/{symbol}/{interval}/{limit}")
async def api_klines(symbol: str, interval: str, limit: int, req: FARequest):
    """Klines pour le front (multi-timeframe)."""
    try:
        interval = _interval_to_binance(interval)
    except Exception:
        raise HTTPException(status_code=400, detail="interval invalide")

    limit = max(10, min(int(limit), 1000))
    sym = _sym_ui_to_binance(symbol)

    session: aiohttp.ClientSession = req.app.state.http
    url = f"{REST_BASE}/api/v3/klines"
    params = {"symbol": sym, "interval": interval, "limit": limit}

    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status != 200:
                txt = await resp.text()
                raise HTTPException(status_code=resp.status, detail=f"Binance error: {txt[:200]}")
            rows = await resp.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"fetch klines failed: {e}")

    candles = []
    closes = []
    for r in rows:
        t = int(int(r[0]) // 1000)
        o = float(r[1])
        h = float(r[2])
        low_ = float(r[3])
        c = float(r[4])
        closes.append(c)
        candles.append({"time": t, "open": o, "high": h, "low": low_, "close": c})

    ema20 = []
    if closes:
        s = pd.Series(closes)
        ema = s.ewm(span=20, adjust=False).mean().tolist()
        ema20 = [{"time": candles[i]["time"], "value": float(ema[i])} for i in range(len(candles))]

    return {"symbol": symbol, "interval": interval, "candles": candles, "ema20": ema20}



# ---------------------------------------------------------------------------
# MODULE 18 — Routes optimisation annuelle
# ---------------------------------------------------------------------------

@app.get("/api/optim/results")
async def optim_results():
    """Retourne les résultats complets d'optimisation annuelle (avec all_results).

    Inclut le top-6 des configurations testées par actif, triées par score.
    Utile pour debug et analyse frontend avancée.
    """
    return {
        "running":       _opt_running,
        "last_run_ts":   _opt_last_run_ts,
        "year":          OPT_YEAR,
        "criteria":      OPT_SCORE_CRITERIA,
        "configs_count": len(OPT_CONFIGURATIONS),
        "results":       _best_results_2025,
    }


@app.get("/api/optim/results/{symbol}")
async def optim_results_symbol(symbol: str):
    """Résultat d'optimisation pour un symbole spécifique (avec all_results complet)."""
    sym = _normalize_sym(symbol)
    if sym not in SYMBOLS:
        raise HTTPException(status_code=404, detail="Symbole inconnu")
    res = _best_results_2025.get(sym)
    if not res:
        return {
            "symbol":  sym,
            "status":  "not_computed",
            "running": _opt_running,
            "message": "Lance /api/optim/run pour calculer les résultats.",
        }
    return {"symbol": sym, **res}


@app.post("/api/optim/run")
async def optim_run(req: FARequest):
    """Déclenche un recalcul d'optimisation annuelle en background.

    Body JSON optionnel :
      {"year": 2025}  — pour forcer une année différente d'OPT_YEAR

    Retourne immédiatement ; le calcul tourne en asyncio.to_thread.
    Polling via GET /api/optim/results pour voir les résultats.
    """
    global _opt_running
    if _opt_running:
        return {"ok": False, "reason": "already_running", "running": True}

    body: dict = {}
    try:
        body = await req.json()
    except Exception:
        pass

    year = int(body.get("year", OPT_YEAR))

    async def _bg():
        await optimise_strategies_year(
            symbols        = SYMBOLS,
            configurations = OPT_CONFIGURATIONS,
            session        = req.app.state.http,
        )

    asyncio.create_task(_bg())
    return {
        "ok":            True,
        "started":       True,
        "year":          year,
        "symbols":       SYMBOLS,
        "configs_count": len(OPT_CONFIGURATIONS),
        "criteria":      OPT_SCORE_CRITERIA,
    }


@app.get("/api/health")
async def api_health():
    now = datetime.now(timezone.utc).timestamp()
    return {
        "utc": datetime.now(timezone.utc).isoformat(),
        "symbols": SYMBOLS,
        "tick_age_s": {s: (now - float(_last_tick_ts.get(s, 0.0) or 0.0)) if float(_last_tick_ts.get(s, 0.0) or 0.0) > 0 else None for s in SYMBOLS},
        "bar_age_s": {s: (now - float(_last_bar_ts.get(s, 0.0) or 0.0)) if float(_last_bar_ts.get(s, 0.0) or 0.0) > 0 else None for s in SYMBOLS},
        "has_any_signal": any(bool(signals.get(s)) for s in SYMBOLS),
        "ws_clients_total": int(sum(len(ws_clients.get(s, set())) for s in SYMBOLS)),
    }

@app.get("/api/metrics")
async def metrics():
    return {
        "clients": _client_counts(),
        "ws_sent_msgs": _ws_sent_msgs,
        "ws_sent_bytes": _ws_sent_bytes,
        "ws_last_broadcast_ts": _ws_last_broadcast_ts,
        "scan_interval_s": SCAN_INTERVAL,
        "vision_cache": _vision_cache.stats(),
    }

@app.get("/api/strict/heatmap/{symbol}")
async def strict_heatmap(symbol: str):
    """Retourne la heatmap STRICT (scores robustes) pour debug/UI.

    Attention: payload plus lourd, mais compressé par GZipMiddleware.
    """
    sym = symbol.upper().replace("_","/")
    hm = _strict_heatmaps.get(sym)
    if hm is not None:
        return hm
    if _strict_running.get(sym):
        return {"ok": False, "error": "computing", "symbol": sym}
    st = _strict_store.get(sym)
    if isinstance(st, dict) and st.get("error"):
        return {"ok": False, "error": st.get("error"), "symbol": sym}
    return {"ok": False, "error": "no_heatmap", "symbol": sym}

@app.get("/api/strict/status/{symbol}")
async def strict_status(symbol: str):
    sym = _normalize_sym(symbol)
    st0 = _strict_store.get(sym, {})
    st = dict(st0) if isinstance(st0, dict) else {}

    running = bool(_strict_running.get(sym, False))
    pending = bool(_strict_pending.get(sym, False))

    # Correctif: si running=True mais status resté 'queued', on l'affiche 'running'
    if running and st.get('status') in (None, '', 'queued'):
        st['status'] = 'running'
    if pending and st.get('status') in (None, ''):
        st['status'] = 'queued'

    # Durée (best-effort)
    now = datetime.now(timezone.utc).timestamp()
    start_ts = float(_strict_job_started_ts.get(sym, 0.0) or 0.0)
    if start_ts <= 0 and isinstance(_strict_job_start_ts.get(sym, 0.0), (int,float)):
        start_ts = float(_strict_job_start_ts.get(sym, 0.0) or 0.0)
    duration_s = max(0.0, now - start_ts) if start_ts > 0 else 0.0

    return {
        'symbol': sym,
        'running': running,
        'pending': pending,
        'job_id': _strict_job_id.get(sym, ''),
        'has_heatmap': sym in _strict_heatmaps,
        'duration_s': duration_s,
        'strict': st,
    }

@app.get("/api/strict/workers")
async def strict_workers_status(req: FARequest):
    out = {"mode_pro": bool(STRICT_OFFLOAD_PROCESS), "queue_max": int(STRICT_JOB_QUEUE_MAX), "workers": []}
    try:
        for pr in getattr(req.app.state, 'strict_workers', []) or []:
            out['workers'].append({"pid": getattr(pr,'pid',None), "alive": bool(pr.is_alive())})
    except Exception:
        pass
    try:
        out['pending_syms'] = [s for s,v in (_strict_pending or {}).items() if v]
        out['running_syms'] = [s for s,v in (_strict_running or {}).items() if v]
    except Exception:
        pass
    return out

@app.get("/api/strict/monitor")
async def strict_monitor(req: FARequest):
    """Snapshot monitoring STRICT (UI).

    Retourne: workers (pid/alive/cpu/mem) + 1 ligne par symbole (phase/durée/iters).
    CPU%/RAM nécessitent psutil; sinon renvoie null.
    """
    now = datetime.now(timezone.utc).timestamp()

    # Workers
    workers = []
    procs = []
    try:
        procs = list(getattr(req.app.state, 'strict_workers', []) or [])
    except Exception:
        procs = []

    for pr in procs:
        pid = getattr(pr, 'pid', None)
        alive = bool(pr.is_alive())
        cpu = None
        mem = None
        if pid and psutil is not None:
            try:
                p = psutil.Process(pid)
                # cpu_percent needs a previous call; we accept a short interval=0.0 snapshot
                cpu = float(p.cpu_percent(interval=0.0))
                mem = float(p.memory_info().rss / (1024*1024))
            except Exception:
                cpu = None
                mem = None
        workers.append({'pid': pid, 'alive': alive, 'cpu_pct': cpu, 'mem_mb': mem})

    # Symbols
    rows = []
    for sym in SYMBOLS:
        st0 = _strict_store.get(sym, {})
        st = dict(st0) if isinstance(st0, dict) else {}
        running = bool(_strict_running.get(sym, False))
        pending = bool(_strict_pending.get(sym, False))
        if running and st.get('status') in (None, '', 'queued'):
            st['status'] = 'running'
        if pending and st.get('status') in (None, ''):
            st['status'] = 'queued'

        start_ts = float(_strict_job_started_ts.get(sym, 0.0) or 0.0)
        if start_ts <= 0:
            start_ts = float(_strict_job_start_ts.get(sym, 0.0) or 0.0)
        duration_s = max(0.0, now - start_ts) if start_ts > 0 else 0.0

        rows.append({
            'symbol': sym,
            'running': running,
            'pending': pending,
            'has_heatmap': sym in _strict_heatmaps,
            'job_id': _strict_job_id.get(sym, ''),
            'status': st.get('status'),
            'phase': st.get('phase') or ('compute_heatmap' if running else ('queued' if pending else None)),
            'duration_s': duration_s,
            'iters_done': st.get('iters_done'),
            'iters_total': st.get('iters_total') or max(200, int(STRICT_RANDOM_ITERS)),
            'zones_count': st.get('zones_count'),
            'error': st.get('error'),
            'updated_at': st.get('updated_at') or st.get('started_at') or st.get('queued_at'),
        })

    return {
        'utc_ts': datetime.now(timezone.utc).isoformat(),
        'mode_pro': bool(STRICT_OFFLOAD_PROCESS),
        'strict_workers': int(STRICT_WORKERS),
        'iters_effective': max(200, int(STRICT_RANDOM_ITERS)),
        'workers': workers,
        'rows': rows,
    }

@app.post("/api/strict/recalibrate/{symbol}")
async def strict_recalibrate(symbol: str, req: FARequest):
    """Déclenche un recalibrage strict sur un symbole.

    - mode-pro ON: enqueue dans une queue (worker process)
    - mode-pro OFF: calcul dans une task asyncio (to_thread)
    """
    body = {}
    try:
        body = await req.json()
    except Exception:
        body = {}

    sym = _normalize_sym(symbol)
    background = bool(body.get("background", True))
    session: aiohttp.ClientSession = req.app.state.http

    if STRICT_OFFLOAD_PROCESS:
        job_q = getattr(req.app.state, "strict_job_q", None)
        if job_q is None:
            return {"ok": False, "error": "worker_not_ready", "symbol": sym}
        return await _strict_enqueue(sym, job_q)

    # fallback (ancienne logique)
    if background:
        asyncio.create_task(strict_recalibrate_symbol(session, sym))
        return {"ok": True, "started": True, "symbol": sym}

    return await strict_recalibrate_symbol(session, sym)

@app.websocket("/ws/{symbol}")
async def ws_endpoint(websocket: WebSocket, symbol: str):
    sym = symbol.upper().replace("_","/")
    await websocket.accept()
    ws_clients.setdefault(sym, set()).add(websocket)

    try:
        # push état actuel si dispo (respecte WS_COMPRESS)
        if sym in signals:
            initial = {"type": "signal", "symbol": sym, "data": signals[sym]}
            await websocket.send_text(_ws_pack(initial))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_clients.get(sym, set()).discard(websocket)
    except Exception:
        ws_clients.get(sym, set()).discard(websocket)

@app.post("/api/push")
async def receive_push(req: FARequest):
    if not ENABLE_BRIDGE:
        return {"ok": False, "disabled": True, "reason": "Bridge désactivé (mode autonome)."}
    try:
        payload = await req.json()
        ptype = payload.get("type")
        sym_raw = payload.get("symbol", "")
        sym = sym_raw.replace("_","/").upper()

        if ptype == "signal":
            data = payload.get("data", {})
            data["source"] = "external_bridge"
            signals[sym] = data
            await broadcast(sym, {"type":"signal","symbol":sym,"data":data})

        elif ptype == "candle":
            cd = payload.get("data", {})
            await broadcast(sym, {"type":"candle","symbol":sym,"data":cd})

        elif ptype == "log":
            msg = payload.get("message", "")
            for s in SYMBOLS:
                await broadcast(s, {"type":"log","symbol":s,"message":msg,"ts":payload.get("ts","")})

        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/vision/analyze")
async def vision_analyze(req: FARequest):
    try:
        body = await req.json()
        image_b64  = body.get("image", "") or ""
        symbol     = body.get("symbol", "BTC/USDT").upper()
        timeframe  = body.get("timeframe", "5m")
        price      = float(body.get("price", 0) or 0)
        side_hint  = body.get("side_hint", None)

        # image_b64 peut être vide — analyse texte seule avec contexte algo
        # On NE retourne plus d'erreur si image absente

        # Contexte algo : priorité au body (envoyé par le dashboard), sinon signals serveur
        algo_context = body.get("algo_context") or {}
        if not algo_context and symbol in signals:
            sig = signals[symbol]
            algo_context = {
                "score":         sig.get("score"),
                "score_max":     int(sig.get("score_max", 9)),   # FIX: /9 pas /7
                "side":          sig.get("side"),
                "confs":         sig.get("confs"),
                "entry":         sig.get("entry"),
                "sl":            sig.get("sl"),
                "tps":           sig.get("tps"),
                "supertrend":    sig.get("supertrend"),
                "volume_poc":    sig.get("volume_poc"),
                "patterns":      sig.get("candle_patterns"),
                "struct_h2":     sig.get("struct_h2"),
                "struct_h1":     sig.get("struct_h1"),
                "fvg":           sig.get("fvg"),
                "ob":            sig.get("ob"),
                "ob_status_30m": (sig.get("ob_context") or {}).get("ob_status_30m"),
                "ob_quality_30m":(sig.get("ob_context") or {}).get("ob_quality_30m"),
                "ob_fvg_15m_ok": (sig.get("ob_context") or {}).get("ob_fvg_15m_ok"),
                "range_30m":     (sig.get("ob_context") or {}).get("range_30m"),
                "rsi_5m":        sig.get("rsi_5m"),
                # v7 fields
                "d1_bias_ok":    sig.get("d1_bias_ok"),
                "struct_1d":     sig.get("struct_1d"),
            }
        elif algo_context and not algo_context.get("score_max"):
            # Fix: forcer score_max=9 si absent du contexte fourni par le dashboard
            algo_context["score_max"] = 9

        session: aiohttp.ClientSession = req.app.state.http
        result = await ollama_vision_analyze(
            session, image_b64, symbol, timeframe, price, side_hint,
            algo_context=algo_context
        )

        await broadcast(symbol, {"type":"vision_result","symbol":symbol,"data":result})
        return result

    except Exception as e:
        logger.warning("Vision error: %s", e)
        return {"error": str(e)}

@app.get("/api/vision/health")
async def vision_health():
    return {
        "ok": True,
        "ollama_chat_url": OLLAMA_CHAT_URL,
        "primary": VISION_MODEL_PRIMARY,
        "fallback": VISION_MODEL_FALLBACK,
        "num_ctx": VISION_NUM_CTX,
        "keep_alive": VISION_KEEP_ALIVE,
        "cache": _vision_cache.stats(),
    }


# ---------------------------------------------------------------------------
# v7 — Routes Futures / Delta Vol / OANDA
# ---------------------------------------------------------------------------

@app.get("/api/futures")
async def get_futures():
    """Retourne les métriques Futures (OI, funding, mark price) pour tous les symboles."""
    return {
        "enabled": FUTURES_ENABLED,
        "symbols": list(FUTURES_SYMBOLS_MAP.keys()),
        "data": futures_store,
        "cache_ttl_s": FUTURES_CACHE_TTL,
    }


@app.get("/api/futures/{symbol}")
async def get_futures_symbol(symbol: str, req: FARequest):
    """Force le rafraîchissement des métriques Futures pour un symbole."""
    sym = _normalize_sym(symbol)
    if sym not in FUTURES_SYMBOLS_MAP:
        raise HTTPException(404, detail=f"{sym} n'est pas un symbole Futures configuré")
    session: aiohttp.ClientSession = req.app.state.http
    # Invalide le cache pour forcer un refresh
    _futures_cache.pop(sym, None)
    result = await fetch_futures_metrics(session, sym)
    if result.get("ok"):
        futures_store[sym] = result
    return result


@app.get("/api/delta_vol")
async def get_delta_vol():
    """Retourne l'état du delta volume rolling pour tous les symboles."""
    out = {}
    for sym in SYMBOLS:
        dv = _delta_vol.get(sym, {})
        out[sym] = {
            "buy_vol":   round(float(dv.get("buy_vol",   0.0)), 4),
            "sell_vol":  round(float(dv.get("sell_vol",  0.0)), 4),
            "delta":     round(float(dv.get("delta",     0.0)), 4),
            "delta_pct": round(float(dv.get("delta_pct", 0.5)), 4),
            "bullish":   bool(dv.get("bullish",  False)),
            "bearish":   bool(dv.get("bearish",  False)),
            "window":    DELTA_VOL_WINDOW,
            "threshold": DELTA_VOL_SIGNAL_PCT,
            "ts":        dv.get("ts", 0.0),
            "fresh":     (datetime.now(timezone.utc).timestamp() - float(dv.get("ts", 0.0) or 0.0)) < 120,
        }
    return {"enabled": DELTA_VOL_ENABLED, "data": out}


@app.get("/api/gold/status")
async def get_gold_status(req: FARequest):
    """Statut du provider Gold XAU/USD (Twelve Data ou Yahoo Finance fallback)."""
    td_configured = bool(TWELVEDATA_API_KEY)
    gold_ok = any(not df.empty for df in gold_store.values()) if gold_store else False

    status = {
        "enabled":        GOLD_REAL_ENABLED,
        "provider":       "twelve_data" if td_configured else "yahoo_finance_fallback",
        "twelvedata_key": "✅ configurée" if td_configured else "⚠️  non configurée (utilise Yahoo Finance)",
        "yahoo_fallback": "✅ actif" if not td_configured else "en réserve",
        "cache_ttl_s":    GOLD_CACHE_TTL,
        "symbols":        list(GOLD_SYMBOL_MAP.keys()),
        "data_available": gold_ok,
        "store_summary": {
            sym: {"rows": len(df), "last": str(df.index[-1]) if not df.empty else None}
            for sym, df in gold_store.items()
        },
    }

    # Test rapide Twelve Data si clé dispo
    if td_configured:
        session: aiohttp.ClientSession = req.app.state.http
        try:
            async with session.get(
                f"{TWELVEDATA_BASE_URL}/api_usage",
                params={"apikey": TWELVEDATA_API_KEY},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                if r.status == 200:
                    usage = await r.json(content_type=None)
                    status["twelve_data_usage"] = {
                        "current":   usage.get("current_usage", "?"),
                        "plan_limit": usage.get("plan_limit", "?"),
                        "remaining": usage.get("plan_limit", 0) - usage.get("current_usage", 0),
                    }
        except Exception as e:
            status["twelve_data_check_error"] = str(e)[:60]

    return status


# ---------------------------------------------------------------------------
# MODULE 20 — Routes Order Book [v8]
# ---------------------------------------------------------------------------

@app.get("/api/orderbook/{symbol}")
async def get_orderbook(symbol: str, req: FARequest):
    """Snapshot du carnet d'ordres N2 pour un symbole donné."""
    engine: Optional[TitaniumOrderBookEngine] = getattr(req.app.state, "ob_engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Order Book Engine désactivé")

    # Normalise : BTCUSDT → BTC/USDT
    sym_ui = symbol.upper()
    # Cherche correspondance dans les symboles connus
    matched = None
    for s in engine.symbols:
        if rest_sym(s) == sym_ui or s.upper() == sym_ui:
            matched = s
            break
    if not matched:
        raise HTTPException(status_code=404, detail=f"Symbole {symbol} non suivi par le moteur")

    snapshot = engine.get_book_snapshot(matched)
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"Aucune donnée order book pour {matched}")
    return snapshot


@app.get("/api/orderbook/{symbol}/signals")
async def get_orderbook_signals(symbol: str, limit: int = 20, req: FARequest = None):
    """Signaux SMC (Murs, Imbalance, Absorption) pour un symbole donné."""
    engine: Optional[TitaniumOrderBookEngine] = getattr(req.app.state, "ob_engine", None) if req else _ob_engine
    if not engine:
        raise HTTPException(status_code=503, detail="Order Book Engine désactivé")

    sym_ui = symbol.upper()
    matched = next((s for s in engine.symbols if rest_sym(s) == sym_ui or s.upper() == sym_ui), None)
    return {"symbol": symbol, "signals": engine.get_recent_signals(matched, limit=limit)}


@app.get("/api/orderbook/signals/all")
async def get_all_ob_signals(limit: int = 50, req: FARequest = None):
    """Tous les signaux SMC Order Book récents (tous symboles confondus)."""
    engine: Optional[TitaniumOrderBookEngine] = getattr(req.app.state, "ob_engine", None) if req else _ob_engine
    if not engine:
        raise HTTPException(status_code=503, detail="Order Book Engine désactivé")
    return {"signals": engine.get_recent_signals(symbol=None, limit=limit)}


@app.get("/api/orderbook/status")
async def get_orderbook_status(req: FARequest):
    """Statut du moteur Order Book — état des carnets par symbole."""
    engine: Optional[TitaniumOrderBookEngine] = getattr(req.app.state, "ob_engine", None)
    if not engine:
        return {"enabled": False, "symbols": []}

    books_status = {}
    for sym in engine.symbols:
        book = engine.books.get(sym)
        books_status[sym] = {
            "active": book is not None and book.last_update > 0,
            "last_update": book.last_update if book else 0,
            "best_bid": book.best_bid if book else 0,
            "best_ask": book.best_ask if book else 0,
            "imbalance": round(book.imbalance, 4) if book else 0,
            "history_size": len(engine.history.get(sym, [])),
        }
    return {
        "enabled": OB_ENABLED,
        "symbols": engine.symbols,
        "books": books_status,
        "total_signals": len(engine.signals),
        "wall_threshold": OB_WALL_THRESHOLD,
        "imbalance_threshold": OB_IMBALANCE_THRESH,
        "absorption_window": OB_ABSORPTION_WINDOW,
    }


# ---------------------------------------------------------------------------
# Entry point (Module 17)
# ---------------------------------------------------------------------------
def main():
    host = os.getenv("UVICORN_HOST", "0.0.0.0")
    port = int(os.getenv("UVICORN_PORT", "8080"))
    log_level = os.getenv("UVICORN_LOG_LEVEL", "info")
    ssl_certfile = os.getenv("SSL_CERTFILE", "").strip() or None
    ssl_keyfile = os.getenv("SSL_KEYFILE", "").strip() or None

    print("=" * 70)
    print("TITANIUM DASHBOARD v8 — SMC LIVE · Order Book N2 · Score /9 · 1D Bias · Delta Vol · Futures OI · Gold XAU · API Key Read-Only")
    print("URL: http://localhost:8080")
    print("Bridge actif:", ENABLE_BRIDGE)
    print("Vision primary:", VISION_MODEL_PRIMARY, "| fallback:", VISION_MODEL_FALLBACK)
    print(f"SCORING: /9 (v8 +EMA200_1D +DELTA_VOL +OB_ENGINE) | RSI seuils: LONG≤{RSI_ENTRY_LONG} SHORT≥{RSI_ENTRY_SHORT} | Fib=[{FIB_LEVEL_LOW},{FIB_LEVEL_HIGH}] dynamique={OB_FVG_FIB_DYNAMIC}")
    print(f"STRICT: tf={STRICT_TF} | iters={STRICT_RANDOM_ITERS} | in_sample={STRICT_IN_SAMPLE_DAYS}j | recalib={STRICT_RECALIB_DAYS}j | sharpe_floor={STRICT_SHARPE_FLOOR}")
    print(f"LEARNING: report_every={LEARNING_REPORT_EVERY//3600}h | min_signals={LEARNING_MIN_SIGNALS} | adapt_rate={LEARNING_ADAPT_RATE}")
    print(f"SL/TP: adaptatif ATR clampé (p20–p80) | frais Binance 4bps | TP ratios 1.2/1.8/2.4 | 4 TPs max")
    print(f"OPTIM 2025: TF={OPT_TF} | in_sample={OPT_IN_SAMPLE_DAYS}j | criteria={OPT_SCORE_CRITERIA} | {len(OPT_CONFIGURATIONS)} configs | refresh={OPT_REFRESH_HOURS}h | endpoint=/api/optim/results")
    print(f"API KEY: {'✅ CONFIGURÉE (rate-limit levé)' if BINANCE_KEY else '⚠️  NON CONFIGURÉE (rate-limit public)'}")
    print(f"FUTURES: {'✅ activé (' + ','.join(FUTURES_SYMBOLS_MAP.keys()) + ')' if FUTURES_ENABLED else '⚠️  désactivé'} | cache={FUTURES_CACHE_TTL}s")
    print(f"GOLD XAU/USD: {'✅ Twelve Data (clé configurée)' if TWELVEDATA_API_KEY else '⚠️  Yahoo Finance fallback (GC=F, aucune clé requise)'} | cache={GOLD_CACHE_TTL}s | endpoint=/api/gold/status")
    print(f"DELTA_VOL: {'✅ activé' if DELTA_VOL_ENABLED else 'désactivé'} | window={DELTA_VOL_WINDOW} trades | seuil={DELTA_VOL_SIGNAL_PCT*100:.0f}%")
    print(f"1D BIAIS: {'✅ activé' if USE_1D_BIAS else 'désactivé'} | cache={D1_CACHE_TTL}s | limit={D1_LIMIT} bougies")
    print(f"CANDLES LIMITS: 1D={D1_LIMIT} | H4=500 | H2=400 | H1=500 | 30m=500 | 15m=500 | 5m=1000 | 3m=150 | 1m=500")
    # MODULE 19 : affichage des overrides par symbole
    for sym_ov, ov in SYM_OVERRIDES.items():
        print(f"  [{sym_ov}] ATR×{ov.get('atr_mult')} | TP={ov.get('tp_ratios')} | RSI {ov.get('rsi_long')}/{ov.get('rsi_short')} | score_min={ov.get('score_min')} | {len(OPT_CONFIGURATIONS_PAXG)} configs dédiées")
    print(f"WS: compress={WS_COMPRESS} | min_bytes={WS_COMPRESS_MIN_BYTES}")
    print(f"ORDER BOOK [v8]: {'✅ activé — wall={:.0%} | imbalance={:.0%} | absorption_window={}'.format(OB_WALL_THRESHOLD, OB_IMBALANCE_THRESH, OB_ABSORPTION_WINDOW) if OB_ENABLED else '⚠️  désactivé (OB_ENABLED=0)'} | endpoints=/api/orderbook/{{symbol}}")
    print("=" * 70)

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=False,
        workers=1,
        log_level=log_level,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
    )

if __name__ == "__main__":
    main()
