import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
import aiohttp
import numpy as np

# Configuration
BINANCE_WS_URL = "wss://fstream.binance.com/ws"
MAX_HISTORY = 500  # Garder les 500 dernières mises à jour pour analyse
WALL_THRESHOLD_PERCENT = 0.05  # Un mur représente > 5% du volume total du book
ICEBERG_DETECTION_WINDOW = 5  # Nombre de mises à jour pour détecter un iceberg

@dataclass
class OrderBookLevel:
    price: float
    quantity: float
    count: int = 1

@dataclass
class MarketSignal:
    timestamp: float
    symbol: str
    signal_type: str  # WALL_BUY, WALL_SELL, ABSORPTION, FAKEOUT, IMBALANCE_SPIKE
    price: float
    value: float
    description: str
    strength: float  # 0.0 to 1.0

@dataclass
class OrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update: float = 0.0
    imbalance: float = 0.0
    total_bid_vol: float = 0.0
    total_ask_vol: float = 0.0

class OrderBookEngine:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.books: Dict[str, OrderBookState] = {s: OrderBookState() for s in symbols}
        self.history: Dict[str, deque] = {s: deque(maxlen=MAX_HISTORY) for s in symbols}
        self.signals: deque = deque(maxlen=100)
        self.callbacks: List[Callable] = []
        self.session: Optional[aiohttp.ClientSession] = None
        self.running = False
        
        # Seuils de stratégie
        self.wall_threshold = WALL_THRESHOLD_PERCENT
        self.imbalance_threshold = 0.7  # 70% d'imbalance déclenche une alerte

    def register_callback(self, callback: Callable):
        """Enregistrer une fonction appelée à chaque nouveau signal"""
        self.callbacks.append(callback)

    async def start(self):
        self.running = True
        self.session = aiohttp.ClientSession()
        tasks = [self._connect_symbol(symbol) for symbol in self.symbols]
        await asyncio.gather(*tasks)

    async def stop(self):
        self.running = False
        if self.session:
            await self.session.close()

    async def _connect_symbol(self, symbol: str):
        stream_name = f"{symbol.lower()}@depth20@100ms"
        url = f"{BINANCE_WS_URL}/{stream_name}"
        
        print(f"[OrderBook] Connexion à {symbol} sur {url}")
        
        try:
            async with self.session.ws_connect(url) as ws:
                async for msg in ws:
                    if not self.running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        await self._process_update(symbol, data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"[OrderBook] Erreur WS {symbol}: {ws.exception()}")
                        break
        except Exception as e:
            print(f"[OrderBook] Exception {symbol}: {e}")
            if self.running:
                await asyncio.sleep(5)
                await self._connect_symbol(symbol)

    async def _process_update(self, symbol: str, data: dict):
        if 'bids' not in data or 'asks' not in data:
            return

        book = self.books[symbol]
        timestamp = time.time()

        # Mise à jour du carnet
        new_bids = {float(p): float(q) for p, q in data['bids']}
        new_asks = {float(p): float(q) for p, q in data['asks']}

        # Calcul des volumes totaux (top 20 niveaux)
        book.total_bid_vol = sum(new_bids.values())
        book.total_ask_vol = sum(new_asks.values())
        
        # Calcul Imbalance (Ratio Acheteur / (Acheteur + Vendeur))
        total_vol = book.total_bid_vol + book.total_ask_vol
        if total_vol > 0:
            book.imbalance = book.total_bid_vol / total_vol
        else:
            book.imbalance = 0.5

        book.bids = new_bids
        book.asks = new_asks
        book.last_update = timestamp

        # Stocker l'historique pour détection de motifs
        state_snapshot = {
            'time': timestamp,
            'imbalance': book.imbalance,
            'best_bid': max(new_bids.keys()) if new_bids else 0,
            'best_ask': min(new_asks.keys()) if new_asks else 0,
            'bid_vol': book.total_bid_vol,
            'ask_vol': book.total_ask_vol
        }
        self.history[symbol].append(state_snapshot)

        # Analyse Stratégique
        await self._analyze_strategies(symbol, state_snapshot)

    async def _analyze_strategies(self, symbol: str, current: dict):
        signals = []
        history = list(self.history[symbol])
        if len(history) < 5:
            return

        # 1. Détection des MURS (Walls)
        book = self.books[symbol]
        max_bid_qty = max(book.bids.values()) if book.bids else 0
        max_ask_qty = max(book.asks.values()) if book.asks else 0
        
        if max_bid_qty > (book.total_bid_vol * self.wall_threshold):
            price = [p for p, q in book.bids.items() if q == max_bid_qty][0]
            signals.append(MarketSignal(
                timestamp=current['time'],
                symbol=symbol,
                signal_type="WALL_BUY",
                price=price,
                value=max_bid_qty,
                description=f"Mur d'achat massif détecté à {price}",
                strength=min(1.0, max_bid_qty / (book.total_bid_vol * 0.1))
            ))

        if max_ask_qty > (book.total_ask_vol * self.wall_threshold):
            price = [p for p, q in book.asks.items() if q == max_ask_qty][0]
            signals.append(MarketSignal(
                timestamp=current['time'],
                symbol=symbol,
                signal_type="WALL_SELL",
                price=price,
                value=max_ask_qty,
                description=f"Mur de vente massif détecté à {price}",
                strength=min(1.0, max_ask_qty / (book.total_ask_vol * 0.1))
            ))

        # 2. Détection d'Imbalance Extrême (Pression Achat/Vente)
        if current['imbalance'] > self.imbalance_threshold:
            signals.append(MarketSignal(
                timestamp=current['time'],
                symbol=symbol,
                signal_type="IMBALANCE_BUY",
                price=current['best_bid'],
                value=current['imbalance'],
                description=f"Pression acheteuse forte ({current['imbalance']:.2%})",
                strength=(current['imbalance'] - 0.5) * 2
            ))
        elif current['imbalance'] < (1 - self.imbalance_threshold):
            signals.append(MarketSignal(
                timestamp=current['time'],
                symbol=symbol,
                signal_type="IMBALANCE_SELL",
                price=current['best_ask'],
                value=1 - current['imbalance'],
                description=f"Pression vendeuse forte ({1-current['imbalance']:.2%})",
                strength=((1 - current['imbalance']) - 0.5) * 2
            ))

        # 3. Détection d'Absorption (Prix stagne mais volume énorme en face)
        if len(history) >= 5:
            recent = history[-5:]
            avg_price_change = abs(recent[-1]['best_bid'] - recent[0]['best_bid'])
            total_vol_pushed = sum(h['ask_vol'] for h in recent) # Volume vendu tentant de faire baisser
            
            # Si gros volume vendu mais prix ne baisse pas -> Absorption (Achat caché)
            if total_vol_pushed > (book.total_ask_vol * 2) and avg_price_change < 0.0005:
                signals.append(MarketSignal(
                    timestamp=current['time'],
                    symbol=symbol,
                    signal_type="ABSORPTION_BUY",
                    price=current['best_bid'],
                    value=total_vol_pushed,
                    description="Absorption detected: Vente massive absorbée sans baisse de prix",
                    strength=0.8
                ))

        # Envoyer les signaux
        for sig in signals:
            self.signals.append(sig)
            for cb in self.callbacks:
                try:
                    if asyncio.iscoroutinefunction(cb):
                        await cb(sig)
                    else:
                        cb(sig)
                except Exception as e:
                    print(f"Erreur callback signal: {e}")

    def get_current_state(self, symbol: str) -> Optional[dict]:
        if symbol not in self.books:
            return None
        book = self.books[symbol]
        return {
            'symbol': symbol,
            'timestamp': book.last_update,
            'imbalance': book.imbalance,
            'total_bid_vol': book.total_bid_vol,
            'total_ask_vol': book.total_ask_vol,
            'bids': [[p, q] for p, q in sorted(book.bids.items(), reverse=True)[:10]],
            'asks': [[p, q] for p, q in sorted(book.asks.items())[:10]],
            'recent_signals': [s.__dict__ for s in list(self.signals)[-10:] if s.symbol == symbol]
        }

# Exemple d'utilisation standalone pour test
if __name__ == "__main__":
    async def print_signal(sig: MarketSignal):
        print(f"🚨 SIGNAL: {sig.signal_type} sur {sig.symbol} @ {sig.price} - {sig.description}")

    engine = OrderBookEngine(["BTCUSDT", "ETHUSDT"])
    engine.register_callback(print_signal)
    
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        asyncio.run(engine.stop())