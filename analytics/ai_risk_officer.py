#!/usr/bin/env python3
"""
VolGuard AI Risk Officer 2.0  –  Bayesian drop-in
Replaces naive VIX>13 veto with statistical learning.
External API 100 % compatible – no other files touched.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import numpy as np
from scipy.stats import beta as beta_dist
from sqlalchemy import select
from groq import Groq
from database.manager import HybridDatabaseManager
from database.models import DbTradeJournal, DbTradePostmortem
from core.models import MultiLegTrade

logger = logging.getLogger("AIRiskOfficer_v2")

# ---------- Bayesian pattern ----------
class BayesianPattern:
    __slots__ = ("name", "conditions", "alpha", "beta_param",
                 "n_trades", "last_updated", "half_life_days")
    def __init__(self, name: str, conditions: Dict):
        self.name = name
        self.conditions = conditions
        self.alpha = 1.0
        self.beta_param = 1.0
        self.n_trades = 0
        self.last_updated: Optional[datetime] = None
        self.half_life_days = 90.0

    def update_evidence(self, won: bool, trade_date: datetime):
        if self.last_updated:
            days = (trade_date - self.last_updated).days
            decay = 0.5 ** (days / self.half_life_days)
            self.alpha *= decay
            self.beta_param *= decay
        (self.alpha if won else self.beta_param) += 1.0
        self.n_trades += 1
        self.last_updated = trade_date

    def win_probability(self) -> float:
        return self.alpha / (self.alpha + self.beta_param)

    def confidence_interval(self, conf=0.95) -> Tuple[float, float]:
        dist = beta_dist(self.alpha, self.beta_param)
        lower = dist.ppf((1 - conf) / 2)
        upper = dist.ppf(1 - (1 - conf) / 2)
        return lower, upper

    def is_significant(self, threshold=0.55) -> bool:
        if self.n_trades < 15:
            return False
        lower_ci, _ = self.confidence_interval(0.95)
        return lower_ci > threshold or lower_ci < (1 - threshold)

    def matches(self, features: Dict) -> bool:
        for key, (lo, hi) in self.conditions.items():
            v = features.get(key)
            if v is None or not (lo <= v <= hi):
                return False
        return True


# ---------- Feature extractor ----------
def _extract_features(market: Dict) -> Dict[str, float]:
    """Lightweight feature set – fast, non-blocking."""
    f = {}
    f["vix"] = market.get("vix", 0)
    f["ivp"] = market.get("ivp", 50)
    f["atm_iv"] = market.get("atm_iv", 0.20)
    f["realized_vol"] = market.get("realized_vol_7d", 15)
    f["term_spread"] = market.get("term_structure_spread", 0)
    f["skew"] = market.get("volatility_skew", 0)
    regime = market.get("regime", "NEUTRAL")
    f["is_high_vol"] = 1.0 if "HIGH" in regime else 0.0
    f["is_panic"] = 1.0 if ("PANIC" in regime or "EXTREME" in regime) else 0.0
    f["atm_theta"] = market.get("atm_theta", 0)
    f["atm_vega"] = market.get("atm_vega", 0)
    return f


def _pattern_buckets(features: Dict) -> Dict[str, Dict]:
    """Create discrete buckets for Bayesian updating."""
    patterns = {}
    vix = features["vix"]
    ivp = features["ivp"]

    vix_bin = "LOW" if vix < 13 else "MED" if vix < 18 else "HIGH"
    ivp_bin = "LOW" if ivp < 25 else "MED" if ivp < 60 else "HIGH"

    patterns[f"VIX_{vix_bin}"] = {"vix": (0, 13) if vix_bin == "LOW" else (13, 18) if vix_bin == "MED" else (18, 100)}
    patterns[f"VIX_{vix_bin}_IVP_{ivp_bin}"] = {
        "vix": patterns[f"VIX_{vix_bin}"]["vix"],
        "ivp": (0, 25) if ivp_bin == "LOW" else (25, 60) if ivp_bin == "MED" else (60, 100)
    }
    if features["skew"] > 5:
        patterns["PUT_SKEW"] = {"skew": (5, 100)}
    if features["is_panic"]:
        patterns["PANIC_MODE"] = {"is_panic": (0.5, 1.0)}
    return patterns


# ---------- Intelligence core ----------
class _AIEngine:
    def __init__(self):
        self.patterns: Dict[str, BayesianPattern] = {}
        self.min_samples = 15
        self.veto_threshold = 0.35

    # ---------- learning ----------
    async def learn(self, db: HybridDatabaseManager):
        logger.info("🧠 Bayesian learning from closed trades...")
        async with db.get_session() as s:
            rows = (await s.execute(select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0))).scalars().all()
        if len(rows) < 10:
            logger.warning("Need ≥ 10 closed trades to learn – skipping.")
            return
        for t in rows:
            ctx = {"vix": t.vix_at_entry or 15, "ivp": 50, "regime": t.regime_at_entry or "NEUTRAL"}
            feats = _extract_features(ctx)
            won = t.net_pnl > 0
            for pname, cond in _pattern_buckets(feats).items():
                if pname not in self.patterns:
                    self.patterns[pname] = BayesianPattern(pname, cond)
                self.patterns[pname].update_evidence(won, t.date)
        sig = [p for p in self.patterns.values() if p.is_significant()]
        logger.info(f"✅ Loaded {len(self.patterns)} patterns ({len(sig)} significant).")

    # ---------- inference ----------
    def evaluate(self, market: Dict) -> Tuple[bool, List[Dict], str]:
        feats = _extract_features(market)
        matching = []
        for p in self.patterns.values():
            if p.is_significant() and p.matches(feats):
                matching.append({
                    "name": p.name,
                    "win_rate": p.win_probability(),
                    "n_trades": p.n_trades,
                    "ci": p.confidence_interval()
                })
        if not matching:
            return True, [], "No significant patterns – approved."

        # ensemble probability
        total_w = 0
        weighted_p = 0
        for m in matching:
            w = np.log1p(m["n_trades"])
            weighted_p += m["win_rate"] * w
            total_w += w
        ensemble_p = weighted_p / total_w if total_w else 0.5

        if ensemble_p < self.veto_threshold:
            worst = min(matching, key=lambda x: x["win_rate"])
            ci_lo, ci_hi = worst["ci"]
            reason = (f"AI VETO: pattern '{worst['name']}' {worst['win_rate']:.1%} win-rate "
                      f"(95 % CI {ci_lo:.1 %}-{ci_hi:.1 %}, n={worst['n_trades']}). "
                      f"Ensemble {ensemble_p:.1 %}")
            return False, matching, reason

        return True, matching, f"Approved – ensemble win-prob {ensemble_p:.1 %}"


# ---------- Public wrapper (old API) ----------
class AIRiskOfficer:
    """Drop-in replacement – same signature."""
    def __init__(self, groq_api_key: str, db_manager: HybridDatabaseManager):
        self.groq = Groq(api_key=groq_api_key) if groq_api_key else None
        self.db = db_manager
        self.engine = _AIEngine()
        self._last_learn = None

    async def learn_from_history(self, force_refresh: bool = False):
        await self.engine.learn(self.db)
        self._last_learn = datetime.utcnow()

    async def validate_trade(self, trade: MultiLegTrade, market: Dict) -> Tuple[bool, List[Dict], str]:
        if not self._last_learn or (datetime.utcnow() - self._last_learn).seconds > 3600:
            await self.learn_from_history()
        return self.engine.evaluate(market)

    async def generate_postmortem(self, trade: MultiLegTrade, pnl: float):
        if not self.groq:
            return
        try:
            grade = "A" if pnl > 3000 else "B" if pnl > 0 else "D" if pnl > -2000 else "F"
            prompt = (f"Grade {grade} trade {trade.strategy_type.value} PnL ₹{pnl:,}. "
                      "JSON: {'lesson': '...', 'key_mistake': '...' or null}")
            resp = self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            import json
            analysis = json.loads(resp.choices[0].message.content)
            async with self.db.get_session() as s:
                s.add(DbTradePostmortem(
                    trade_id=trade.id,
                    grade=grade,
                    lessons_learned=analysis.get("lesson", "N/A"),
                    ai_analysis=json.dumps(analysis)
                ))
                await self.db.safe_commit(s)
            logger.info(f"✅ Post-mortem saved for {trade.id} (Grade {grade})")
        except Exception as e:
            logger.error(f"Post-mortem failed: {e}")

    # legacy no-op stubs (kept for compat)
    async def fetch_fii_sentiment(self): return {}
    async def fetch_global_macro(self): return []
    async def fetch_smart_news(self): return []
    async def generate_comprehensive_briefing(self): return {}
