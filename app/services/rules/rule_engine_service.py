from __future__ import annotations

"""Rule engine extracted from v3.

The service evaluates InsO clawback heuristics (§130–§135) for a transaction.
It is designed to be *data-driven* and side-effect free: no DB writes.

Persistence is handled by the caller (typically ingest/background pipeline).
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, List
import json

from app.db.models import Case, Transaction, Counterparty


ENFORCEMENT_KEYWORDS = [
    "pfändung", "vollstreckung", "gerichtsvollzieher", "zwangsvollstreckung",
    "mahnbescheid", "vollstreckungsbescheid", "bailiff", "garnishment",
    "inkasso", "arrest",
]

CRISIS_KNOWLEDGE_INDICATORS = [
    "mahnung", "zahlungserinnerung", "ratenzahlung", "stundung",
    "rücklastschrift", "nicht eingelöst", "bounced", "zahlungsunfähig",
    "insolvenz", "krise", "liquiditätsengpass",
]

GRATUITOUS_KEYWORDS = [
    "schenkung", "donation", "gift", "erlass", "verzicht",
    "unentgeltlich", "gratuitous", "ohne gegenleistung",
]

SHAREHOLDER_LOAN_KEYWORDS = [
    "gesellschafterdarlehen", "shareholder loan", "darlehen gesellschafter",
    "rückzahlung darlehen", "loan repayment",
]


@dataclass
class RuleResult:
    rule_id: str
    rule_version: str
    decision: str
    confidence: float
    explanation: str
    legal_basis: Optional[str] = None
    lookback_start: Optional[str] = None
    lookback_end: Optional[str] = None
    conditions_met: Optional[list] = None
    conditions_missing: Optional[list] = None
    evidence_present: Optional[list] = None
    evidence_missing: Optional[list] = None



def _safe_json_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            data = json.loads(value or '[]')
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []

def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _days_before(anchor: date, tx_date: date) -> int:
    return (anchor - tx_date).days


def _in_window(tx_date: date, anchor: date, days: int) -> bool:
    start = anchor - timedelta(days=days)
    return start <= tx_date <= anchor


def _text_contains(text: str, keywords: List[str]) -> List[str]:
    if not text:
        return []
    t = text.lower()
    return [kw for kw in keywords if kw in t]


def _lookback_start(anchor: Optional[date], days: int) -> Optional[str]:
    if not anchor:
        return None
    return str(anchor - timedelta(days=days))


def _lookback_end(anchor: Optional[date]) -> Optional[str]:
    return str(anchor) if anchor else None


def _decision(confidence: float, conditions_missing: list, *, min_conf_hit: float) -> str:
    """Shared decision heuristic for rules that use confidence.

    - HIT if confidence >= min_conf_hit and no critical missing conditions
    - NEEDS_REVIEW if in-window but not enough confidence
    - NO_HIT otherwise
    """
    # If we have any condition explicitly indicating we're out of window, treat as NO_HIT
    out_of_window = any(
        (isinstance(c, str) and "window" in c and "in_" in c) or (isinstance(c, dict) and c.get("condition") == "in_lookback" and c.get("met") is False)
        for c in (conditions_missing or [])
    )
    if out_of_window:
        return "NO_HIT"
    if confidence >= min_conf_hit:
        return "HIT"
    # if at least something suggests potential (i.e., not out_of_window), request review
    return "NEEDS_REVIEW" if confidence > 0 else "NO_HIT"


def _explain(rule_id: str, decision: str, confidence: float, met: list, missing: list, evidence_missing: list) -> str:
    parts = [f"{rule_id} decision={decision} confidence={round(confidence, 3)}"]
    if met:
        parts.append(f"met={met}")
    if missing:
        parts.append(f"missing={missing}")
    if evidence_missing:
        parts.append(f"evidence_missing={evidence_missing}")
    return "; ".join(parts)


def evaluate_P130(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> RuleResult:
    anchor = case.insolvenzantrag_date or case.cutoff_date
    tx_date = _parse_iso_date(tx.transaction_date)

    conditions_met, conditions_missing = [], []
    evidence_present, evidence_missing = [], []
    confidence = 0.0

    in_3m = bool(anchor and tx_date and _in_window(tx_date, anchor, 90))
    in_1m = bool(anchor and tx_date and _in_window(tx_date, anchor, 30))

    if in_3m:
        conditions_met.append({"condition": "in_lookback_3m", "met": True, "detail": f"{_days_before(anchor, tx_date)} days before Antrag"})
        confidence += 0.3
    else:
        conditions_missing.append({"condition": "in_lookback_3m", "met": False})

    conditions_met.append({"condition": "debtor_illiquidity", "met": "assumed", "detail": "Case exists — illiquidity assumed for MVP"})
    evidence_missing.append("Independent illiquidity assessment / Gutachten")
    confidence += 0.15

    desc = (tx.transaction_description or "")
    crisis_hits = _text_contains(desc, CRISIS_KNOWLEDGE_INDICATORS)
    if crisis_hits:
        conditions_met.append({"condition": "creditor_knowledge_indicators", "met": True, "detail": f"Keywords: {', '.join(crisis_hits)}"})
        evidence_present.append(f"Crisis indicators in description: {crisis_hits}")
        confidence += 0.2
    else:
        conditions_missing.append({"condition": "creditor_knowledge", "met": "unknown"})
        evidence_missing.append("Evidence of creditor knowledge (Kenntnis)")

    if cp and cp.is_related_party == "yes":
        conditions_met.append({"condition": "related_party_knowledge_presumed", "met": True, "detail": f"Related party: {cp.name}"})
        evidence_present.append(f"Related party confirmed for {cp.name}")
        confidence += 0.25

    conditions_met.append({"condition": "congruent_performance", "met": "assumed", "detail": "Standard payment — congruent assumed"})

    decision = "HIT" if in_3m and confidence >= 0.45 else ("NEEDS_REVIEW" if in_3m else "NO_HIT")

    return RuleResult(
        rule_id="§130",
        rule_version="1.0",
        decision=decision,
        confidence=min(confidence, 1.0),
        explanation=(
            f"§130 Congruent satisfaction. Transaction {(_days_before(anchor, tx_date) if (anchor and tx_date) else '?')} days before Antrag. "
            f"{'Related party — knowledge presumed.' if (cp and cp.is_related_party == 'yes') else 'Creditor knowledge needs proof.'}"
        ),
        legal_basis="InsO §130 Abs. 1 S. 1 Nr. 1" if in_3m and not in_1m else "InsO §130 Abs. 1 S. 1 Nr. 2",
        lookback_start=str(anchor - timedelta(days=90)) if anchor else None,
        lookback_end=str(anchor) if anchor else None,
        conditions_met=conditions_met,
        conditions_missing=conditions_missing,
        evidence_present=evidence_present,
        evidence_missing=evidence_missing,
    )


def evaluate_P131(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> RuleResult:
    anchor = case.insolvenzantrag_date or case.cutoff_date
    tx_date = _parse_iso_date(tx.transaction_date)

    conditions_met, conditions_missing = [], []
    evidence_present, evidence_missing = [], []
    confidence = 0.0

    in_1m = bool(anchor and tx_date and _in_window(tx_date, anchor, 30))
    in_3m = bool(anchor and tx_date and _in_window(tx_date, anchor, 90))

    if in_1m:
        conditions_met.append({"condition": "in_lookback_1m", "met": True, "detail": f"{_days_before(anchor, tx_date)} days before Antrag"})
        confidence += 0.35
    elif in_3m:
        conditions_met.append({"condition": "in_lookback_3m", "met": True})
        confidence += 0.2
    else:
        conditions_missing.append({"condition": "in_lookback", "met": False})

    desc = (tx.transaction_description or "")
    enforcement_hits = _text_contains(desc, ENFORCEMENT_KEYWORDS)
    if enforcement_hits:
        conditions_met.append({"condition": "enforcement_pressure", "met": True, "detail": f"Keywords: {', '.join(enforcement_hits)}"})
        evidence_present.append(f"Enforcement/pressure indicators: {enforcement_hits}")
        confidence += 0.3

    if any(kw in desc.lower() for kw in ["bar", "cash", "kasse", "dritter", "third party"]):
        conditions_met.append({"condition": "unusual_payment_method", "met": True})
        evidence_present.append("Unusual payment method detected")
        confidence += 0.2

    if not enforcement_hits:
        evidence_missing.append("Evidence of incongruence: enforcement, unusual method, premature payment")

    decision = "HIT" if (in_1m and confidence >= 0.35) or (in_3m and confidence >= 0.5) else ("NEEDS_REVIEW" if in_3m else "NO_HIT")

    return RuleResult(
        rule_id="§131",
        rule_version="1.0",
        decision=decision,
        confidence=min(confidence, 1.0),
        explanation=(
            f"§131 Incongruent satisfaction. {'Within 1-month strict window.' if in_1m else 'Within 3-month window.'} "
            f"{'Enforcement pressure detected.' if enforcement_hits else 'No clear incongruence indicators found.'}"
        ),
        legal_basis="InsO §131 Abs. 1 Nr. 1" if in_1m else "InsO §131 Abs. 1 Nr. 2/3",
        lookback_start=str(anchor - timedelta(days=30 if in_1m else 90)) if anchor else None,
        lookback_end=str(anchor) if anchor else None,
        conditions_met=conditions_met,
        conditions_missing=conditions_missing,
        evidence_present=evidence_present,
        evidence_missing=evidence_missing,
    )



def evaluate_P132(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> RuleResult:
    """§132 InsO — Directly prejudicial acts (3 months)."""
    anchor = case.insolvenzantrag_date or case.cutoff_date
    tx_date = _parse_iso_date(tx.transaction_date)

    conditions_met, conditions_missing = [], []
    evidence_present, evidence_missing = [], []
    confidence = 0.0

    in_3m = bool(anchor and tx_date and _in_window(tx_date, anchor, 90))
    if in_3m:
        conditions_met.append("in_3_month_window")
        confidence += 0.3
    else:
        conditions_missing.append("in_3_month_window")

    # Heuristic: outflow without clear consideration (fees, taxes, penalties, donations)
    desc = (tx.transaction_description or "").lower()
    suspect = any(k in desc for k in ["strafe", "penalty", "gebühr", "fee", "donation", "spende", "fine"])
    if suspect and (tx.amount or 0.0) < 0:
        conditions_met.append("direct_prejudice_indicator")
        confidence += 0.4
        evidence_present.append("transaction_description_signal")
    elif tx.amount < 0:
        conditions_missing.append("direct_prejudice_indicator")
        evidence_missing.append("proof_no_equivalent_benefit")

    decision = _decision(confidence, conditions_missing, min_conf_hit=0.7)
    explanation = _explain("§132", decision, confidence, conditions_met, conditions_missing, evidence_missing)

    return RuleResult(
        rule_id="§132",
        rule_version="1.0",
        decision=decision,
        confidence=round(confidence, 3),
        explanation=explanation,
        legal_basis="InsO §132",
        lookback_start=_lookback_start(anchor, 90),
        lookback_end=_lookback_end(anchor),
        conditions_met=conditions_met,
        conditions_missing=conditions_missing,
        evidence_present=evidence_present,
        evidence_missing=evidence_missing,
    )

def evaluate_P133(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> RuleResult:
    """§133 InsO — Intentional prejudice (up to 4 years, heuristics)."""
    anchor = case.insolvenzantrag_date or case.cutoff_date
    tx_date = _parse_iso_date(tx.transaction_date)

    conditions_met, conditions_missing = [], []
    evidence_present, evidence_missing = [], []
    confidence = 0.0

    in_4y = bool(anchor and tx_date and _in_window(tx_date, anchor, 365 * 4))
    if in_4y:
        conditions_met.append("in_4_year_window")
        confidence += 0.2
    else:
        conditions_missing.append("in_4_year_window")

    # Crisis + selective payment heuristics
    tags = set((_safe_json_list(tx.tags) or []) + (tx.system_tags or []) + (tx.user_tags or []))
    if any(t in tags for t in ["crisis", "overdue", "collection", "mahnung"]):
        conditions_met.append("crisis_signal")
        confidence += 0.2
        evidence_present.append("tag_signal")

    # Related party increases probability
    if cp and (cp.is_related_party or "").lower() in ["yes", "true", "1"]:
        conditions_met.append("related_party")
        confidence += 0.3
        evidence_present.append("counterparty_related_party")
    elif cp and cp.role and cp.role.lower() in ["shareholder", "affiliate", "management"]:
        conditions_met.append("related_party_role")
        confidence += 0.25
        evidence_present.append("counterparty_role")

    # Large outflow close to anchor
    if anchor and tx_date and tx.amount < 0 and abs(tx.amount) >= 10000 and _in_window(tx_date, anchor, 180):
        conditions_met.append("large_payment_close_to_anchor")
        confidence += 0.2

    decision = _decision(confidence, conditions_missing, min_conf_hit=0.75)
    explanation = _explain("§133", decision, confidence, conditions_met, conditions_missing, evidence_missing)

    return RuleResult(
        rule_id="§133",
        rule_version="1.0",
        decision=decision,
        confidence=round(confidence, 3),
        explanation=explanation,
        legal_basis="InsO §133",
        lookback_start=_lookback_start(anchor, 365 * 4),
        lookback_end=_lookback_end(anchor),
        conditions_met=conditions_met,
        conditions_missing=conditions_missing,
        evidence_present=evidence_present,
        evidence_missing=evidence_missing,
    )

def evaluate_P134(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> RuleResult:
    """§134 InsO — gratuitous transactions (4 years). Heuristic via keywords."""
    anchor = case.eroeffnung_date or case.cutoff_date or case.insolvenzantrag_date
    tx_date = _parse_iso_date(tx.transaction_date)

    conditions_met, conditions_missing = [], []
    evidence_present, evidence_missing = [], []
    confidence = 0.0

    in_4y = bool(anchor and tx_date and _in_window(tx_date, anchor, 1460))
    if in_4y:
        conditions_met.append({"condition": "in_lookback_4y", "met": True})
        confidence += 0.25
    else:
        conditions_missing.append({"condition": "in_lookback_4y", "met": False})

    desc = (tx.transaction_description or "")
    hits = _text_contains(desc, GRATUITOUS_KEYWORDS)
    if hits:
        conditions_met.append({"condition": "gratuitous_indicators", "met": True, "detail": f"Keywords: {', '.join(hits)}"})
        evidence_present.append(f"Gratuitous indicators: {hits}")
        confidence += 0.5
    else:
        conditions_missing.append({"condition": "gratuitous", "met": "unknown"})
        evidence_missing.append("Evidence of lack of consideration")

    decision = "HIT" if in_4y and confidence >= 0.55 else ("NEEDS_REVIEW" if in_4y else "NO_HIT")

    return RuleResult(
        rule_id="§134",
        rule_version="1.0",
        decision=decision,
        confidence=min(confidence, 1.0),
        explanation="§134 Gratuitous transaction heuristic based on description keywords.",
        legal_basis="InsO §134 Abs. 1",
        lookback_start=str(anchor - timedelta(days=1460)) if anchor else None,
        lookback_end=str(anchor) if anchor else None,
        conditions_met=conditions_met,
        conditions_missing=conditions_missing,
        evidence_present=evidence_present,
        evidence_missing=evidence_missing,
    )


def evaluate_P135(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> RuleResult:
    """§135 InsO — shareholder loan repayment (1 year). Heuristic via keywords / related party."""
    anchor = case.insolvenzantrag_date or case.cutoff_date
    tx_date = _parse_iso_date(tx.transaction_date)

    conditions_met, conditions_missing = [], []
    evidence_present, evidence_missing = [], []
    confidence = 0.0

    in_1y = bool(anchor and tx_date and _in_window(tx_date, anchor, 365))
    if in_1y:
        conditions_met.append({"condition": "in_lookback_1y", "met": True})
        confidence += 0.2
    else:
        conditions_missing.append({"condition": "in_lookback_1y", "met": False})

    desc = (tx.transaction_description or "")
    hits = _text_contains(desc, SHAREHOLDER_LOAN_KEYWORDS)
    if hits:
        conditions_met.append({"condition": "shareholder_loan_keywords", "met": True, "detail": f"Keywords: {', '.join(hits)}"})
        evidence_present.append(f"Shareholder loan indicators: {hits}")
        confidence += 0.4

    if cp and cp.is_related_party == "yes":
        conditions_met.append({"condition": "related_party", "met": True, "detail": cp.name})
        confidence += 0.25

    if not hits:
        evidence_missing.append("Loan agreement / evidence of shareholder loan")

    decision = "HIT" if in_1y and confidence >= 0.55 else ("NEEDS_REVIEW" if in_1y else "NO_HIT")

    return RuleResult(
        rule_id="§135",
        rule_version="1.0",
        decision=decision,
        confidence=min(confidence, 1.0),
        explanation="§135 Shareholder loan repayment heuristic (keywords + related party).",
        legal_basis="InsO §135 Abs. 1 Nr. 2",
        lookback_start=str(anchor - timedelta(days=365)) if anchor else None,
        lookback_end=str(anchor) if anchor else None,
        conditions_met=conditions_met,
        conditions_missing=conditions_missing,
        evidence_present=evidence_present,
        evidence_missing=evidence_missing,
    )


def evaluate_all(tx: Transaction, case: Case, cp: Optional[Counterparty]) -> list[RuleResult]:
    # Ported from v3: §130–§135
    return [
        evaluate_P130(tx, case, cp),
        evaluate_P131(tx, case, cp),
        evaluate_P132(tx, case, cp),
        evaluate_P133(tx, case, cp),
        evaluate_P134(tx, case, cp),
        evaluate_P135(tx, case, cp),
    ]
