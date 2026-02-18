from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Optional

from sqlalchemy.orm import Session

from app.db.models import Counterparty
from app.repositories.audit_repo import log_event


_SUFFIXES = {
    "gmbh", "mbh", "ag", "kg", "ug", "ohg", "gbr", "e.v.", "ev",
    "sp. z o.o.", "spzoo", "spolka", "sa", "s.a.", "llc", "ltd", "inc",
    # common long-form equivalents
    "gesellschaft", "mit", "beschränkter", "beschrankter", "haftung",
    "beschraenkter",
}


def _norm_name(name: str) -> str:
    s = (name or "").strip().lower()
    # German transliteration for stable matching
    s = (
        s.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    s = re.sub(r"[\t\n\r]+", " ", s)
    s = re.sub(r"[^a-z0-9ąćęłńóśżź\s\-\.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    parts = [p for p in re.split(r"\s+", s) if p]
    parts = [p for p in parts if p not in _SUFFIXES]
    return " ".join(parts)


def _norm_acct(acct: Optional[str]) -> Optional[str]:
    if not acct:
        return None
    s = re.sub(r"\s+", "", str(acct)).upper()
    return s or None


def _similar(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def get_or_create_counterparty(
    db: Session,
    *,
    case_id: str,
    name: Optional[str],
    account_number: Optional[str],
    actor: str = "system",
    fuzzy_threshold: float = 0.92,
) -> Optional[Counterparty]:
    """v3-parity-ish resolution:

    Priority:
      1) exact account match
      2) exact normalized name match
      3) fuzzy normalized name match (>= threshold)
      4) create new

    Aliases are tracked in enrichment_json['aliases'].
    """

    nm = (name or "").strip()
    acct = _norm_acct(account_number)
    if not nm and not acct:
        return None

    q = db.query(Counterparty).filter(Counterparty.case_id == case_id)

    # 1) account match
    if acct:
        cp = q.filter(Counterparty.account_number == acct).first()
        if cp:
            _maybe_add_alias(cp, nm)
            if nm and cp.name != nm:
                # keep canonical name, but remember alias
                pass
            return cp

    norm = _norm_name(nm) if nm else ""

    # 2) exact normalized name match
    if norm:
        for cp in q.all():
            cp_norm = (cp.enrichment_json or {}).get("name_norm") or _norm_name(cp.name)
            if cp_norm == norm:
                if acct and not cp.account_number:
                    cp.account_number = acct
                _maybe_add_alias(cp, nm)
                return cp

        # 3) fuzzy match
        best = None
        best_score = 0.0
        for cp in q.all():
            cp_norm = (cp.enrichment_json or {}).get("name_norm") or _norm_name(cp.name)
            score = _similar(norm, cp_norm)
            if score > best_score:
                best_score = score
                best = cp
        if best and best_score >= fuzzy_threshold:
            if acct and not best.account_number:
                best.account_number = acct
            _maybe_add_alias(best, nm)
            best.enrichment_json = {**(best.enrichment_json or {}), "matched_by": "fuzzy_name", "match_score": best_score}
            log_event(
                db,
                case_id=case_id,
                action="counterparty.matched_fuzzy",
                entity_type="counterparty",
                entity_id=str(best.id),
                payload={"name": nm, "norm": norm, "score": best_score, "account_number": acct},
                actor=actor,
            )
            return best

    # 4) create
    cp = Counterparty(case_id=case_id, name=nm or (acct or "Unknown"), account_number=acct)
    cp.enrichment_json = {"name_norm": _norm_name(cp.name), "aliases": []}
    if nm and nm != cp.name:
        cp.enrichment_json["aliases"].append(nm)
    db.add(cp)
    db.flush()
    log_event(
        db,
        case_id=case_id,
        action="counterparty.created",
        entity_type="counterparty",
        entity_id=str(cp.id),
        payload={"name": cp.name, "account_number": cp.account_number},
        actor=actor,
    )
    return cp


def _maybe_add_alias(cp: Counterparty, alias: str) -> None:
    alias = (alias or "").strip()
    if not alias or alias == cp.name:
        return
    ej = cp.enrichment_json or {}
    aliases = list(ej.get("aliases") or [])
    if alias not in aliases:
        aliases.append(alias)
    ej["aliases"] = aliases
    ej.setdefault("name_norm", _norm_name(cp.name))
    cp.enrichment_json = ej
