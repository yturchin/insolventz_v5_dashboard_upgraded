from __future__ import annotations

"""Seed demo data into the local SQLite DB.

Usage:
  python seed_demo.py
  python seed_demo.py --case-id case_0002 --company "ACME GmbH" --iban "DE..."
"""

import argparse

from app.api.deps import get_db
from app.tools.seed_demo_data import seed_demo_data


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--case-id", default="case_0001")
    p.add_argument("--company", default="Demo Insolvent GmbH")
    p.add_argument("--iban", default="DE89370400440532013000")
    p.add_argument("--currency", default="EUR")
    args = p.parse_args()

    db = next(get_db())
    try:
        res = seed_demo_data(db, case_id=args.case_id, company_name=args.company, account_iban=args.iban, currency=args.currency)
        print(f"Seeded {res.case_id}: documents={res.documents}, inserted={res.inserted}, evaluated={res.evaluated}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
