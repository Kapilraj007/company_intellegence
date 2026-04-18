"""
Rebuild Pinecone vectors from locally stored consolidated company records.

Usage:
    ./venv/bin/python reindex_pinecone.py --user-id <supabase-user-id>
    ./venv/bin/python reindex_pinecone.py --user-id <supabase-user-id> --company Apple
    ./venv/bin/python reindex_pinecone.py --user-id <supabase-user-id> --delete-first
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from core.local_store import get_local_store_client
from core.pinecone_store import VERSION, get_pinecone_client


def _make_company_id(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in str(value or "").lower())
    return "_".join(part for part in safe.split("_") if part) or "unknown"


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Reindex local company records into Pinecone")
    parser.add_argument("--company", default="", help="Optional company name to reindex")
    parser.add_argument("--delete-first", action="store_true", help="Delete existing vectors for the target company before upsert")
    parser.add_argument("--user-id", default="", help="Authenticated user id for scoped reindexing")
    args = parser.parse_args()
    user_id = str(args.user_id or "").strip()
    if not user_id:
        raise SystemExit("--user-id is required for scoped reindexing.")

    store = get_local_store_client()
    pinecone = get_pinecone_client()

    if args.company.strip():
        docs = [store.get_company(
            company_id=_make_company_id(args.company),
            company_name=args.company,
            user_id=user_id,
        )]
    else:
        docs = store.list_companies(user_id=user_id)

    docs = [doc for doc in docs if isinstance(doc, dict) and doc]
    if not docs:
        raise SystemExit("No matching local company records found.")

    print(f"Reindexing {len(docs)} company record(s) into Pinecone version {VERSION}")
    for doc in docs:
        company_name = str(doc.get("company_name") or "").strip()
        company_id = str(doc.get("company_id") or _make_company_id(company_name))
        consolidated_json = ((doc.get("consolidated") or {}).get("json") or {})
        run_id = str((doc.get("consolidated") or {}).get("run_id") or "local-reindex")

        if not isinstance(consolidated_json, dict) or not consolidated_json:
            print(f"Skipping {company_name or company_id}: no consolidated JSON found")
            continue

        if args.delete_first:
            pinecone.delete_company(company_id=company_id, user_id=user_id)

        vector_count = pinecone.upsert_golden_record(
            run_id=run_id,
            company_id=company_id,
            company_name=company_name or company_id,
            golden_record=consolidated_json,
            user_id=user_id,
        )
        print(f"  {company_name or company_id}: {vector_count} vectors upserted")


if __name__ == "__main__":
    main()
