"""
verify_pinecone.py
------------------
Verifies that a company's golden record was correctly pushed to Pinecone.

Usage:
    python verify_pinecone.py --company TCS --user-id <supabase-user-id>
    python verify_pinecone.py --company TCS --user-id <supabase-user-id> --show-text
    python verify_pinecone.py --company TCS --user-id <supabase-user-id> --delete
"""

import argparse
import os
from dotenv import load_dotenv
from pinecone import Pinecone

from core.pinecone_store import (
    INDEX_NAME,
    MAX_CHUNKS_PER_CATEGORY,
    NAMESPACE,
    VERSION,
    _user_scope_slug,
)

load_dotenv()

def category_slug(category: str) -> str:
    return (
        category.lower()
        .replace(" & ", "_and_")
        .replace(" / ", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )

def vector_id(company_id: str, category: str, user_id: str = "") -> str:
    if user_id.strip():
        return f"{_user_scope_slug(user_id)}__{company_id}_{category_slug(category)}"
    return f"{company_id}_{category_slug(category)}"

def chunk_vector_id(company_id: str, category: str, chunk_index: int, user_id: str = "") -> str:
    if user_id.strip():
        return f"{_user_scope_slug(user_id)}__{company_id}_{category_slug(category)}_{chunk_index:02d}"
    return f"{company_id}_{category_slug(category)}_{chunk_index:02d}"

def all_categories():
    from core.prompts import _SCHEMA_ROWS

    return sorted({row[1] for row in _SCHEMA_ROWS})

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--company",   required=True, help="Company name e.g. TCS")
    parser.add_argument("--user-id",   default="", help="Authenticated user id (recommended)")
    parser.add_argument("--show-text", action="store_true", help="Print chunk text")
    parser.add_argument("--delete",    action="store_true", help="Delete all vectors for this company")
    args = parser.parse_args()

    company_id = "_".join(args.company.lower().split())
    user_id = str(args.user_id or "").strip()
    pc    = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    index = pc.Index(INDEX_NAME)

    print(f"\n{'='*60}")
    print(f"  Pinecone Verification — {args.company}")
    print(f"  Index: {INDEX_NAME} | Namespace: {NAMESPACE} | Version: {VERSION}")
    if user_id:
        print(f"  User ID: {user_id}")
    else:
        print("  User ID: (not provided — checking legacy unscoped IDs)")
    print(f"{'='*60}\n")

    # ── Delete mode ───────────────────────────────────────────────────────────
    if args.delete:
        ids = []
        for cat in all_categories():
            ids.append(vector_id(company_id, cat, user_id))
            for chunk_index in range(MAX_CHUNKS_PER_CATEGORY):
                ids.append(chunk_vector_id(company_id, cat, chunk_index, user_id))
        index.delete(ids=ids, namespace=NAMESPACE)
        print(f"  Deleted all vectors for '{args.company}' (company_id={company_id})")
        return

    # ── Stats ─────────────────────────────────────────────────────────────────
    stats = index.describe_index_stats()
    total = stats.get("total_vector_count", 0)
    print(f"[Index Stats]")
    print(f"  Total vectors : {total}")
    print(f"  Dimension     : {stats.get('dimension', 'N/A')}")
    print()

    # ── Fetch by deterministic IDs ────────────────────────────────────────────
    ids_to_fetch = []
    for cat in all_categories():
        ids_to_fetch.append(vector_id(company_id, cat, user_id))
        ids_to_fetch.append(vector_id(company_id, cat))
        for chunk_index in range(MAX_CHUNKS_PER_CATEGORY):
            ids_to_fetch.append(chunk_vector_id(company_id, cat, chunk_index, user_id))
            ids_to_fetch.append(chunk_vector_id(company_id, cat, chunk_index))
    result = index.fetch(ids=ids_to_fetch, namespace=NAMESPACE)
    vectors = result.vectors if hasattr(result, "vectors") else {}

    print(f"[Category Check for '{args.company}' — company_id: {company_id}]")
    print(f"  {'Category':<30} {'Chunks':<8} {'Fields':<8} {'Version':<8} {'Run ID'}")
    print(f"  {'-'*86}")

    found   = []
    missing = []

    for category in all_categories():
        chunk_rows = []
        for chunk_index in range(MAX_CHUNKS_PER_CATEGORY):
            data = vectors.get(chunk_vector_id(company_id, category, chunk_index, user_id))
            if not data:
                data = vectors.get(chunk_vector_id(company_id, category, chunk_index))
            if data:
                owner = (getattr(data, "metadata", {}) or {}).get("user_id")
                if user_id and owner and owner != user_id:
                    continue
                chunk_rows.append(data)
        if not chunk_rows:
            legacy = vectors.get(vector_id(company_id, category, user_id))
            if not legacy:
                legacy = vectors.get(vector_id(company_id, category))
            if legacy:
                owner = (getattr(legacy, "metadata", {}) or {}).get("user_id")
                if user_id and owner and owner != user_id:
                    legacy = None
            if legacy:
                chunk_rows.append(legacy)

        if chunk_rows:
            meta = (chunk_rows[0].metadata or {}) if hasattr(chunk_rows[0], "metadata") else {}
            run_short = meta.get("run_id", "N/A")[:8] + "..."
            field_count = sum(int((getattr(row, "metadata", {}) or {}).get("field_count", 0) or 0) for row in chunk_rows)
            version = meta.get("version", "?")
            print(f"  {'✓  ' + category:<30} {str(len(chunk_rows)):<8} {str(field_count):<8} {version:<8} {run_short}")
            found.append(category)

            if args.show_text:
                for idx, row in enumerate(chunk_rows, start=1):
                    row_meta = getattr(row, "metadata", None) or {}
                    print(f"\n     Chunk {idx}:")
                    for line in row_meta.get("chunk_text", "")[:400].split("\n"):
                        print(f"       {line}")
                print()
        else:
            print(f"  {'✗  ' + category:<30} {'0':<8} {'—':<8} {'—':<8} —")
            missing.append(category)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Summary")
    print(f"{'='*60}")
    print(f"  Categories found   : {len(found)}/{len(all_categories())}")
    print(f"  Categories missing : {len(missing)}")

    if missing:
        print(f"\n  Missing (fields were all 'Not Found' in golden record):")
        for c in missing:
            print(f"    - {c}")

    if len(found) == len(all_categories()):
        print(f"\n  ALL CATEGORIES PRESENT — golden record fully stored.")
    elif len(found) > 0:
        print(f"\n  PARTIAL — {len(found)} categories stored.")
    else:
        print(f"\n  NO DATA FOUND for '{args.company}'.")
        print(f"  Check: pipeline ran? PINECONE_API_KEY set? company_id='{company_id}'?")
    print()

if __name__ == "__main__":
    main()
