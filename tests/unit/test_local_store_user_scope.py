from __future__ import annotations

import json

from core.local_store import LocalStoreClient


def test_local_store_writes_user_id_metadata(tmp_path):
    store = LocalStoreClient(root=str(tmp_path))

    store.create_pipeline_run(
        run_id="run-1",
        company_name="Acme",
        company_id="acme",
        user_id="user-a",
    )
    store.upsert_company_consolidated_data(
        run_id="run-1",
        company_name="Acme",
        company_id="acme",
        consolidated_json={"company_name": "Acme"},
        chunk_count=1,
        chunk_coverage_pct=100.0,
        user_id="user-a",
    )

    run_doc = json.loads((tmp_path / "runs" / "run_1.json").read_text(encoding="utf-8"))
    company_doc = store.get_company(company_id="acme", user_id="user-a")

    assert run_doc["user_id"] == "user-a"
    assert company_doc["user_id"] == "user-a"
    assert company_doc["consolidated"]["json"]["company_name"] == "Acme"


def test_local_store_hides_records_tagged_to_another_user(tmp_path):
    store = LocalStoreClient(root=str(tmp_path))
    store.upsert_company_consolidated_data(
        run_id="run-1",
        company_name="Acme",
        company_id="acme",
        consolidated_json={"company_name": "Acme"},
        chunk_count=1,
        chunk_coverage_pct=100.0,
        user_id="user-a",
    )

    assert store.get_company(company_id="acme", user_id="user-b") is None
    assert store.list_companies(user_id="user-b") == []


def test_local_store_preserves_legacy_untagged_visibility(tmp_path):
    store = LocalStoreClient(root=str(tmp_path))
    legacy_path = tmp_path / "companies" / "legacy.json"
    legacy_path.write_text(
        json.dumps(
            {
                "company_id": "legacy",
                "company_name": "Legacy",
                "consolidated": {"json": {"company_name": "Legacy"}},
            }
        ),
        encoding="utf-8",
    )

    assert store.get_company(company_id="legacy", user_id="user-a")["company_name"] == "Legacy"
    assert store.get_company(company_id="legacy", user_id="user-b")["company_name"] == "Legacy"
