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


def test_local_store_shares_records_across_authenticated_users(tmp_path):
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

    visible = store.get_company(company_id="acme", user_id="user-b")
    assert visible is not None
    assert visible["company_name"] == "Acme"
    assert len(store.list_companies(user_id="user-b")) == 1


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


def test_local_store_tracks_versions_per_user_company(tmp_path):
    store = LocalStoreClient(root=str(tmp_path))
    store.insert_company_raw_data(
        run_id="run-1",
        company_name="Acme",
        company_id="acme",
        raw_json={"employees": "100"},
        user_id="user-a",
    )
    store.upsert_company_consolidated_data(
        run_id="run-1",
        company_name="Acme",
        company_id="acme",
        consolidated_json={"company_name": "Acme"},
        chunk_count=4,
        chunk_coverage_pct=92.0,
        user_id="user-a",
    )

    versions = store.list_company_versions(user_id="user-a", company_id="acme", limit=10)
    assert len(versions) >= 2
    assert {row["version_kind"] for row in versions}.issuperset({"raw_data", "consolidated"})
    assert all(row["user_id"] == "user-a" for row in versions)


def test_local_store_records_activity_and_error_logs(tmp_path):
    store = LocalStoreClient(root=str(tmp_path))

    store.record_admin_activity(
        actor_user_id="admin-1",
        activity_type="user_verified",
        scope="admin",
        details={"target_user_id": "user-2"},
    )
    store.record_error_event(
        user_id="user-2",
        error_type="pipeline_failed",
        message="LLM timeout",
        source="pipeline_worker",
    )

    activity = store.list_activity_logs(limit=10)
    errors = store.list_error_logs(limit=10)
    assert activity and activity[0]["activity_type"] == "user_verified"
    assert errors and errors[0]["error_type"] == "pipeline_failed"
