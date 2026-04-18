from __future__ import annotations

import main


class DummyStore:
    def __init__(self):
        self.created = []

    def create_pipeline_run(self, **kwargs):
        self.created.append(dict(kwargs))


class DummyGraph:
    def __init__(self):
        self.invocations = []

    def invoke(self, state, config=None):
        self.invocations.append({"state": dict(state), "config": dict(config or {})})
        return {
            "golden_record": {},
            "test_results": {},
            "golden_record_path": None,
        }


def test_run_full_pipeline_threads_user_id(monkeypatch):
    store = DummyStore()
    graph = DummyGraph()
    supabase_calls = []

    monkeypatch.setattr(main, "get_local_store_client", lambda: store)
    monkeypatch.setattr(main, "graph", graph)
    monkeypatch.setattr(
        main,
        "_register_supabase_run",
        lambda run_id, company_name, company_id, *, user_id: supabase_calls.append(
            {
                "run_id": run_id,
                "company_name": company_name,
                "company_id": company_id,
                "user_id": user_id,
            }
        ),
    )

    main.run_full_pipeline("Acme Corp", user_id="user-123")

    assert store.created[0]["user_id"] == "user-123"
    assert supabase_calls[0]["user_id"] == "user-123"
    assert graph.invocations[0]["state"]["user_id"] == "user-123"
    assert graph.invocations[0]["state"]["company_name"] == "Acme Corp"


def test_run_full_pipeline_rejects_missing_user_id(monkeypatch):
    graph = DummyGraph()
    monkeypatch.setattr(main, "graph", graph)

    try:
        main.run_full_pipeline("Acme Corp", user_id="")
    except ValueError as exc:
        assert "user_id" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing user_id")

    assert graph.invocations == []
