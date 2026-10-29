import pytest


@pytest.fixture
def use_silver(monkeypatch):
    def _use(module, tables):
        def fake_silver(table):
            assert table in tables, f"unexpected silver table: {table}"
            return tables[table]

        monkeypatch.setattr(module, "silver", fake_silver)

    return _use