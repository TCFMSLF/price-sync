import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[3] / "scripts" / "demo.py"


def load_demo():
    spec = importlib.util.spec_from_file_location("demo_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_patch_price_retries_after_version_conflict(monkeypatch):
    demo = load_demo()
    calls = []
    responses = iter(
        [
            (409, {"detail": {"message": "конфликт", "currentPriceVersion": 7}}),
            (200, {"priceVersion": 8, "newPrice": "900.00"}),
        ]
    )

    def fake_request(method, url, payload=None):
        calls.append(payload)
        return next(responses)

    monkeypatch.setattr(demo, "request_json", fake_request)

    result = demo.patch_price("http://erp", "SKU-001", Decimal("900.00"), 1)

    assert result["priceVersion"] == 8
    assert calls[1]["expectedPriceVersion"] == 7


def test_patch_price_reports_unchanged_price(monkeypatch):
    demo = load_demo()
    monkeypatch.setattr(
        demo,
        "request_json",
        lambda *args, **kwargs: (409, {"detail": "Новая цена совпадает с текущей"}),
    )

    with pytest.raises(demo.PriceUnchanged):
        demo.patch_price("http://erp", "SKU-001", Decimal("900.00"), 1)
