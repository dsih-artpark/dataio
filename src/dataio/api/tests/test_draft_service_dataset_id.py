from __future__ import annotations

from dataio.api.services.draft_service import (
    _build_dataset_slug,
    _resolve_and_reserve_raw_dataset_id,
    _resolve_dataset_id,
)


def test_build_dataset_slug_uses_llm_provided_words():
    assert _build_dataset_slug("CS0007DS0113", "consolidated-livestock-census", "ignored") == (
        "cs0007ds0113-consolidated-livestock-census"
    )


def test_build_dataset_slug_strips_llm_guessed_id_prefix():
    # The LLM was told not to prefix the slug with an ID, but if it does
    # anyway (possibly guessing wrong), the real one must still win.
    assert _build_dataset_slug("CS0007DS0113", "cs0001ds0099-consolidated-census", "ignored") == (
        "cs0007ds0113-consolidated-census"
    )


def test_build_dataset_slug_falls_back_to_title_when_slug_missing():
    assert _build_dataset_slug("CS0007DS0113", None, "Consolidated Livestock Census!") == (
        "cs0007ds0113-consolidated-livestock-census"
    )


def test_build_dataset_slug_falls_back_to_dataset_when_nothing_usable():
    assert _build_dataset_slug("CS0007DS0113", "", "") == "cs0007ds0113-dataset"


def test_resolve_dataset_id_returns_existing_id_unchanged(monkeypatch):
    def fail_if_called(*a, **kw):
        raise AssertionError("should not mint a new ID when one was already supplied")

    monkeypatch.setattr("dataio.api.services.draft_service.suggest_next_dataset_id", fail_if_called)
    monkeypatch.setattr("dataio.api.services.draft_service.create_reserved_dataset_id", fail_if_called)

    assert _resolve_dataset_id("CS0007DS0112", "CS0007", "engineer@artpark.in") == "CS0007DS0112"


def test_resolve_dataset_id_mints_and_reserves_when_none_given(monkeypatch):
    recorded = {}

    monkeypatch.setattr(
        "dataio.api.services.draft_service.suggest_next_dataset_id",
        lambda collection_id: "CS0007DS0999",
    )

    def fake_reserve(ds_id, collection_id, note, reserved_by):
        recorded.update(ds_id=ds_id, collection_id=collection_id, note=note, reserved_by=reserved_by)

    monkeypatch.setattr("dataio.api.services.draft_service.create_reserved_dataset_id", fake_reserve)

    result = _resolve_dataset_id(None, "CS0007", "engineer@artpark.in")

    assert result == "CS0007DS0999"
    assert recorded["ds_id"] == "CS0007DS0999"
    assert recorded["collection_id"] == "CS0007"
    assert recorded["reserved_by"] == "engineer@artpark.in"


def test_resolve_and_reserve_raw_dataset_id_returns_existing_id_unchanged(monkeypatch):
    def fail_if_called(*a, **kw):
        raise AssertionError("should not mint a new rds_id when one was already supplied")

    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category",
        fail_if_called,
    )
    monkeypatch.setattr(
        "dataio.api.services.draft_service.create_reserved_raw_dataset_id",
        fail_if_called,
    )

    result = _resolve_and_reserve_raw_dataset_id("CSRDS0016", "CS", "CS0007", "engineer@artpark.in")
    assert result == "CSRDS0016"


def test_resolve_and_reserve_raw_dataset_id_mints_and_reserves_when_none_given(monkeypatch):
    recorded = {}

    monkeypatch.setattr(
        "dataio.api.database.rds_id_helpers.suggest_next_raw_dataset_id_for_category",
        lambda category_id: "CSRDS0099",
    )

    def fake_reserve(rds_id, category_id, note, reserved_by):
        recorded.update(rds_id=rds_id, category_id=category_id, note=note, reserved_by=reserved_by)

    monkeypatch.setattr(
        "dataio.api.services.draft_service.create_reserved_raw_dataset_id",
        fake_reserve,
    )

    result = _resolve_and_reserve_raw_dataset_id(None, "CS", "CS0007", "engineer@artpark.in")

    assert result == "CSRDS0099"
    assert recorded["rds_id"] == "CSRDS0099"
    assert recorded["category_id"] == "CS"
    assert recorded["reserved_by"] == "engineer@artpark.in"
