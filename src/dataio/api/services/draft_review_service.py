"""Curator-facing review gate for LLM-drafted metadata.yaml. This tool only
generates and validates a draft for download right now - it does not
upload/persist anything to S3 or Postgres. "Approve" just marks the draft
as accepted; the draft's dataset_id and raw_dataset_id were already
reserved at generation time (see draft_service.generate_draft) and stay
reserved (visible under the admin "Reserved IDs" tab) until some future
import/upload process consumes them - create_dataset/create_raw_dataset
already release a matching reservation automatically when that happens.
Both reject_draft and delete_draft below release any reservation that
hasn't been consumed yet, since neither leaves the draft as something that
will ever go on to consume it itself.
"""

from __future__ import annotations

import datetime
import os

import yaml
from fastapi import HTTPException, UploadFile

from dataio.api.database import functions as database
from dataio.api.database.functions import (
    check_if_raw_dataset_exists,
    delete_reserved_dataset_id,
    delete_reserved_raw_dataset_id,
)
from dataio.api.services.base_service import BaseService
from dataio.api.services.draft_upload_storage import save_upload
from dataio.api.services.manifest_v2_conversion import convert_v2_manifest_to_contract
from dataio.validate.sdk import DataIOValidator


def _draft_to_dict(draft, *, dataset_exists: bool | None = None) -> dict:
    return {
        "draft_id": str(draft.draft_id),
        "dataset_id": draft.dataset_id,
        # Whether draft.dataset_id (a reserved ID, set at generation time)
        # already corresponds to a real Dataset row. A draft always has a
        # dataset_id now, so the frontend can't tell "brand new" from
        # "existing" just by checking for null anymore - it needs this.
        "dataset_exists": dataset_exists,
        "collection_id": draft.collection_id,
        "category_id": draft.category_id,
        "source_csv_path": draft.source_csv_path,
        "digitization_log_path": draft.digitization_log_path,
        "status": draft.status.value,
        "draft_yaml": draft.draft_yaml,
        "draft_json": draft.draft_json,
        "flagged_fields": draft.flagged_fields,
        "reviewer_notes": draft.reviewer_notes,
        "validation_result": draft.validation_result,
        "llm_model_id": draft.llm_model_id,
        "created_by": draft.created_by,
        "created_at": draft.created_at.isoformat() if draft.created_at else None,
        "reviewed_by": draft.reviewed_by,
        "reviewed_at": draft.reviewed_at.isoformat() if draft.reviewed_at else None,
        "superseded_by_draft_id": str(draft.superseded_by_draft_id) if draft.superseded_by_draft_id else None,
    }


def _stringify_dates(value):
    """yaml.safe_load implicitly parses an ISO-8601-looking scalar (e.g. a
    temporalCoverage value like "2019-06-30") into a datetime.date/datetime
    object - every manifest field is a plain string everywhere else in the
    app (CuratorMetadataInput, field_inference, etc. never produce a real
    date object), and the JSONB column's json serializer has no idea how to
    write one out, so a curator-edited draft containing one fails to save.
    Round-trips it back to the plain-string contract the rest of the app
    expects.
    """
    if isinstance(value, dict):
        return {k: _stringify_dates(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_stringify_dates(v) for v in value]
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


class DraftReviewService(BaseService):
    def _get_draft_or_404(self, draft_id: str):
        draft = database.get_manifest_draft(draft_id)
        if not draft:
            raise HTTPException(status_code=404, detail=f"Manifest draft {draft_id} not found")
        return draft

    def list_drafts(self, status: str | None = None, dataset_id: str | None = None, limit: int = 50, offset: int = 0):
        drafts, total = database.list_manifest_drafts(status=status, dataset_id=dataset_id, limit=limit, offset=offset)
        return {"drafts": [_draft_to_dict(d) for d in drafts], "total": total}

    def get_draft(self, draft_id: str) -> dict:
        draft = self._get_draft_or_404(draft_id)
        dataset_exists = database.check_if_dataset_exists(draft.dataset_id) if draft.dataset_id else False
        return _draft_to_dict(draft, dataset_exists=dataset_exists)

    def generate_draft_from_upload(
        self,
        *,
        csv_files: list[UploadFile],
        category_id: str,
        collection_id: str,
        created_by: str,
        data_owner_name: str,
        dataset_id: str | None = None,
        digitization_log_file: UploadFile | None = None,
    ) -> dict:
        """Backs the web "Generate draft" form: persists the uploaded CSVs
        (and optional digitization log) to local disk - not S3, since a
        draft is by definition pre-upload - then runs the same
        generate_draft() the CLI uses. One or more CSVs may be uploaded;
        each becomes its own table in the drafted manifest.
        """
        from dataio.api.services.draft_service import generate_draft

        csv_paths = [save_upload(f) for f in csv_files]
        digitization_log_path = save_upload(digitization_log_file) if digitization_log_file else None

        try:
            draft = generate_draft(
                csv_paths=csv_paths,
                category_id=category_id,
                collection_id=collection_id,
                created_by=created_by,
                data_owner_name=data_owner_name,
                dataset_id=dataset_id,
                digitization_log_path=digitization_log_path,
            )
        except Exception as e:
            self.logger.error(f"Failed to generate manifest draft: {e!s}")
            raise HTTPException(status_code=502, detail=f"Draft generation failed: {e}") from e

        return draft.model_dump()

    def generate_deterministic_draft_from_upload(
        self,
        *,
        csv_files: list[UploadFile],
        category_id: str,
        collection_id: str,
        created_by: str,
        data_owner_name: str,
        curator_input: dict,
        dataset_id: str | None = None,
    ) -> dict:
        """Backs the web "Generate deterministic draft" form - the no-LLM
        self-service path agreed with Lijith (2026-07-24 Data-Platform
        Discussion). Same upload-then-draft pattern as
        generate_draft_from_upload, but curator_input (the structured
        fields a human must supply - see CuratorMetadataInput) takes the
        place of a digitization log file, and there's no LLM call to make.
        """
        from pydantic import ValidationError

        from dataio.api.services.deterministic_draft_service import (
            CuratorMetadataInput,
            generate_deterministic_draft,
        )

        try:
            parsed_curator_input = CuratorMetadataInput(**curator_input)
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=f"Invalid curator_input: {e}") from e

        csv_paths = [save_upload(f) for f in csv_files]

        try:
            draft = generate_deterministic_draft(
                csv_paths=csv_paths,
                category_id=category_id,
                collection_id=collection_id,
                created_by=created_by,
                data_owner_name=data_owner_name,
                curator_input=parsed_curator_input,
                dataset_id=dataset_id,
            )
        except Exception as e:
            self.logger.error(f"Failed to generate deterministic manifest draft: {e!s}")
            raise HTTPException(status_code=502, detail=f"Draft generation failed: {e}") from e

        return draft.model_dump()

    def classify_columns(self, *, column_names: list[str]) -> dict:
        """Backs the intake form's dynamic per-column description prompts:
        splits column_names into "fixed" (auto-filled by
        field_inference.infer_fixed_column_description - never prompted)
        and "needsDescription" (everything else - the curator must supply
        one, see deterministic_draft_service.CuratorMetadataInput.
        columnDescriptions). No CSV read, no LLM call - the same rule both
        this endpoint and generate_deterministic_draft use, so the intake
        form and the actual validation can never disagree about which
        columns require a description. Classification only depends on each
        column's own name, not which table it's in, so no table_name here.
        """
        from dataio.api.services.field_inference import infer_fixed_column_description

        fixed = [name for name in column_names if infer_fixed_column_description(name) is not None]
        needs_description = [name for name in column_names if name not in fixed]
        return {"fixed": fixed, "needsDescription": needs_description}

    def revalidate_draft(self, draft_id: str) -> dict:
        """Re-runs the same conversion + existing-DataIOValidator check
        generate_draft() used, in case the CSV(s) or the manifest have
        changed since the draft was created (e.g. after a manual edit).
        """
        from dataio.api.services.draft_service import decode_csv_paths

        draft = self._get_draft_or_404(draft_id)
        contract_manifest = convert_v2_manifest_to_contract(draft.draft_json)
        manifest_yaml = yaml.safe_dump(contract_manifest, sort_keys=False, allow_unicode=True)
        table_names = list(contract_manifest.get("datasetTables", {}).keys())
        csv_paths_by_table = decode_csv_paths(draft.source_csv_path, table_names)
        data_files = {name: csv_paths_by_table[name] for name in table_names if name in csv_paths_by_table}
        result = DataIOValidator().validate_tabular(
            manifest=manifest_yaml, data_files=data_files, deep_check=False, full_scan=True,
        )
        updated = database.update_manifest_draft_status(
            draft_id, draft.status.value, validation_result=result.model_dump(),
        )
        return _draft_to_dict(updated)

    def update_draft_content(self, draft_id: str, draft_yaml: str) -> dict:
        """Persists curator-edited manifest YAML (the Draft Review screen's
        inline editor), re-validating against the same real CSVs
        revalidate_draft uses. Only allowed while a draft is still under
        review - an approved/rejected draft is final, and its ids may
        already be in use elsewhere.
        """
        from dataio.api.services.draft_service import _reorder_manifest_keys, decode_csv_paths

        draft = self._get_draft_or_404(draft_id)
        if draft.status.value in ("approved", "rejected"):
            raise HTTPException(
                status_code=400, detail=f"Cannot edit a draft that is already {draft.status.value}."
            )

        try:
            draft_json = _stringify_dates(yaml.safe_load(draft_yaml))
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=400, detail=f"Not valid YAML: {exc}") from exc
        if not isinstance(draft_json, dict):
            raise HTTPException(status_code=400, detail="Manifest must be a YAML mapping.")

        table_names = list((draft_json.get("tables") or {}).keys())
        csv_paths_by_table = decode_csv_paths(draft.source_csv_path, table_names)

        # Re-canonicalize key/column order on every save, not just at
        # generation time (_finalize_draft) - a full-manifest submission
        # (the Apply numeric-settings action, a raw YAML edit) round-trips
        # draft_json through the DB's JSONB column, which does not preserve
        # object key order at any nesting depth, so without this the saved
        # draft_yaml would drift away from the hand-authored ordering every
        # real metadata.yaml uses (see CANONICAL_KEY_ORDER).
        draft_json = _reorder_manifest_keys(draft_json, csv_paths_by_table)
        draft_yaml = yaml.safe_dump(draft_json, sort_keys=False, allow_unicode=True)

        contract_manifest = convert_v2_manifest_to_contract(draft_json)
        manifest_yaml = yaml.safe_dump(contract_manifest, sort_keys=False, allow_unicode=True)
        data_files = {
            name: csv_paths_by_table[name] for name in table_names if name in csv_paths_by_table
        }
        result = DataIOValidator().validate_tabular(
            manifest=manifest_yaml, data_files=data_files, deep_check=False, full_scan=True,
        )

        updated = database.update_manifest_draft_content(
            draft_id,
            draft_yaml=draft_yaml,
            draft_json=draft_json,
            validation_result=result.model_dump(),
        )
        return _draft_to_dict(updated)

    def _release_reservations(self, draft) -> None:
        """Releases draft.dataset_id and draft.raw_dataset_id, but only the
        ones that are still just reservations (no real Dataset/RawDataset
        row exists yet) - a draft whose ids were already consumed by real
        rows must not have those rows' ids touched. Shared by delete_draft
        and reject_draft: neither leaves the draft in a state that will
        ever go on to consume the reservation itself, so both must free it
        up rather than let it sit unused forever.
        """
        if draft.dataset_id and not database.check_if_dataset_exists(draft.dataset_id):
            try:
                delete_reserved_dataset_id(draft.dataset_id)
            except ValueError:
                pass  # already released, or never actually reserved - fine either way
        if draft.raw_dataset_id and not check_if_raw_dataset_exists(draft.raw_dataset_id):
            try:
                delete_reserved_raw_dataset_id(draft.raw_dataset_id)
            except ValueError:
                pass

    def _delete_uploaded_files(self, draft) -> None:
        """Best-effort cleanup of the CSV(s)/digitization log save_upload()
        wrote to local disk for this draft - delete_manifest_draft only
        removes the DB row, so without this every generated-then-deleted
        draft would leak its uploaded files on disk indefinitely.
        """
        from dataio.api.services.draft_service import decode_csv_paths

        table_names = list((draft.draft_json or {}).get("tables", {}).keys())
        paths = list(decode_csv_paths(draft.source_csv_path, table_names).values())
        if draft.digitization_log_path:
            paths.append(draft.digitization_log_path)
        for path in paths:
            try:
                os.remove(path)
            except OSError:
                pass  # already gone, or never existed - not worth failing the delete over

    def delete_draft(self, draft_id: str) -> None:
        """Removes a draft row outright. Never touches anything already
        approved and persisted - approval writes through the existing
        filestore/datasets path, entirely separate from this table.

        Also releases any reservation (dataset_id and/or raw_dataset_id)
        this draft was the only thing holding, and cleans up its uploaded
        source files - otherwise a deleted, unwanted draft would
        permanently squat on those IDs and leak disk space.
        """
        draft = self._get_draft_or_404(draft_id)
        self._release_reservations(draft)
        self._delete_uploaded_files(draft)
        database.delete_manifest_draft(draft_id)

    def approve_draft(self, draft_id: str, reviewed_by: str) -> dict:
        """Marks a draft as accepted. Does not upload or persist anything -
        this tool only generates and validates a draft for download right
        now. The dataset_id and raw_dataset_id were already reserved at
        generation time and simply stay reserved; nothing further needs to
        happen here for them to show up under "Reserved IDs".
        """
        self._get_draft_or_404(draft_id)
        updated = database.update_manifest_draft_status(draft_id, "approved", reviewed_by=reviewed_by)
        return _draft_to_dict(updated)

    def reject_draft(self, draft_id: str, reviewed_by: str, reason: str | None = None) -> dict:
        """Marks a draft as rejected and releases any reservation it was
        holding - a rejected draft (unlike one mid-regeneration, see
        regenerate_draft) is not going to go on to consume that id itself,
        so holding onto it would permanently and pointlessly skip that
        number in the global/category counter.
        """
        draft = self._get_draft_or_404(draft_id)
        if reason:
            database.append_manifest_draft_note(draft_id, {"note": reason, "by": reviewed_by})
        self._release_reservations(draft)
        updated = database.update_manifest_draft_status(draft_id, "rejected", reviewed_by=reviewed_by)
        return _draft_to_dict(updated)

    def flag_field(self, draft_id: str, field_path: str, note: str, reviewed_by: str) -> dict:
        self._get_draft_or_404(draft_id)
        updated = database.flag_manifest_draft_field(draft_id, field_path, note, reviewed_by)
        return _draft_to_dict(updated)

    def regenerate_draft(self, draft_id: str, reviewed_by: str) -> dict:
        """Regenerates the whole draft from the same inputs, superseding the
        original. Scoping regeneration to a single field is a further
        refinement not implemented here - this re-runs the full drafting
        pass. Dispatches on llm_model_id (None means the original was a
        deterministic draft - see generate_deterministic_draft_from_upload)
        since the two paths need different regeneration inputs (a
        digitization log file vs. a reconstructed CuratorMetadataInput).
        """
        from dataio.api.services.draft_service import decode_csv_paths, generate_draft

        original = self._get_draft_or_404(draft_id)
        table_names = list(original.draft_json.get("tables", {}).keys())
        csv_paths_by_table = decode_csv_paths(original.source_csv_path, table_names)

        if original.llm_model_id is None:
            new_draft = self._regenerate_deterministic_draft(
                original, csv_paths_by_table, reviewed_by
            )
        else:
            new_draft = generate_draft(
                csv_paths=list(csv_paths_by_table.values()),
                category_id=original.category_id,
                collection_id=original.collection_id,
                created_by=reviewed_by,
                data_owner_name=original.draft_json.get("datasetOwner", ""),
                dataset_id=original.dataset_id,
                digitization_log_path=original.digitization_log_path,
                superseded_by_draft_id=str(original.draft_id),
                # Reuse the same reserved rds_id rather than reserving a
                # second one - this is a redraft of the same dataset, not a
                # new one.
                raw_dataset_id=original.raw_dataset_id,
            )
        database.update_manifest_draft_status(draft_id, "rejected", reviewed_by=reviewed_by)
        return new_draft.model_dump()

    def _regenerate_deterministic_draft(self, original, csv_paths_by_table: dict, reviewed_by: str):
        """Reconstructs a CuratorMetadataInput from the original draft's own
        draft_json (its top-level curator-owned fields round-trip verbatim)
        and re-runs generate_deterministic_draft against the same CSVs -
        the deterministic equivalent of the LLM path's full re-drafting
        pass. Regenerating with unchanged input reproduces the same output
        by design (no randomness to re-roll); it only differs from the
        original if the curator edits fields first via a future intake-form
        "regenerate with edits" flow.

        Region-history comments (prefixed "[region history]", see
        region_gap_detector.py) are excluded from the round-tripped
        `comments` list since generate_deterministic_draft recomputes and
        re-appends them fresh from region_history.yaml - carrying the old
        ones forward would duplicate every one of them.
        """
        from dataio.api.services.deterministic_draft_service import (
            CuratorMetadataInput,
            TagsInput,
            generate_deterministic_draft,
        )
        from dataio.api.services.field_inference import infer_fixed_column_description

        manifest = original.draft_json
        tags = manifest.get("tags") or {}
        curator_input = CuratorMetadataInput(
            datasetDescription=manifest.get("datasetDescription", ""),
            source=manifest.get("source", []),
            references=manifest.get("references", []),
            tags=TagsInput(concept=tags.get("concept", []), epiType=tags.get("epiType", [])),
            spatialCoverage=manifest.get("spatialCoverage", ""),
            spatialResolution=manifest.get("spatialResolution", ""),
            temporalCoverage=manifest.get("temporalCoverage", ""),
            temporalResolution=manifest.get("temporalResolution", ""),
            updateFrequency=manifest.get("updateFrequency", ""),
            comments=[
                c for c in manifest.get("comments", []) if not c.startswith("[region history]")
            ],
            joinKeyColumns=manifest.get("joinKeys", []),
            tableDescriptions={
                name: t.get("description", "") for name, t in (manifest.get("tables") or {}).items()
            },
            # Fixed-pattern columns (region identifiers, source/provenance)
            # regenerate fresh from infer_fixed_column_description - only
            # curator-supplied ones need to round-trip.
            columnDescriptions={
                table_name: {
                    column_name: field.get("description", "")
                    for column_name, field in (table.get("data_dictionary") or {}).items()
                    if infer_fixed_column_description(column_name) is None
                }
                for table_name, table in (manifest.get("tables") or {}).items()
            },
            datasetTitle=manifest.get("datasetTitle", ""),
        )
        return generate_deterministic_draft(
            csv_paths=list(csv_paths_by_table.values()),
            category_id=original.category_id,
            collection_id=original.collection_id,
            created_by=reviewed_by,
            data_owner_name=manifest.get("datasetOwner", ""),
            curator_input=curator_input,
            dataset_id=original.dataset_id,
            superseded_by_draft_id=str(original.draft_id),
            raw_dataset_id=original.raw_dataset_id,
        )
