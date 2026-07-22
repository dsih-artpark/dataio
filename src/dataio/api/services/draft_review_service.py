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
        """Regenerates the whole draft from the same inputs (CSVs,
        digitization log, category/collection), superseding the original.
        Scoping regeneration to a single field is a further refinement not
        implemented here - this re-runs the full drafting pass.
        """
        from dataio.api.services.draft_service import decode_csv_paths, generate_draft

        original = self._get_draft_or_404(draft_id)
        table_names = list(original.draft_json.get("tables", {}).keys())
        csv_paths_by_table = decode_csv_paths(original.source_csv_path, table_names)
        new_draft = generate_draft(
            csv_paths=list(csv_paths_by_table.values()),
            category_id=original.category_id,
            collection_id=original.collection_id,
            created_by=reviewed_by,
            data_owner_name=original.draft_json.get("datasetOwner", ""),
            dataset_id=original.dataset_id,
            digitization_log_path=original.digitization_log_path,
            superseded_by_draft_id=str(original.draft_id),
            # Reuse the same reserved rds_id rather than reserving a second
            # one - this is a redraft of the same dataset, not a new one.
            raw_dataset_id=original.raw_dataset_id,
        )
        database.update_manifest_draft_status(draft_id, "rejected", reviewed_by=reviewed_by)
        return new_draft.model_dump()
