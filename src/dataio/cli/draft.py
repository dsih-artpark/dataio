from __future__ import annotations

from typing import List

import typer

app = typer.Typer(
    help="Draft dataset metadata.yaml from a raw CSV using an LLM, for curator review. "
    "Requires the 'api' extras group (pip install \"dataio-artpark[api]\") plus DB and "
    "OPENROUTER_API_KEY env vars - this is an internal ops tool, not part of the public SDK."
)


@app.command("generate")
def generate(
    csv: List[str] = typer.Option(
        ..., "--csv", help="Path to a raw CSV file. Repeat --csv once per table for a multi-table dataset."
    ),
    category_id: str = typer.Option(..., "--category-id", help='e.g. "CS".'),
    collection_id: str = typer.Option(..., "--collection-id", help='e.g. "CS0007".'),
    data_owner_name: str = typer.Option(..., "--data-owner-name", help='e.g. "DAHD".'),
    created_by: str = typer.Option(..., "--created-by", help="Email of the requesting user."),
    dataset_id: str = typer.Option(None, "--dataset-id", help="Existing ds_id, if updating an existing dataset."),
    digitization_log: str = typer.Option(None, "--digitization-log", help="Path to digitization_log.yaml."),
):
    # Lazy import: this pulls in dataio.api.database (and pandas), neither of
    # which ship in the public dataio-artpark wheel/sdist. Importing it at
    # module top-level would break `dataio` for anyone who installed the
    # public package without the `api` extras, since cli.py imports every
    # registered sub-app's module eagerly.
    from dataio.api.services.draft_service import generate_draft

    draft = generate_draft(
        csv_paths=csv,
        category_id=category_id,
        collection_id=collection_id,
        data_owner_name=data_owner_name,
        created_by=created_by,
        dataset_id=dataset_id,
        digitization_log_path=digitization_log,
    )
    typer.echo(f"Draft created: {draft.draft_id} (status={draft.status}, validation={draft.validation_status})")
    if draft.flagged_fields:
        typer.echo("Flagged fields:")
        for flag in draft.flagged_fields:
            typer.echo(f"  - {flag.get('field')}: {flag.get('reason')}")
    raise typer.Exit(0)
