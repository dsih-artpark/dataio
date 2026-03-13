from __future__ import annotations

import typer

from dataio.validate.reports.renderers import render_text_summary
from dataio.validate.sdk import DataIOValidator

app = typer.Typer(help="Validate dataset manifests and data files locally.")
TABLE_OPTION = typer.Option(
    None,
    "--table",
    help="Table mapping in the form table_name=/path/to/file.csv. Repeat for multiple tables.",
)
DEEP_CHECK_OPTION = typer.Option(
    False,
    "--deep-check",
    help="Run platform-backed checks via the DataIO API.",
)
OUTPUT_OPTION = typer.Option("text", "--output", help="Output format: text or json.")


@app.command("tabular")
def validate_tabular(
    manifest: str = typer.Option(..., "--manifest", help="Path to the manifest YAML file."),
    table: list[str] = TABLE_OPTION,
    deep_check: bool = DEEP_CHECK_OPTION,
    output: str = OUTPUT_OPTION,
):
    validator = DataIOValidator()
    data_files: dict[str, str] = {}
    for item in table or []:
        if "=" not in item:
            raise typer.BadParameter(
                "Each --table value must be in the form table_name=/path/to/file.csv"
            )
        table_name, path = item.split("=", 1)
        data_files[table_name] = path

    result = validator.validate_tabular(
        manifest=manifest,
        data_files=data_files,
        deep_check=deep_check,
    )
    if output == "json":
        typer.echo(result.model_dump_json(indent=2))
    else:
        typer.echo(render_text_summary(result))
    raise typer.Exit(0 if result.status != "fail" else 1)


@app.command("geojson")
def validate_geojson(
    manifest: str = typer.Option(..., "--manifest", help="Path to the manifest YAML file."),
    data: str = typer.Option(..., "--data", help="Path to the GeoJSON file."),
    deep_check: bool = DEEP_CHECK_OPTION,
    output: str = OUTPUT_OPTION,
):
    validator = DataIOValidator()
    result = validator.validate_geojson(manifest=manifest, data=data, deep_check=deep_check)
    if output == "json":
        typer.echo(result.model_dump_json(indent=2))
    else:
        typer.echo(render_text_summary(result))
    raise typer.Exit(0 if result.status != "fail" else 1)
