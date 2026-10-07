from __future__ import annotations

from dataio.validate.contracts.models import DatasetManifest
from dataio.validate.reports.models import Finding, ValidationResult

REGION_FIELD_NAMES = {
    "country.ID",
    "state.ID",
    "district.ID",
    "subdistrict.ID",
    "village.ID",
    "ulb.ID",
    "zone.ID",
    "ward.ID",
    "prabhag.ID",
}


def apply_cross_field_rules(manifest: DatasetManifest, result: ValidationResult) -> None:
    all_fields = [
        (field_name, field)
        for table in manifest.datasetTables.values()
        for field_name, field in table.dataDictionary.items()
    ]
    has_region_field = any(
        name in REGION_FIELD_NAMES or field.type == "regionID"
        for name, field in all_fields
    )
    has_temporal_year_field = any(
        field.type in {"date", "dateTime"} and field.format and "%Y" in field.format
        for _, field in all_fields
    )
    if has_region_field and not has_temporal_year_field:
        result.add_finding(
            Finding(
                severity="error",
                code="missing_temporal_context",
                message=(
                    "Region-bearing manifests must declare a temporal field "
                    "with a year component."
                ),
                path="datasetTables",
                rule_id="region_requires_year",
                hint=(
                    "Add a date/dateTime field using a strftime format that includes %Y, "
                    "for example %Y or %Y-%m-%d."
                ),
            )
        )
