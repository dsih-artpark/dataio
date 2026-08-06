"""Builds the system/user prompt for the LLM metadata drafter, and parses
its response back into a manifest dict + a flags list. Encodes every
decided rule from the metadata-architecture memo (LLM = Claude via
OpenRouter, access_level not requested, dates best-effort not parsed,
source facts never invented, gap explanations from general knowledge into
comments, digitization-log-covered items never re-flagged).

Column typing (type/nullable/format/allowedValues/enumRef/isJoinKey/
joinKeyType), joinKeys, canonicalEnumDefinitions, and region-history
comments are all decided deterministically before the LLM is ever called
(see draft_service._infer_deterministic_base, field_inference.py) - the
LLM only drafts genuinely natural-language content: descriptions, enum
value/block definitions, dataset/table narrative fields, and comment
categories no rule can derive (null-value semantics, units, redundant
columns, completeness math, naming, source-to-year mapping, digitization
log items). It never sees a table's raw CSV content, only the bounded
per-column profile summary (stats + up to 200 distinct values + 20 sample
rows) - so prompt size no longer scales with a table's row count.
"""

from __future__ import annotations

import yaml

from dataio.api.services.csv_profiler import CsvProfile
from dataio.api.services.digitization_log import DigitizationLog

# A real, trimmed metadata.yaml (condensed from the actual CS0007DS0112
# dataset in data/) shown to the LLM verbatim as a structural template - not
# to copy the content, but so the exact skeleton (key names, nesting depth,
# list-vs-dict shape, the two-table convention) stays fixed across every
# generation instead of drifting run to run. Trimmed to only the fields the
# LLM is actually asked to produce (type/nullable/joinKeys/
# canonicalEnumDefinitions and most data_dictionary columns are decided
# deterministically - see module docstring - so they're absent here too).
REFERENCE_METADATA_EXAMPLE = """
datasetDescription: 'State-level livestock population counts from India''s 16th through
  20th Livestock Censuses (1997-2019), disaggregated by species, breed, sex, age group,
  utility, and locality. Each row represents the count of animals for a specific
  state x year x species x breed x sex x age.group x utility x locality combination.

  '
source:
- 16th Livestock Census 1997 - Department of Animal Husbandry, Dairying & Fisheries,
  Government of India
- 20th Livestock Census 2019 - Department of Animal Husbandry & Dairying, Government
  of India
tags:
  concept:
  - livestock
  - cattle
  - buffalo
  epiType:
  - population
spatialCoverage: India
spatialResolution: state
temporalCoverage: 1997, 2003, 2007, 2012, 2019
temporalResolution: quinquennial
updateFrequency: Quinquennial
enumDefinitions:
  livestockSpecies:
    description: Canonical livestock species vocabulary for this dataset.
    values:
      cattle:
        description: Domestic cattle. Includes both crossbred/exotic and indigenous
          breeds.
      buffalo:
        description: Domestic water buffalo.
comments:
- Values are in actual animal counts (not thousands). Source PDFs report values in
  thousands; conversion to actual counts was applied during extraction.
- Count is null for state x year x species combinations where no census enumeration
  occurred, not zero population - a blank means "not counted", not "counted as none".
references:
- https://dahd.gov.in/sites/default/files/2019-12/16thLivestockCensusBook.pdf
tables:
  consolidated-livestock-census:
    description: 'State-level livestock population counts disaggregated by species,
      breed, sex, age group, utility, and locality across five census years (1997-2019).

      '
    source: 16th-20th Livestock Census PDFs (DAHD, Government of India)
    data_dictionary:
      count:
        description: 'Number of livestock of this species in the state for the given
          census year.

          '
      species:
        description: Livestock species. Either 'cattle' or 'buffalo'.
  another-example-table-from-a-second-csv-in-this-dataset:
    description: 'Second table present ONLY when more than one CSV was uploaded for this
      dataset (e.g. a per-species yield table alongside a production table) - each
      uploaded CSV becomes its own entry here, named after that CSV''s own filename
      stem verbatim, not this placeholder name.

      '
    source: Same or a different source document, as appropriate for that table
    data_dictionary:
      indicator:
        description: Example column that still needs a description - most columns
          (region identifiers, source-provenance columns, year/date columns) already
          have a fixed or deterministically-typed description and won't appear here
          at all; only genuinely domain-specific columns do.
"""

# This is the REAL, established metadata.yaml schema ("v2") used by every
# existing ARTPARK dataset - not the newer dataio.validate.contracts.models
# Pydantic contract, which uses different key names (datasetTables/
# dataDictionary). manifest_v2_conversion.py converts between the two for
# validation - the exact same conversion the platform's own dataset-import
# flow already uses.
MANIFEST_SCHEMA_HINT = """
Produce a YAML document matching the real, established metadata.yaml schema (schema
version "v2") used across every existing ARTPARK dataset. Do NOT produce datasetID,
datasetSlug, metadataSpecVersion, category, collection, datasetOwner, or lastUpdated -
the system fills those in automatically after you respond, from information (the curator's
own selections) you don't have access to. datasetTitle: if only one CSV table is shown
below, also omit datasetTitle - the system uses that CSV's own filename verbatim, matching
the established single-table convention. If MORE THAN ONE CSV table is shown below,
INCLUDE datasetTitle yourself: a short kebab-case, dataset-level name describing the whole
collection of tables (not any single table's name), since with multiple tables there's no
one filename the system can safely default to.

ALSO do NOT produce joinKeys or canonicalEnumDefinitions at any level, and do NOT include
type, nullable, format, allowedValues, enumRef, isJoinKey, or joinKeyType inside any
data_dictionary column entry - the system already decided all of that deterministically
from the column statistics (shown below for context only) before calling you. Each table's
"columns needing a `description`" section below tells you exactly which columns still need
one and lists each one's already-decided type/enumRef as read-only context - do not restate
it, only provide `description`. A column not listed there already has a description; do not
add a data_dictionary entry for it at all.

FIXED TOP-LEVEL KEYS - every one of the following MUST appear in your MANIFEST output
(some may hold an empty list/dict when genuinely inapplicable, but the key itself must
never be omitted): datasetDescription, source, tags (with both concept and epiType),
spatialCoverage, spatialResolution, temporalCoverage, temporalResolution, updateFrequency,
comments, references, tables. (datasetTitle, datasetSlug, datasetID, metadataSpecVersion,
category, collection, datasetOwner, lastUpdated, joinKeys, and canonicalEnumDefinitions are
also fixed keys, but the system fills those in for you - see above.) enumDefinitions is
conditionally-fixed: include it whenever a table's "enum blocks needing definitions"
section below is non-empty, omit it entirely otherwise. This is the same fixed-key set
documented in metadata_field_reference.md - do not invent additional top-level keys and
do not drop any of the keys listed above.

datasetDescription: str (required) - narrative description of the dataset
source: list of str (required) - one citation per source document/report
tags: (required)
  concept: list of str - domain concepts this dataset relates to
  epiType: list of str - e.g. "population", "incidence", "mortality"
spatialCoverage: str (required) - e.g. "India"
spatialResolution: str (required) - e.g. "state", "district"
temporalCoverage: str (required) - free text describing the years/period covered, in
  whatever shape is natural for this data (e.g. "1997, 2003, 2007, 2012, 2019" for
  irregular census years, or "1950-2024" for a continuous range) - NOT split into
  separate start/end date fields.
temporalResolution: str (required) - free text, e.g. "quinquennial", "annual", "monthly"
updateFrequency: str (required) - e.g. "Quinquennial", "Annual", "One-time", "Adhoc"
enumDefinitions:
  <enumBlockName>:
    description: str
    values:
      <value>:
        description: str - explain what this specific value actually MEANS, not just
          restate the value literal (e.g. for value "crossbred/exotic": "Cattle/buffalo
          resulting from cross-breeding indigenous with exotic breeds, or purebred exotic
          stock" - not "crossbred/exotic animals").
comments: list of str (required) - one distinct, plainly-stated FACT or documented decision
  about the DATA per list item (NOT a single paragraph - do not join everything into one
  string, and NOT meta-commentary about your own confidence as the drafter - see rule 2).
  Real metadata.yaml files are dense with these; match that density rather than settling for
  one or two generic lines. Categories to check for on every dataset (region-history/
  bifurcation gaps are already covered automatically - do not duplicate those here):
    - Null-value semantics: what a null/blank in a specific column actually means (not
      reported vs. structurally not applicable vs. aggregated elsewhere) - reason only from
      the null-count/sample-values context given per column, not from data you can't see.
    - Year/date column definition: whether "year" is the calendar year, the starting or
      ending year of an agricultural/financial year, a vaccination year vs. a report year, etc.
    - Source-to-year mapping: when different year ranges were extracted from different
      source editions/reports/tables, document exactly which years came from which source.
    - Units and conventions: counts vs. thousands, absolute vs. percentage, any conversion
      applied during extraction.
    - Redundant/derived columns: note when one column's value is fully determined by another
      (e.g. species derivable from breed) and is kept only for filtering convenience.
    - Completeness: row-count math (e.g. states x categories = total rows) and whether the
      dataset is fully populated or has known gaps.
    - Naming/standardization: which naming convention was applied and why (e.g. LGD standard,
      post-2014 official renamings).
references: list of str (required) - source document URLs
tables: (required) - EXACTLY one entry per CSV table shown below, no more, no fewer. Use
  the table name given for each CSV (its filename's stem) verbatim as the key - do not
  rename, reformat, or invent a different table name.
  <tableName>:
    description: str
    source: str
    data_dictionary:
      <columnName>:   # ONLY for columns listed under this table's "columns needing a
                       # `description`" section below - omit every other column entirely
        description: str
"""

RULES = """
Rules you must follow exactly:
1. Source-citation facts - which document this came from, which table within it, which page -
   are expected to already be in the CSV, under whatever column names this dataset actually
   uses (naming varies: a PDF-table extraction might have "sourceTableID"/"sourcePage"; an
   Excel-derived dataset may have no per-row table/page concept at all). Do NOT invent a value
   for a concept the CSV doesn't actually contain a column for (see "Missing source concepts"
   below, detected by keyword against the real column names, not an exact-name check) - add an
   entry to the `flags` list instead of leaving it blank or making something up. If a concept is
   missing because it genuinely doesn't apply to how this dataset was digitized (e.g. no
   page-level reference for a single Excel workbook), say so in the flag rather than treating it
   as an error.
2. If you notice a genuine gap in the data not already covered by an automatic region-history
   comment (e.g. a missing year, a missing category due to a survey methodology change),
   explain it using your own general/domain knowledge as its own item in `comments`, stated
   plainly as documented fact - never fold it into `datasetDescription`. `comments` documents
   the data, not the drafting process: never write things like "I am not fully confident" or
   "please verify this explanation" inside a comment. If you are not confident in an
   explanation, leave the comment out and raise a `flags` entry for the curator instead - do
   not hedge inside `comments`.
3. Anything listed below under "Already explained by the digitization log" is a normalization
   step or observation the data engineer already investigated and resolved on purpose. Do not re-flag
   it as a fresh caveat or gap - if relevant, fold the engineer's own explanation into `comments`
   as its own item, prefixed with "[digitization log]".
4. Anything listed under "Flagged by the digitization log as unresolved" should still be
   surfaced - the engineer noticed it but did not resolve it.
5. Do not raise a flag or comment just to ask the curator to "verify" something that isn't
   actually a problem:
   - Do not question whether a code column (an LGD code, a state ID, etc.) matches some
     external official registry - take the CSV's values at face value.
   - Do not flag inconsistent formatting in a free-text/string-typed column (e.g. sourceTableID)
     as a caveat.
   - Do not flag a real-world name (a place name, an organization name, etc.) as "unusual" or
     "worth checking" just because it's long, verbose, or unfamiliar-looking. Long official
     names are common and are not evidence of a digitization error.
6. YAML syntax safety - your output is parsed by a strict YAML parser, not read by a human:
   double-quote (") any string value that contains a colon followed by a space (e.g. a table
   title like "Table 15R: Buffaloes Male Rural"), since an unquoted colon+space inside a plain
   scalar is parsed as a new mapping key and breaks the whole document. This applies to every
   free-text value: `description`, `comments` list items, `source`/`references` entries, enum
   value descriptions, flag reasons - anything that isn't a short bare keyword. When in doubt,
   double-quote the value. Never let a multi-line value span two physical lines without either
   quoting it onto one line or using a `|`/`>` block scalar.
7. Output ONLY two YAML blocks, in this exact order, and nothing else:
   ---MANIFEST---
   <the manifest YAML>
   ---FLAGS---
   flags:
     - field: <dotted path or column name>
       reason: <why this needs curator attention>
"""


def _format_csv_profile(table_name: str, profile: CsvProfile) -> str:
    lines = [f"## Table: {table_name}  (source file: {profile.path})", f"Row count: {profile.row_count}", "Columns:"]
    for col in profile.columns:
        lines.append(
            f"  - {col.name}: dtype={col.dtype}, nulls={col.null_count} "
            f"({col.null_fraction:.1%}), distinct={col.distinct_count}, "
            f"sample={col.sample_values[:5]}"
            + (f", range=[{col.min_value}, {col.max_value}]" if col.min_value is not None else "")
        )
        if col.all_distinct_values is not None:
            lines.append(f"      all distinct values ({len(col.all_distinct_values)}): {col.all_distinct_values}")
    if profile.missing_source_columns:
        lines.append(
            f"\nMissing source concepts (no column keyword-matched, checked across all "
            f"column names above): {profile.missing_source_columns}"
        )
    else:
        lines.append("\nMissing source concepts: none")
    lines.append("\nSample rows (CSV):")
    lines.append(profile.sample_rows_csv)
    return "\n".join(lines)


def _format_deterministic_context(table_name: str, table_base: dict) -> str:
    """Lists this table's columns that still need an LLM-authored
    `description` - a column with a fixed structural description (region
    identifiers, source-provenance columns - see
    field_inference.infer_fixed_column_description) is already filled in
    and omitted here entirely, so the LLM never wastes a turn re-describing
    it. Each listed column's already-decided `type`/`enumRef` (from
    field_inference.infer_column_type) is shown as read-only context to
    help write an accurate description - the LLM is told not to restate it.
    """
    lines = [f"Table '{table_name}' - columns needing a `description` (type already decided, do not restate it):"]
    any_needed = False
    for column_name, field in table_base["data_dictionary"].items():
        if field.get("description") is not None:
            continue
        any_needed = True
        type_info = f"type={field.get('type')}"
        if field.get("enumRef"):
            type_info += f", enumRef={field['enumRef']}"
        lines.append(f"  - {column_name}: {type_info}")
    if not any_needed:
        lines.append("  (none - every column in this table already has a fixed description)")
    return "\n".join(lines)


def _format_enum_context(table_base: dict, enum_definitions: dict) -> str:
    """Lists the enum blocks this table's columns actually use (derived
    from data_dictionary's own enumRef fields) that still need an
    LLM-authored block description and per-value definitions - the
    canonical/canonicalRollup linkage on each value (if any) is already
    resolved deterministically (field_inference.match_canonical_values) and
    not shown here, since the LLM isn't asked to reproduce it.
    """
    enum_refs = sorted(
        {field["enumRef"] for field in table_base["data_dictionary"].values() if field.get("type") == "enum"}
    )
    if not enum_refs:
        return ""
    lines = [
        "Enum blocks used by this table needing definitions - write a block-level "
        "`description` and one `description` per value (explain what each value "
        "actually MEANS, don't just restate the value literal):"
    ]
    for enum_ref in enum_refs:
        values = list(enum_definitions.get(enum_ref, {}).get("values", {}).keys())
        lines.append(f"  - {enum_ref}: values = {values}")
    return "\n".join(lines)


def estimate_table_context_size(table_name: str, profile: CsvProfile, table_base: dict | None = None) -> int:
    """Character length of this table's prompt section - used by
    draft_service._batch_tables to decide whether a dataset's tables fit in
    one LLM call. Bounded by CsvProfile's own caps (up to 200 distinct
    values, 20 sample rows) regardless of the table's real row count,
    unlike the old full-CSV-text sizing this replaced.
    """
    size = len(_format_csv_profile(table_name, profile))
    if table_base is not None:
        size += len(_format_deterministic_context(table_name, table_base))
    return size


def _build_system_prompt() -> str:
    return (
        "You are drafting a metadata.yaml manifest for a new ARTPARK dataset, "
        "for a human curator to review and approve before anything is uploaded. "
        "You are not the final decision-maker - flag anything you're unsure about "
        "rather than guessing silently.\n\n"
        + MANIFEST_SCHEMA_HINT
        + RULES
        + "\n\nReference example - a real (trimmed) metadata.yaml showing the exact "
        "structural skeleton to follow (key names, nesting, list-vs-dict shape). Do not "
        "reuse its subject matter/content, only its shape - the second table entry shown "
        "is a placeholder illustrating the two-CSVs-in-one-dataset case; include a second "
        "table only if more than one CSV was actually provided to you below:\n"
        + REFERENCE_METADATA_EXAMPLE
    )


def _digitization_log_sections(digitization_log: DigitizationLog | None) -> list[str]:
    already_explained = digitization_log.already_explained_summary() if digitization_log else ""
    needs_investigation = digitization_log.needs_investigation_summary() if digitization_log else ""
    source_documents = (
        yaml.safe_dump([d.model_dump(exclude_none=True) for d in digitization_log.sourceDocuments])
        if digitization_log and digitization_log.sourceDocuments
        else ""
    )

    sections = [
        "Already explained by the digitization log (do not re-flag these):\n"
        + (already_explained or "(none - no digitization log was provided, or nothing was marked expected)"),
        "Flagged by the digitization log as unresolved (still surface these):\n"
        + (needs_investigation or "(none)"),
    ]
    if source_documents:
        sections.append("Source documents recorded in the digitization log:\n" + source_documents)
    return sections


def _deterministic_context_sections(table_name: str, deterministic_base: dict | None) -> list[str]:
    if deterministic_base is None:
        return []
    table_base = deterministic_base.get("tables", {}).get(table_name)
    if table_base is None:
        return []
    sections = [_format_deterministic_context(table_name, table_base)]
    enum_context = _format_enum_context(table_base, deterministic_base.get("enum_definitions", {}))
    if enum_context:
        sections.append(enum_context)
    return sections


def build_prompt(
    *,
    csv_profiles: dict[str, CsvProfile],
    digitization_log: DigitizationLog | None,
    deterministic_base: dict | None = None,
) -> tuple[str, str]:
    """Returns (system_prompt, user_prompt). `csv_profiles` maps table name
    (the uploaded CSV's filename stem) to its profile - one CSV upload per
    table, per the same convention the dataset-import flow already uses
    (see web_admin_service._parse_dataset_package's csv_by_stem). A single
    upload is just the len(csv_profiles) == 1 case, not a special path.

    `deterministic_base` (see draft_service._infer_deterministic_base) is
    the deterministically-inferred type/joinKeys/canonicalEnumDefinitions
    skeleton - its per-table "columns needing a description"/"enum blocks
    needing definitions" context replaces raw CSV row data in the prompt
    entirely, so prompt size is bounded regardless of row count.

    Single LLM call, all tables at once - used when the dataset's combined
    prompt-context size fits one call's context budget. Datasets too large
    for that use build_batch_prompt instead (see draft_service._batch_tables).
    """
    system_prompt = _build_system_prompt()

    sections = [f"This dataset has {len(csv_profiles)} table(s), one per uploaded CSV:"]
    for table_name, profile in csv_profiles.items():
        sections.append(_format_csv_profile(table_name, profile))
        sections.extend(_deterministic_context_sections(table_name, deterministic_base))

    sections.extend(_digitization_log_sections(digitization_log))

    user_prompt = "\n\n".join(sections)
    return system_prompt, user_prompt


def build_batch_prompt(
    *,
    csv_profiles: dict[str, CsvProfile],
    digitization_log: DigitizationLog | None,
    deterministic_base: dict,
    batch_table_names: list[str],
    include_dataset_level_fields: bool,
) -> tuple[str, str]:
    """Same task rules/schema as build_prompt (the system prompt never
    changes per batch) - used when a dataset has too many/large tables for
    one call's context budget (see draft_service._batch_tables). Every
    table in the dataset is listed by name for cross-table context (so
    comments/tags stay consistent across calls), but a `tables:` entry -
    and per-table description/enum-definition context - is requested only
    for batch_table_names.

    Dataset-wide narrative fields (datasetDescription, source, etc.) are
    requested only when include_dataset_level_fields is True - normally
    just the first batch - so the results from every batch can be merged
    into one manifest afterward without conflicting narratives (see
    draft_service._merge_batch_manifests).
    """
    system_prompt = _build_system_prompt()

    all_table_names = list(csv_profiles.keys())
    batch_set = set(batch_table_names)
    sections = [
        f"This dataset has {len(all_table_names)} table(s) total: {', '.join(all_table_names)}.",
        f"This call covers {len(batch_table_names)} of them: {', '.join(batch_table_names)}. "
        "Produce a `tables:` entry ONLY for these - the rest are listed below for "
        "cross-table context (so your comments/tags stay consistent with the rest "
        "of the dataset) but are drafted in separate calls; do not invent a "
        "`tables:` entry for any table not in this list.",
    ]
    if include_dataset_level_fields:
        sections.append(
            "This is the FIRST call for this dataset - also produce the dataset-wide "
            "fields (datasetDescription, source, spatialCoverage, spatialResolution, "
            "temporalCoverage, temporalResolution, updateFrequency, references) as "
            "normal, based on the tables shown to you here."
        )
    else:
        sections.append(
            "This is NOT the first call for this dataset - a separate call already "
            "produced datasetDescription, source, spatialCoverage, spatialResolution, "
            "temporalCoverage, temporalResolution, updateFrequency, and references. Omit "
            "those keys entirely from your MANIFEST output. You MAY still include tags "
            "and comments scoped to the tables shown to you here - these will be merged "
            "with the other calls' contributions afterward."
        )

    for table_name, profile in csv_profiles.items():
        if table_name in batch_set:
            sections.append(_format_csv_profile(table_name, profile))
            sections.extend(_deterministic_context_sections(table_name, deterministic_base))
        else:
            column_names = [col.name for col in profile.columns]
            sections.append(
                f"## Table: {table_name} (context only, drafted in a separate call - "
                f"columns: {column_names})"
            )

    sections.extend(_digitization_log_sections(digitization_log))

    user_prompt = "\n\n".join(sections)
    return system_prompt, user_prompt


def parse_llm_output(text: str) -> tuple[dict, list[dict]]:
    """Parses the ---MANIFEST--- / ---FLAGS--- delimited response into
    (manifest_dict, flags). Raises ValueError on malformed output (missing
    delimiters, non-mapping YAML, or YAML the LLM produced that doesn't
    even parse) so the caller can retry once with a corrective follow-up turn.
    """
    if "---MANIFEST---" not in text or "---FLAGS---" not in text:
        raise ValueError("LLM output missing required ---MANIFEST--- / ---FLAGS--- delimiters")

    _, rest = text.split("---MANIFEST---", 1)
    manifest_text, flags_text = rest.split("---FLAGS---", 1)

    try:
        manifest_dict = yaml.safe_load(manifest_text)
    except yaml.YAMLError as exc:
        raise ValueError(f"Manifest block is not valid YAML: {exc}") from exc
    if not isinstance(manifest_dict, dict):
        raise ValueError("Parsed manifest is not a YAML mapping")

    try:
        flags_doc = yaml.safe_load(flags_text) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Flags block is not valid YAML: {exc}") from exc
    flags = flags_doc.get("flags", []) if isinstance(flags_doc, dict) else []

    return manifest_dict, flags
