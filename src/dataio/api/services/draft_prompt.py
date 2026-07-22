"""Builds the system/user prompt for the LLM metadata drafter, and parses
its response back into a manifest dict + a flags list. Encodes every
decided rule from the metadata-architecture memo (LLM = Claude via
OpenRouter, access_level not requested, dates best-effort not parsed,
source facts never invented, gap explanations from general knowledge into
comments, digitization-log-covered items never re-flagged).
"""

from __future__ import annotations

import yaml

from dataio.api.services.csv_profiler import CsvProfile
from dataio.api.services.digitization_log import DigitizationLog

# The exact, complete set of type keywords the validator recognizes -
# anything else is a hard validation error, so the LLM must be given this
# literal list rather than free-text examples it might paraphrase (e.g.
# "integer" instead of "int").
SUPPORTED_FIELD_TYPES = ("string", "boolean", "int", "float", "enum", "regionID", "regionName", "date", "dateTime")

# A real, trimmed metadata.yaml (condensed from the actual CS0007DS0112 and
# CS0026DS0111 datasets in data/) shown to the LLM verbatim as a structural
# template - not to copy the content, but so the exact skeleton (key names,
# nesting depth, list-vs-dict shape, the two-table convention) stays fixed
# across every generation instead of drifting run to run. Enum value lists
# and comments are trimmed to a representative few - real datasets are
# denser than this, per the "Categories to check" list under `comments` below.
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
canonicalEnumDefinitions:
  canonicalSpecies:
    description: 'Cross-dataset canonical species vocabulary. Resolve species series
      by this canonical key rather than dataset-local literal strings.'
    values:
      cattle:
        grain: leaf
      buffalo:
        grain: leaf
      bovine:
        grain: group
        components:
        - cattle
        - buffalo
enumDefinitions:
  livestockSpecies:
    description: Canonical livestock species vocabulary for this dataset.
    values:
      cattle:
        description: Domestic cattle. Includes both crossbred/exotic and indigenous
          breeds.
        canonical: cattle
      buffalo:
        description: Domestic water buffalo.
        canonical: buffalo
  livestockSourceTableID:
    description: Source table identifier within the livestock census publications.
    values:
      'Table 15R: Buffaloes Male Rural':
        description: 'Source table: Table 15R: Buffaloes Male Rural'
joinKeys:
- state.lgd_code
- state.ID
- year
- species
comments:
- Data covers cattle and buffalo only. Other species (sheep, goat, pig, etc.) are
  not included.
- Values are in actual animal counts (not thousands). Source PDFs report values in
  thousands; conversion to actual counts was applied during extraction.
- Telangana is absent from 1997-2012 data (it was part of Andhra Pradesh until 2014).
  It appears as a separate state from 2019 onwards.
- state.lgd_code stores the numeric LGD code extracted from state.ID for both states
  and union territories.
references:
- https://dahd.gov.in/sites/default/files/2019-12/16thLivestockCensusBook.pdf
tables:
  consolidated-livestock-census:
    description: 'State-level livestock population counts disaggregated by species,
      breed, sex, age group, utility, and locality across five census years (1997-2019).

      '
    source: 16th-20th Livestock Census PDFs (DAHD, Government of India)
    joinKeys:
    - state.lgd_code
    - state.ID
    - year
    - species
    data_dictionary:
      state.lgd_code:
        type: int
        description: 'Numeric LGD code extracted from state.ID for the state or union
          territory.

          '
        nullable: false
        isJoinKey: true
        joinKeyType: compositeComponent
      state.ID:
        type: regionID
        description: 'Prefixed LGD identifier for the state or union territory. Uses
          ''state_'' for states and ''ut_'' for union territories.

          '
        nullable: false
        isJoinKey: true
        joinKeyType: compositeComponent
      state.name:
        type: regionName
        description: State or union territory name standardised to LGD classification.
        nullable: false
      year:
        type: date
        format: '%Y'
        description: Census reference year.
        nullable: false
        isJoinKey: true
        joinKeyType: temporal
      species:
        type: enum
        description: Livestock species. Either 'cattle' or 'buffalo'.
        nullable: false
        isJoinKey: true
        joinKeyType: compositeComponent
        enumRef: livestockSpecies
      count:
        type: int
        description: 'Number of livestock of this species in the state for the given
          census year.

          '
        nullable: true
        additive: true
        aggregation: sum
      sourceTableID:
        type: enum
        description: Identifier of the specific table within the source PDF from which
          this row was extracted.
        nullable: true
        enumRef: livestockSourceTableID
  another-example-table-from-a-second-csv-in-this-dataset:
    description: 'Second table present ONLY when more than one CSV was uploaded for this
      dataset (e.g. a per-species yield table alongside a production table) - each
      uploaded CSV becomes its own entry here, named after that CSV''s own filename
      stem verbatim, not this placeholder name.

      '
    source: Same or a different source document, as appropriate for that table
    joinKeys:
    - state.lgd_code
    - state.ID
    - year
    data_dictionary:
      state.lgd_code:
        type: int
        description: Numeric LGD code, same convention as the first table.
        nullable: false
        isJoinKey: true
        joinKeyType: compositeComponent
"""

# This is the REAL, established metadata.yaml schema ("v2") used by every
# existing ARTPARK dataset - not the newer dataio.validate.contracts.models
# Pydantic contract, which uses different key names (datasetTables/
# dataDictionary). manifest_v2_conversion.py converts between the two for
# validation - the exact same conversion the platform's own dataset-import
# flow already uses.
MANIFEST_SCHEMA_HINT = f"""
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

FIXED TOP-LEVEL KEYS - every one of the following MUST appear in your MANIFEST output
(some may hold an empty list/dict when genuinely inapplicable, but the key itself must
never be omitted): datasetDescription, source, tags (with both concept and epiType),
spatialCoverage, spatialResolution, temporalCoverage, temporalResolution, updateFrequency,
joinKeys, comments, references, tables. (datasetTitle, datasetSlug, datasetID,
metadataSpecVersion, category, collection, datasetOwner, lastUpdated are also fixed keys,
but the system fills those in for you - see above.) enumDefinitions and
canonicalEnumDefinitions are conditionally-fixed: include enumDefinitions whenever any
column is typed enum (nearly always), omit canonicalEnumDefinitions only when no column's
values are worth rolling up cross-dataset. This is the exact same fixed-key set documented
in metadata_field_reference.md - do not invent additional top-level keys and do not drop any
of these.

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
joinKeys: list of str (required) - dotted column names that together form this dataset's
  composite key across its table(s). CRITICAL: whenever a `regionID`-typed column (e.g.
  `state.ID`, `district.ID`) is part of the join key, its paired LGD code column (e.g.
  `state.lgd_code`, `district.lgd_code`) MUST be included alongside it if that column exists
  in the data - every real dataset in this system pairs the two together (e.g.
  `["state.lgd_code", "state.ID", "year", ...]`), never the regionID alone.
canonicalEnumDefinitions: optional - only include this block if this dataset has an enum
  column whose values are worth rolling up to a broader, cross-dataset-joinable category
  (e.g. a `species` column with "cattle" and "buffalo" both being kinds of "bovine"), so
  that a curator can later join this dataset against others on the shared broader concept.
  ESTABLISHED REGISTRY - livestock species/breed: every real dataset in this system with a
  species or breed column uses this EXACT existing registry (ID "INL-98") - reuse it
  verbatim, do not invent a new block or ID for this concept:
    canonicalSpecies:
      description: 'Cross-dataset canonical species vocabulary (INL-98). Resolve species
        series by this canonical key rather than dataset-local literal strings.'
      values:
        cattle: {{grain: leaf}}
        buffalo: {{grain: leaf}}
        goat: {{grain: leaf}}
        sheep: {{grain: leaf}}
        pig: {{grain: leaf}}
        poultry: {{grain: leaf}}
        bovine: {{grain: group, components: [cattle, buffalo]}}
        ovine_and_other_mammals: {{grain: group, components: [sheep, goat]}}
        others: {{grain: residual}}
    canonicalBreed:
      description: 'Cross-dataset canonical breed vocabulary (INL-98). Census reports the
        combined grain (crossbred_exotic); milk reports the split grain (exotic,
        crossbred). For cross-dataset joins, match on canonicalRollup; for fine
        within-dataset analysis, use canonical.'
      values:
        crossbred_exotic: {{grain: coarse, components: [exotic, crossbred]}}
        exotic: {{grain: leaf, rollup: crossbred_exotic}}
        crossbred: {{grain: leaf, rollup: crossbred_exotic}}
        indigenous: {{grain: leaf}}
        non_descript: {{grain: leaf}}
        indigenous_non_descript: {{grain: coarse, components: [indigenous, non_descript]}}
        none: {{grain: na}}
        unspecified: {{grain: unknown}}
  Include only the species/breed values this dataset actually needs (a dataset with no
  breed column omits canonicalBreed entirely), but never rename the block, never rename a
  value, and never invent a different ID for this same concept.
  For any OTHER cross-dataset concept (not livestock species/breed): do NOT invent a registry ID
  for the block (e.g. do not make up something like "INL-99") - name the block descriptively
  instead and let the curator assign a real registry ID during review, since you have no way
  of knowing whether one already exists for that other concept.
    <canonicalBlockName>:
      description: str
      values:
        <leafValue>:
          grain: str - "leaf" for a value with no further breakdown
        <groupValue>:
          grain: str - "group" (or "coarse" for a rollup of leaf values) - a broader
            category this dataset's own enum values roll up into
          components: list of str - the sibling values (from this same block) that
            make up this group
        <valueThatRollsUp>:
          grain: leaf
          rollup: str - name of the group/coarse value (in this same block) that this
            leaf value belongs to, when relevant
enumDefinitions:
  <enumBlockName>:
    description: str
    values:
      <value>:
        description: str
comments: list of str (required) - one distinct, plainly-stated FACT or documented decision
  about the DATA per list item (NOT a single paragraph - do not join everything into one
  string, and NOT meta-commentary about your own confidence as the drafter - see rule 2).
  Real metadata.yaml files are dense with these; match that density rather than settling for
  one or two generic lines. Categories to check for on every dataset:
    - Geographic entity history: which states/UTs/districts are absent in which years and
      why (bifurcations, mergers, renamings), naming the specific years/dates involved.
    - Null-value semantics: what a null/blank in a specific column actually means (not
      reported vs. structurally not applicable vs. aggregated elsewhere).
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
    joinKeys: list of str - same composite key columns, scoped to this table
    data_dictionary:
      <columnName>:   # must exactly match the CSV's column header, verbatim
        type: str - MUST be exactly one of: {", ".join(SUPPORTED_FIELD_TYPES)}. No other
          value is accepted (e.g. "integer" is invalid - use "int").
        description: str
        nullable: bool
        isJoinKey: bool - true if this column is part of the composite key (omit otherwise)
        joinKeyType: str - "compositeComponent" or "temporal", present only when isJoinKey is true
        format: str - required for date/dateTime types, a strftime format (e.g. "%Y" for a
          bare calendar year, "%Y-%m-%d" for a full date)
        enumRef: str - name of an entry in enumDefinitions, for enum-type columns. STRONGLY
          prefer this over allowedValues - a real enumDefinitions block with a per-value
          description is far more useful to a curator than a bare list of values.
        allowedValues: list - only if enumRef genuinely doesn't apply. Either way, every
          distinct value this column actually takes in the data must be covered (see the
          "all distinct values" list per column below where provided) - a value present in
          the data but missing from the enum makes every row with that value fail validation.
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
2. If you notice a genuine gap in the data (a missing year, a missing region, etc.), explain it
   using your own general/historical/administrative knowledge (e.g. a region absent from before
   a certain year because of a state reorganization) as its own item in `comments`, stated
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
5. Any column representing a calendar year or date MUST be typed `date` (or `dateTime`) with a
   `format` containing `%Y` (e.g. `"%Y"` for a bare year column) - NOT `int` or `string`.
6. Only use type `regionID` for a column whose values already look like `<lowercase-word>_<code>`
   (e.g. `state_KA`). If the column is a plain code or name (a numeric LGD code, a state name,
   etc.) that doesn't already follow that exact shape, use `string` or `int` instead. When a
   `regionID` column is paired with a separate human-readable name column for the same place
   (e.g. `state.ID` and `state.name`), type that name column `regionName`, not `string`.
7. Whenever a `regionID` column (e.g. `state.ID`, `district.ID`) is part of the composite join
   key, and a matching LGD code column for that same entity exists in the data (e.g.
   `state.lgd_code`, `district.lgd_code`), that LGD code column MUST also be added to `joinKeys`
   (both at the top level and in the table's own `joinKeys`) and marked `isJoinKey: true` with
   `joinKeyType: compositeComponent` - never include the regionID alone. Every real dataset in
   this system pairs them this way (e.g. `state.lgd_code` always appears next to `state.ID`).
8. Prefer `enum` over `string`/`int` for ANY column with a small, closed set of distinct values
   in the data - not just the columns that are obviously categorical. This absolutely includes
   `sourceDocument` and `sourceTableID` when they only take a handful of distinct values (one
   source PDF per year, a fixed set of table titles, etc.) - declare a real `enumDefinitions`
   entry for them with a description per value, exactly like any other enum column, rather than
   leaving them as plain `string`. Check the "all distinct values" list per column below: if it's
   short, that column is very likely an enum candidate even if its name suggests free text.
9. Do not raise a flag or comment just to ask the curator to "verify" something that isn't
   actually a problem:
   - Do not question whether a code column (an LGD code, a state ID, etc.) matches some
     external official registry - take the CSV's values at face value.
   - Do not flag inconsistent formatting in a free-text/string-typed column (e.g. sourceTableID)
     as a caveat. Only raise a flag about a column's values if that column is typed `enum` and
     some of its actual values aren't covered by the enum you declared.
   - Do not flag a real-world name (a place name, an organization name, etc.) as "unusual" or
     "worth checking" just because it's long, verbose, or unfamiliar-looking. Long official
     names are common and are not evidence of a digitization error.
10. YAML syntax safety - your output is parsed by a strict YAML parser, not read by a human:
   double-quote (") any string value that contains a colon followed by a space (e.g. a table
   title like "Table 15R: Buffaloes Male Rural"), since an unquoted colon+space inside a plain
   scalar is parsed as a new mapping key and breaks the whole document. This applies to every
   free-text value: `description`, `comments` list items, `source`/`references` entries, enum
   value descriptions, flag reasons - anything that isn't a short bare keyword. When in doubt,
   double-quote the value. Never let a multi-line value span two physical lines without either
   quoting it onto one line or using a `|`/`>` block scalar.
11. Output ONLY two YAML blocks, in this exact order, and nothing else:
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


def build_prompt(
    *,
    csv_profiles: dict[str, CsvProfile],
    digitization_log: DigitizationLog | None,
    full_csv_texts: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Returns (system_prompt, user_prompt). `csv_profiles` maps table name
    (the uploaded CSV's filename stem) to its profile - one CSV upload per
    table, per the same convention the dataset-import flow already uses
    (see web_admin_service._parse_dataset_package's csv_by_stem). A single
    upload is just the len(csv_profiles) == 1 case, not a special path.

    Single LLM call, all tables at once - used when the dataset's combined
    full-CSV-text fits one call's context budget. Datasets too large for
    that use build_batch_prompt instead (see draft_service._batch_tables).
    """
    system_prompt = _build_system_prompt()

    full_csv_texts = full_csv_texts or {}
    sections = [f"This dataset has {len(csv_profiles)} table(s), one per uploaded CSV:"]
    for table_name, profile in csv_profiles.items():
        sections.append(_format_csv_profile(table_name, profile))
        full_csv_text = full_csv_texts.get(table_name)
        if full_csv_text is not None:
            sections.append(f"Full CSV contents for table '{table_name}':\n" + full_csv_text)

    sections.extend(_digitization_log_sections(digitization_log))

    user_prompt = "\n\n".join(sections)
    return system_prompt, user_prompt


def build_batch_prompt(
    *,
    csv_profiles: dict[str, CsvProfile],
    digitization_log: DigitizationLog | None,
    full_csv_texts: dict[str, str],
    batch_table_names: list[str],
    include_dataset_level_fields: bool,
) -> tuple[str, str]:
    """Same task rules/schema as build_prompt (the system prompt never
    changes per batch) - used when a dataset has too many/large CSVs for
    one call's context budget (see draft_service._batch_tables). Every
    table in the dataset is listed by name for cross-table context (so
    joinKeys/comments/tags stay consistent across calls), but full CSV
    content - and a `tables:` entry - is requested only for
    batch_table_names; every table's real content still reaches the LLM,
    just split across separate calls instead of ever being summarized away.

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
        "cross-table context (so your joinKeys/comments/tags stay consistent with the "
        "rest of the dataset) but are drafted in separate calls; do not invent a "
        "`tables:` entry for any table not in this list.",
    ]
    if include_dataset_level_fields:
        sections.append(
            "This is the FIRST call for this dataset - also produce the dataset-wide "
            "fields (datasetDescription, source, spatialCoverage, spatialResolution, "
            "temporalCoverage, temporalResolution, updateFrequency, references, "
            "canonicalEnumDefinitions) as normal, based on the tables shown to you here."
        )
    else:
        sections.append(
            "This is NOT the first call for this dataset - a separate call already "
            "produced datasetDescription, source, spatialCoverage, spatialResolution, "
            "temporalCoverage, temporalResolution, updateFrequency, and references. Omit "
            "those keys entirely from your MANIFEST output. You MAY still include "
            "joinKeys, tags, comments, and enumDefinitions/canonicalEnumDefinitions "
            "scoped to the tables shown to you here - these will be merged with the "
            "other calls' contributions afterward."
        )

    for table_name, profile in csv_profiles.items():
        if table_name in batch_set:
            sections.append(_format_csv_profile(table_name, profile))
            full_csv_text = full_csv_texts.get(table_name)
            if full_csv_text is not None:
                sections.append(f"Full CSV contents for table '{table_name}':\n" + full_csv_text)
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
