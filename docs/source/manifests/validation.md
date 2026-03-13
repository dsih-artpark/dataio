# Manifest Validation

`DataIO Validate` is a local-first validation layer that uses the dataset manifest as its input document.

## What Gets Validated

Cross-cutting checks:

- required dataset metadata
- table and field completeness
- enum definition and reference resolution
- type declarations
- nullability and value constraints
- temporal context requirements for region-bearing data

Tabular checks:

- required tables and files exist
- required columns exist
- extra columns are reported
- row values match declared types

GeoJSON checks:

- GeoJSON root and feature structure
- feature geometry presence
- property validation through the same manifest type rules

## Result Model

Validation returns:

- status: `pass`, `warn`, or `fail`
- summary counts
- structured findings with severity, code, path, table, row, and field

This keeps CLI, SDK, and API behavior aligned.
