# Group Scholar Cohort Snapshotter

CLI to capture cohort health snapshots into Postgres and export a CSV report for program updates.

## Features
- Capture cohort snapshots with program, source, and notes metadata.
- Seed a roster of scholar engagement signals for immediate reporting.
- Export snapshot data into CSV for leadership updates.
- Uses a dedicated Postgres schema to avoid cross-project collisions.

## Tech
- Dart 3
- PostgreSQL (production)

## Setup
Install dependencies:

```bash
dart pub get
```

Set the required environment variables for the production database connection:

```
PGHOST=...
PGPORT=...
PGDATABASE=...
PGUSER=...
PGPASSWORD=...
```

Note: the CLI disables SSL in the Postgres connection settings to match the
production server configuration.

## Usage
Initialize the schema and tables:

```bash
dart run bin/groupscholar_cohort_snapshotter.dart init --schema cohort_snapshotter
```

Capture a snapshot (also inserts sample cohort members):

```bash
dart run bin/groupscholar_cohort_snapshotter.dart snapshot \\
  --schema cohort_snapshotter \\
  --program "Foundations" \\
  --source "airtable" \\
  --date 2026-02-08 \\
  --notes "Weekly cohort review"
```

Generate a CSV report:

```bash
dart run bin/groupscholar_cohort_snapshotter.dart report \\
  --schema cohort_snapshotter \\
  --date 2026-02-08 \\
  --out reports/cohort_snapshot_report.csv
```

Generate an aggregated summary report:

```bash
dart run bin/groupscholar_cohort_snapshotter.dart summary \\
  --schema cohort_snapshotter \\
  --date 2026-02-08 \\
  --stale-days 14 \\
  --out reports/cohort_summary_report.csv
```

Stream the summary report to stdout:

```bash
dart run bin/groupscholar_cohort_snapshotter.dart summary \\
  --schema cohort_snapshotter \\
  --date 2026-02-08 \\
  --out -
```

Generate a leadership brief (writes markdown and stores summary metrics):

```bash
dart run bin/groupscholar_cohort_snapshotter.dart brief \\
  --schema cohort_snapshotter \\
  --date 2026-02-08 \\
  --stale-days 14 \\
  --out reports/cohort_snapshot_brief.md
```

## Testing
```bash
dart test
```
