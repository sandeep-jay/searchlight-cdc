# Migrations

SQL migration guidance for schema `boa_app_rds_direct`.

## SQL files

Run in order:

| File | Purpose |
|------|---------|
| `sql/001_schema.sql` | Create schema `boa_app_rds_direct` |
| `sql/002_tables.sql` | Create tables and views |
| `sql/003_indexes.sql` | B-tree and GIN indexes |

Placeholders `{rds_schema_boa_app_rds_data}` and `{rds_app_boa_user}` are substituted by `scripts/apply_sql.py` at apply time.

## Critical: 002_tables.sql drops tables

`002_tables.sql` begins with:

```sql
DROP TABLE IF EXISTS ... advising_notes_cdc_log CASCADE;
DROP TABLE IF EXISTS ... advising_notes_search_index CASCADE;
DROP TABLE IF EXISTS ... advising_note_topics_pending CASCADE;
DROP TABLE IF EXISTS ... advising_note_topics CASCADE;
DROP TABLE IF EXISTS ... advising_notes CASCADE;
```

### Safe usage

| Environment | Safe to run full `002_tables.sql`? |
|-------------|-------------------------------------|
| Empty local dev DB | Yes — use `make apply-sql` |
| RDS with existing advising data | **No** — destroys all data |

**Never run `make apply-sql` or `002_tables.sql` on production RDS if advising tables already exist.**

## Local development

For a fresh local database:

```bash
make apply-sql
```

This runs all three files in order. Equivalent:

```bash
python scripts/apply_sql.py
```

## Production / RDS strategy

For RDS where tables may already exist:

1. **Initial setup (empty schema):** Run `001_schema.sql`, then apply table DDL **without** the `DROP TABLE` statements (extract `CREATE TABLE` / `CREATE VIEW` only from `002_tables.sql`), then run `003_indexes.sql`.

2. **Schema already exists with data:** Do **not** re-run `002_tables.sql`. Apply incremental changes only:
   - New columns: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`
   - New indexes: `CREATE INDEX IF NOT EXISTS ...` (see `003_indexes.sql` for patterns)
   - New tables (e.g. CDC log added later): `CREATE TABLE IF NOT EXISTS ...`

3. **Bulk refresh (nightly export):** Reload data via your ETL/export pipeline into `advising_notes` and related tables. Do not drop tables. Replay CDC from `advising_notes_cdc_log` afterward — see [Operations — Full refresh](operations.md#full-refresh).

## Adding new migrations

If you extend the schema:

1. Add a new numbered file (e.g. `004_add_column.sql`) with idempotent DDL (`IF NOT EXISTS`)
2. **Do not** add `DROP TABLE` to production migration files
3. Test locally with `make apply-sql` on an empty DB, then test the incremental file against a DB that already has tables
4. Document the change in this file or a changelog

## Verify schema after migration

```sql
-- Tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'boa_app_rds_direct'
ORDER BY table_name;

-- Indexes on FTS
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'boa_app_rds_direct'
  AND tablename = 'advising_notes_search_index';
```

Expected tables: `advising_notes`, `advising_note_topics`, `advising_notes_search_index`, `advising_note_topics_pending`, `advising_notes_cdc_log`, plus `*_vw` views.
