# Language-Specific Table Separation Implementation

## Overview
This project has been modified to use separate database tables for English (EN) and Spanish (ES) data. All tables now have language-specific suffixes (`_es` or `_en`), allowing independent tracking and avoiding mixed-language data.

## Database Changes

### Modified File: `db.py`

#### New Features
- **Language Parameter**: Added `language` parameter to `init_db()` function (defaults to `"es"`)
- **Global Language Tracking**: New `_DB_LANGUAGE` global variable tracks current language
- **Table Name Helper**: New `_get_table_name()` function dynamically generates language-specific table names
- **Separate Table Creation**: New `create_tables(language)` function creates tables with language suffix

#### Table Naming Convention
All 7 tables now use language suffixes:
- `search_runs_es` / `search_runs_en`
- `videos_raw_es` / `videos_raw_en`
- `videos_normalized_es` / `videos_normalized_en`
- `channels_raw_es` / `channels_raw_en`
- `channel_videos_raw_es` / `channel_videos_raw_en`
- `channels_processed_es` / `channels_processed_en`
- `channels_discovery_claims_es` / `channels_discovery_claims_en`

#### Updated Functions
All database functions now use `_get_table_name()` to access language-specific tables:
- `create_search_run()`
- `finish_search_run()`
- `get_executed_queries()`
- `insert_videos_raw()`
- `fetch_unprocessed_videos_raw()`
- `insert_videos_normalized()`
- `claim_channels_for_discovery()`
- `upsert_channel_raw()`
- `upsert_channel_videos_raw()`
- `mark_channel_processed()`
- `is_channel_processed()`

## Python Scripts Changes

### 1. `yt_discovery.py`
**Changes:**
- Converts locale flag (`--EN`/`--ES`) to simple language code (`"en"`/`"es"`)
- Passes `language` parameter to `db.init_db()`

**Usage:**
```bash
python yt_discovery.py --query "history" --EN  # Uses English tables
python yt_discovery.py --query "historia" --ES  # Uses Spanish tables
```

### 2. `run_discovery.py`
**Changes:**
- Updated `get_already_run_queries()` to accept `language` parameter
- Converts locale to language code before calling DB functions
- Passes language to both check-batches mode and normal execution

**Usage:**
```bash
python run_discovery.py --batch-size 10 --EN  # English tables
python run_discovery.py --batch-size 10 --ES  # Spanish tables
```

### 3. `run_parallel_discovery.py`
**Changes:**
- Updated `get_already_run_queries()` to accept `language` parameter
- Converts locale to language code for DB operations
- Language context properly passed through worker functions

**Usage:**
```bash
python run_parallel_discovery.py --instances 5 --batch-size 50 --EN
python run_parallel_discovery.py --instances 5 --batch-size 50 --ES
```

### 4. `yt_normalization_validation.py`
**Changes:**
- Added command-line arguments for language selection (`--EN`/`--ES`)
- Updated `main()` to accept and pass `language` parameter to `db.init_db()`

**Usage:**
```bash
python yt_normalization_validation.py --EN  # Normalize English data
python yt_normalization_validation.py --ES  # Normalize Spanish data
```

### 5. `yt_channel_discovery.py`
**Changes:**
- Added `language` parameter to `run()` function
- Added `--EN`/`--ES` command-line arguments to arg parser
- Passes `language` parameter to `db.init_db()`

**Usage:**
```bash
python yt_channel_discovery.py --limit-channels 100 --EN
python yt_channel_discovery.py --limit-channels 100 --ES
```

## GitHub Workflows Changes

### 1. `.github/workflows/parallel-discovery.yml`
**Changes:**
- Already had `language` input parameter (EN/ES choice)
- Workflow correctly passes language flag to Python scripts
- No changes needed - already compatible!

**Usage:**
- Select "EN" or "ES" from workflow dispatch dropdown
- Language flag is automatically passed to all batch jobs

### 2. `.github/workflows/parallel-channel-discovery.yml`
**Changes:**
- **Added** `language` input parameter (EN/ES choice)
- Updated job to conditionally pass `--EN` or `--ES` flag to `yt_channel_discovery.py`

**Usage:**
- Select "EN" or "ES" from workflow dispatch dropdown
- Each parallel job uses the correct language tables

## Migration Strategy

### For New Deployments
1. Run `db.py` table creation twice:
   ```python
   await db.init_db(language="es")  # Creates _es tables
   await db.init_db(language="en")  # Creates _en tables
   ```

### For Existing Data
**Option A: Migrate existing data to `_es` tables (Spanish was default)**
```sql
-- Example migration (adjust table names as needed)
INSERT INTO search_runs_es SELECT * FROM search_runs;
INSERT INTO videos_raw_es SELECT * FROM videos_raw;
-- ... repeat for all tables
```

**Option B: Start fresh with new table structure**
- Keep existing tables as legacy/archive
- All new runs use language-specific tables
- Gradually migrate or retire old data

## Testing Checklist

- [ ] Run English discovery: `python yt_discovery.py --query "test" --EN`
- [ ] Run Spanish discovery: `python yt_discovery.py --query "prueba" --ES`
- [ ] Verify EN data in `*_en` tables
- [ ] Verify ES data in `*_es` tables
- [ ] Run normalization for both languages
- [ ] Run channel discovery for both languages
- [ ] Test GitHub Actions workflows with both languages
- [ ] Verify no cross-contamination between language tables

## Benefits

✅ **Clean Data Separation**: English and Spanish data never mix
✅ **Independent Processing**: Process EN/ES pipelines separately
✅ **Accurate Metrics**: Track progress per language independently
✅ **Flexible Scaling**: Scale EN and ES workloads independently
✅ **Backward Compatible**: All scripts maintain same CLI interface
✅ **Workflow Ready**: GitHub Actions fully support both languages

## Important Notes

- **Default Language**: All scripts default to Spanish (`"es"`) if no flag specified
- **Database Schema**: Tables are created idempotently (safe to re-run)
- **Foreign Keys**: All foreign key references updated to use language-specific tables
- **Indices**: All indices created per-language for optimal performance
- **Thread Safety**: DB operations remain thread-safe via asyncpg pool

## Rollback Plan

If issues occur, to rollback:
1. Restore original `db.py` without language parameters
2. Restore original script files from git
3. Continue using legacy tables without suffixes
4. All changes are in version control for easy reversion
