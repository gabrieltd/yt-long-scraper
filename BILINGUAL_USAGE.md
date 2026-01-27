# Bilingual YouTube Scraper - Usage Guide

## Overview
The YouTube scraper now supports both **English (en-US)** and **Spanish (es-MX)** interfaces with configurable search filters.

## Language Selection

### Command-Line Arguments
- `--EN` : Use English interface (en-US locale, queries_en.txt)
- `--ES` : Use Spanish interface (es-MX locale, queries.txt) **[DEFAULT]**

### Examples
```bash
# English version
python yt_discovery.py --query "documentary" --EN

# Spanish version (default)
python yt_discovery.py --query "documental" --ES
python yt_discovery.py --query "documental"  # Same as --ES
```

## Search Filters

All filters are **optional**. If not specified, YouTube's default behavior applies (no filters).

### Upload Date Filter
`--upload-date <option>`

Options:
- `last_hour` - Videos from the last hour
- `today` - Videos uploaded today
- `this_week` - Videos from this week
- `this_month` - Videos from this month
- `this_year` - Videos from this year

**Example:**
```bash
python yt_discovery.py --query "news" --EN --upload-date today
```

### Duration Filter
`--duration <option>`

Options:
- `under_4` - Videos under 4 minutes
- `4_20` - Videos between 4-20 minutes
- `over_20` - Videos over 20 minutes

**Example:**
```bash
python yt_discovery.py --query "documentary" --EN --duration over_20
```

### Features Filter
`--features <feature1> <feature2> ...`

Options (can combine multiple):
- `live` - Live streams
- `4k` - 4K resolution
- `hd` - HD quality
- `subtitles` - Subtitles/CC available
- `creative_commons` - Creative Commons licensed
- `360` - 360° videos
- `vr180` - VR180 format
- `3d` - 3D videos
- `hdr` - HDR quality
- `location` - Videos with location
- `purchased` - Purchased content

**Example:**
```bash
# Single feature
python yt_discovery.py --query "nature" --EN --features 4k

# Multiple features
python yt_discovery.py --query "concert" --EN --features hd subtitles
```

### Sort By Filter
`--sort-by <option>`

Options:
- `relevance` - Sort by relevance (YouTube default)
- `upload_date` - Sort by upload date (newest first)
- `view_count` - Sort by view count
- `rating` - Sort by rating

**Example:**
```bash
python yt_discovery.py --query "tutorial" --EN --sort-by view_count
```

## Combined Examples

### English - Full Filter Stack
```bash
python yt_discovery.py \
  --query "science documentary" \
  --EN \
  --upload-date this_month \
  --duration over_20 \
  --features 4k hd subtitles \
  --sort-by view_count \
  --headed
```

### Spanish - Basic Filters
```bash
python yt_discovery.py \
  --query "documental historia" \
  --ES \
  --upload-date this_month \
  --duration over_20 \
  --headless
```

### English - Live Content
```bash
python yt_discovery.py \
  --query "concert" \
  --EN \
  --features live hd \
  --sort-by relevance
```

## Batch Discovery (run_discovery.py)

The batch runner supports all language and filter options for sequential or batch processing.

### Basic Usage
```bash
# English - Auto-selects queries_en.txt
python run_discovery.py --EN

# Spanish - Auto-selects queries.txt (default)
python run_discovery.py --ES
python run_discovery.py  # Same as --ES
```

### With Batch Mode
```bash
# Process batch 0 (first 50 queries) in English
python run_discovery.py --batch-size 50 --batch-index 0 --EN

# Process batch 2 in Spanish with filters
python run_discovery.py --batch-size 50 --batch-index 2 --ES --duration over_20 --upload-date this_month
```

### Check Pending Batches
```bash
# Get list of batches with pending queries (English)
python run_discovery.py --batch-size 50 --check-batches --EN

# Get list of batches with pending queries (Spanish)
python run_discovery.py --batch-size 50 --check-batches --ES
```

### With Filters
```bash
python run_discovery.py \
  --EN \
  --upload-date this_week \
  --duration over_20 \
  --features 4k hd \
  --sort-by view_count
```

## Parallel Discovery (run_parallel_discovery.py)

The parallel runner also supports all language and filter options.

### Basic Usage
```bash
# English - Auto-selects queries_en.txt
python run_parallel_discovery.py \
  --instances 5 \
  --batch-size 10 \
  --EN

# Spanish - Auto-selects queries.txt (default)
python run_parallel_discovery.py \
  --instances 5 \
  --batch-size 10 \
  --ES
```

### With Custom Query File
```bash
python run_parallel_discovery.py \
  --instances 3 \
  --batch-size 5 \
  --EN \
  --queries-file "custom_queries.txt"
```

### With Filters
```bash
python run_parallel_discovery.py \
  --instances 5 \
  --batch-size 10 \
  --EN \
  --upload-date this_week \
  --duration over_20 \
  --features 4k hd \
  --sort-by view_count
```

### Reprocess Duplicates
```bash
python run_parallel_discovery.py \
  --instances 5 \
  --batch-size 10 \
  --EN \
  --reprocess-duplicates
```

## Configuration Details

### English (en-US)
- **Locale:** en-US
- **Timezone:** America/New_York
- **Accept-Language:** en-US,en;q=0.9
- **Default Query File:** queries_en.txt

### Spanish (es-MX)
- **Locale:** es-MX
- **Timezone:** America/Mexico_City
- **Accept-Language:** es-MX,es;q=0.9
- **Default Query File:** queries.txt

## UI String Mappings

### English Interface
- Search filters button: "Search filters"
- This month: "This month"
- Over 20 minutes: "Over 20 minutes"
- No more results: "No more results"

### Spanish Interface
- Search filters button: "Filtros de búsqueda"
- This month: "Este mes"
- Over 20 minutes: "Más de 20 minutos"
- No more results: "No hay más resultados"

## Notes

1. **Default Behavior:** If no language flag is specified, Spanish (--ES) is used by default
2. **Query Files:** The script auto-selects the appropriate query file based on language unless explicitly specified
3. **Filter Compatibility:** All filters work with both languages
4. **Browser Mode:** Use `--headed` to see the browser in action, `--headless` (default) for background execution
5. **Output:** Results maintain the same JSON structure regardless of language
6. **Parsing:** Data normalization (views, dates, durations) supports both English and Spanish formats automatically
7. **GitHub Actions:** The workflow `.github/workflows/parallel-discovery.yml` now supports language and filter selection through workflow inputs

## GitHub Actions Workflow

The GitHub Actions workflow has been updated to support language and filter selection through the workflow dispatch UI:

### Available Inputs
- **batch_size** (required): Number of queries per parallel job
- **language** (optional): `EN` or `ES` (default: `ES`)
- **upload_date** (optional): Filter by upload date
- **duration** (optional): Filter by video duration
- **sort_by** (optional): Sort results by specific criteria

The workflow will automatically:
- Use the correct query file based on language selection
- Pass all filter parameters to discovery jobs
- Calculate pending batches considering the language setting

## Troubleshooting

### Filter Not Applying
- Ensure filter names match exactly (case-sensitive in code, but arguments are validated)
- Check YouTube's UI hasn't changed (filter text might need updating in LANG_CONFIG)
- Run with `--headed` to visually debug filter application

### Wrong Language Results
- Verify the correct flag (--EN or --ES) is used
- Check browser locale in debug screenshots
- Ensure Accept-Language header is correct

### No Results Found
- Some filter combinations may be too restrictive
- Try broader filters or remove some
- Check if query matches the language selected
