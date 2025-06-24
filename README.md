# LoC Chronicling America Client

A comprehensive Python client for the Library of Congress Chronicling America API, designed for systematic discovery, queuing, and downloading of historical newspaper archives.

## Overview

The **LoC Chronicling America Client** provides automated workflows for large-scale newspaper archive collection from the Library of Congress's Chronicling America digital collection. Built specifically for the LoC API, it handles the complexities of rate limiting, faceted search pagination, and systematic content discovery.

## Key Features

### 🔍 **Automated Discovery & Queuing**
- **Systematic periodical discovery** - Automatically catalog all available newspapers
- **Intelligent facet creation** - Generate date ranges and state-based search facets
- **Priority-based queuing** - Smart download queue management with size limits
- **Complete workflow automation** - End-to-end setup from discovery to download

### 📥 **Robust Download Processing**
- **Queue-based downloads** - Process thousands of items systematically
- **Multiple file formats** - PDF, JP2 images, OCR text, and metadata
- **Organized file structure** - Hierarchical storage by LCCN/year/month
- **Resume capability** - Automatic retry of failed downloads
- **Progress tracking** - Real-time progress bars and statistics

### 🛡️ **API Compliance & Reliability**
- **Rate limiting compliance** - Respects LoC's 20 requests/minute limit
- **CAPTCHA/429 handling** - Automatic 1-hour pause on rate limit detection
- **Faceted search optimization** - Avoids deep paging limits (100,000+ results)
- **Error recovery** - Graceful handling of timeouts and network issues

### 📊 **Advanced Management**
- **Comprehensive statistics** - Track discovery progress and download metrics
- **Flexible filtering** - Download by priority, type, date range, or state
- **Database tracking** - SQLite-based metadata and progress storage
- **Interactive CLI** - Full-featured command-line interface

## Quick Start

### 1. Setup Environment
```bash
# Clone and setup
git clone <repository-url>
cd newsagger
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Basic Discovery & Download
```bash
# Complete automated workflow for California newspapers (1906)
python main.py setup-download-workflow \
  --start-year 1906 --end-year 1906 \
  --states "California" \
  --auto-discover --auto-enqueue \
  --max-size-gb 1.0

# Process the download queue
python main.py process-downloads --max-items 10
```

### 3. Monitor Progress
```bash
# Check overall system status
python main.py discovery-status

# View download queue
python main.py show-queue --limit 10

# Check download statistics
python main.py download-stats
```

## Complete Workflow Examples

### 🏛️ **Large-Scale Historical Collection**
```bash
# 1. Discover all available periodicals
python main.py discover --max-papers 1000

# 2. Create systematic date facets
python main.py create-facets --start-year 1900 --end-year 1920 --facet-size 1

# 3. Auto-discover content for all facets
python main.py auto-discover-facets --auto-enqueue --max-items 1000

# 4. Process high-priority downloads first
python main.py download-priority --priority 1 --max-items 50

# 5. Monitor progress
python main.py discovery-status
```

### 📰 **Focused State/Event Collection**
```bash
# San Francisco earthquake coverage (1906)
python main.py setup-download-workflow \
  --start-year 1906 --end-year 1906 \
  --states "California" \
  --auto-discover --auto-enqueue \
  --max-size-gb 5.0

# WWI era coverage (1917-1919)  
python main.py setup-download-workflow \
  --start-year 1917 --end-year 1919 \
  --states "New York,Illinois,California" \
  --auto-discover --auto-enqueue \
  --max-size-gb 10.0
```

### 🔍 **Research-Focused Discovery**
```bash
# Test discovery with small dataset
python main.py test-discovery --year 1906 --max-items 20

# Search for specific content
python main.py search-text "earthquake" --date1 1906 --date2 1907 --limit 100

# Download specific newspaper
python main.py download-newspaper sn84038012 --date1 1906 --date2 1906
```

## Advanced Commands

### Discovery & Planning
- `discover` - Catalog available periodicals from LoC
- `create-facets` - Create systematic date/state facets
- `auto-discover-facets` - Systematically discover content for all facets
- `test-discovery` - Test discovery with focused datasets
- `discovery-status` - Comprehensive discovery progress

### Queue Management
- `auto-enqueue` - Automatically queue discovered content
- `populate-queue` - Populate queue with priority settings
- `show-queue` - View queued items with filtering
- `list-facets` - View facet status and progress

### Download Processing
- `process-downloads` - Main download queue processor
- `download-page` - Download individual pages
- `download-priority` - Download by priority/type
- `resume-downloads` - Resume failed downloads
- `download-stats` - Comprehensive download statistics
- `cleanup-downloads` - Clean incomplete files

### Workflow Automation
- `setup-download-workflow` - Complete automated setup
- `status` - Overall system status
- `search-newspapers` - Search available newspapers
- `search-text` - Search content across archives

## Configuration

Environment variables (`.env` file):
```bash
# API Settings
REQUEST_DELAY=3.0          # Seconds between requests (min 3.0)
MAX_RETRIES=3              # API retry attempts

# Storage Settings  
DATABASE_PATH=./data/newsagger.db
DOWNLOAD_DIR=./downloads

# Logging
LOG_LEVEL=INFO             # DEBUG, INFO, WARNING, ERROR
```

## Project Structure

```
newsagger/
├── src/newsagger/
│   ├── api_client.py      # LoC API client with rate limiting
│   ├── discovery.py       # Automated discovery and queuing
│   ├── downloader.py      # Download queue processor
│   ├── processor.py       # API response processing
│   ├── storage.py         # SQLite database operations
│   ├── config.py          # Configuration management
│   └── cli.py             # Command-line interface
├── tests/                 # Comprehensive test suite
├── downloads/             # Downloaded content (auto-created)
├── data/                  # Database storage (auto-created)
└── main.py               # CLI entry point
```

## API Compliance

### Library of Congress Guidelines
- **Rate Limiting**: 20 requests/minute maximum
- **Deep Paging**: Limited to 100,000 items per query
- **Timeout Handling**: 1-hour pause on 429/CAPTCHA responses
- **Faceted Search**: Uses date/state facets to manage large result sets

### Best Practices
- Always use `--estimate-only` for large downloads
- Start with small test datasets (`test-discovery`)
- Monitor progress with `discovery-status` and `download-stats`
- Use priority downloads for time-sensitive research
- Set reasonable size limits with `--max-size-gb`

## Data Organization

### File Structure
```
downloads/
└── {lccn}/           # Library of Congress Control Number
    └── {year}/       # Publication year
        └── {month}/  # Publication month
            ├── {item_id}.pdf           # Page PDF
            ├── {item_id}.jp2           # Page image
            ├── {item_id}_ocr.txt       # OCR text
            └── {item_id}_metadata.json # Complete metadata
```

### Database Schema
- **Periodicals** - Newspaper metadata and discovery tracking
- **Search Facets** - Date/state facets with progress tracking  
- **Pages** - Individual page metadata and download status
- **Download Queue** - Priority-based download management
- **Sessions** - Progress tracking for bulk operations

## Testing

```bash
# Install test dependencies
pip install pytest pytest-mock

# Run comprehensive test suite
python -m pytest tests/ -v

# Test specific components
python -m pytest tests/test_downloader.py -v
python -m pytest tests/test_discovery_automation.py -v

# Run with coverage
pip install pytest-cov
python -m pytest --cov=src/newsagger tests/
```

## Contributing

This project focuses specifically on the Library of Congress Chronicling America API. Contributions should maintain compatibility with LoC's API structure and rate limiting requirements.

## License

Educational and research use. Respects Library of Congress terms of service and API guidelines.