# Newsagger

A Python tool for downloading and processing complete news archives from the Library of Congress Chronicling America collection.

## Features

- **Rate-limited API access** - Respects LOC's 20 requests/minute limit with automatic CAPTCHA/429 handling
- **Faceted search** - Avoids deep paging limits by using date facets for large result sets
- **Resumable downloads** - SQLite-based storage tracks progress and allows resuming interrupted downloads
- **Metadata extraction** - Stores newspaper and page metadata for offline analysis
- **Interactive CLI** - Easy-to-use command line interface for browsing and downloading

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up configuration:**
   ```bash
   cp .env.example .env
   # Edit .env with your preferred settings
   ```

3. **List available newspapers:**
   ```bash
   python main.py list-newspapers
   ```

4. **Search for specific newspapers:**
   ```bash
   python main.py search-newspapers --state "California" --limit 5
   ```

5. **Download a newspaper's pages:**
   ```bash
   python main.py download-newspaper sn84038012 --date1 1900 --date2 1910
   ```

6. **Search for text across archives:**
   ```bash
   python main.py search-text "earthquake" --date1 1906 --date2 1907
   ```

## Bulk Download Examples

### Download Entire State Collections

Download all California newspapers from 1900-1920:
```bash
# First, find California newspapers
python main.py search-newspapers --state "California" --limit 50

# Download each newspaper individually (recommended for large collections)
python main.py download-newspaper sn84038012 --date1 1900 --date2 1920
python main.py download-newspaper sn85066387 --date1 1900 --date2 1920
```

### Download by Decade Facets

Download newspapers by decade to manage large collections:
```bash
# Download 1900s decade
python main.py download-newspaper sn84038012 --date1 1900 --date2 1909

# Download 1910s decade  
python main.py download-newspaper sn84038012 --date1 1910 --date2 1919
```

### Estimate Before Downloading

Always check download size before starting large collections:
```bash
# Get size estimate without downloading
python main.py download-newspaper sn84038012 --date1 1900 --date2 1920 --estimate-only

# Example output:
# 📊 Download Estimate for sn84038012:
#    Total pages: 45,230
#    Estimated size: 113.1 GB
#    Estimated time: 37.7 hours
```

### Systematic State-by-State Collection

For comprehensive historical research:
```bash
# 1. List all newspapers to understand scope
python main.py list-newspapers > newspapers_list.txt

# 2. Focus on specific states and time periods
python main.py search-newspapers --state "New York" --limit 20
python main.py search-newspapers --state "California" --limit 20

# 3. Download major newspapers by decade
python main.py download-newspaper sn83030214 --date1 1900 --date2 1909  # NY Times
python main.py download-newspaper sn85066387 --date1 1900 --date2 1909  # SF Chronicle

# 4. Monitor progress
python main.py status
```

### Text-Based Historical Research

Search across multiple time periods for specific events:
```bash
# Search for Civil War coverage
python main.py search-text "civil war" --date1 1861 --date2 1865 --limit 500

# Search for economic events
python main.py search-text "stock market" --date1 1929 --date2 1932 --limit 200

# Search for technology adoption
python main.py search-text "automobile" --date1 1900 --date2 1920 --limit 300
```

### Managing Large Downloads

Best practices for multi-terabyte collections:
```bash
# 1. Start with estimates to plan storage
python main.py download-newspaper sn84038012 --estimate-only

# 2. Download in smaller time chunks
python main.py download-newspaper sn84038012 --date1 1900 --date2 1905
python main.py download-newspaper sn84038012 --date1 1906 --date2 1910

# 3. Check progress regularly
python main.py status

# 4. Resume interrupted downloads (automatically handled)
python main.py download-newspaper sn84038012 --date1 1900 --date2 1920
```

## Commands

- `list-newspapers` - Fetch and display all available newspapers
- `search-newspapers` - Search newspapers with filters (state, language)
- `download-newspaper LCCN` - Download pages for a specific newspaper
- `search-text TEXT` - Search for text across newspaper archives
- `status` - Show download progress and database statistics

## Configuration

Environment variables (see `.env.example`):

- `REQUEST_DELAY` - Seconds between API requests (minimum 3.0)
- `DATABASE_PATH` - SQLite database location
- `DOWNLOAD_DIR` - Directory for downloaded files
- `LOG_LEVEL` - Logging verbosity (DEBUG, INFO, WARNING, ERROR)

## Architecture

- **API Client** (`api_client.py`) - Handles LOC API interactions with rate limiting
- **Data Processor** (`processor.py`) - Processes and validates API responses
- **Storage Layer** (`storage.py`) - SQLite-based metadata and progress storage
- **CLI Interface** (`cli.py`) - Command-line user interface
- **Configuration** (`config.py`) - Environment-based configuration management

## Rate Limiting

The tool enforces LOC's rate limits:
- Maximum 20 requests per minute
- Automatic 1-hour pause on 429 responses or CAPTCHA detection
- Faceted search to stay under 100,000 result limits per query

## Data Storage

- **Newspapers table** - Stores newspaper metadata (title, location, date ranges)
- **Pages table** - Stores individual page metadata and download status
- **Download sessions** - Tracks progress of bulk download operations
- **Automatic deduplication** - Prevents duplicate entries across faceted searches

## Testing

Run the comprehensive test suite:

```bash
# Install test dependencies
pip install pytest pytest-mock responses

# Run all tests
python run_tests.py

# Or run pytest directly
pytest tests/

# Run specific test categories
pytest tests/test_api_client.py -v
pytest tests/test_integration.py -v

# Run with coverage (requires pytest-cov)
pip install pytest-cov
pytest --cov=newsagger --cov-report=html tests/
```

**Test Coverage:**
- **Unit Tests** - Individual component testing (API client, processor, storage, config)
- **Integration Tests** - End-to-end workflow testing
- **Mock API Responses** - Tests work without actual API calls
- **Error Handling** - Rate limiting, network errors, invalid data
- **Database Operations** - SQLite storage and retrieval
- **Configuration** - Environment variable handling