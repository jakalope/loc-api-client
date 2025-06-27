# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The **LoC Chronicling America Client** is a comprehensive Python client for systematic discovery, queuing, and downloading of historical newspaper archives from the Library of Congress Chronicling America API. The project is now **functionally complete** with full automation workflows for large-scale newspaper archive collection.

## Current Project Status

### ✅ **Fully Implemented Features**

1. **Complete API Client** (`api_client.py`)
   - Rate limiting compliance (20 requests/minute)
   - CAPTCHA/429 detection with 1-hour pause
   - Faceted search to avoid deep paging limits
   - Timeout handling and retry logic

2. **Automated Discovery System** (`discovery.py`)
   - Systematic periodical discovery and cataloging
   - Intelligent facet creation (date ranges, states)
   - Content discovery with batch processing
   - Priority-based queuing with size management

3. **Complete Download Processor** (`downloader.py`)
   - Queue-based download processing
   - Multiple file format support (PDF, JP2, OCR, metadata)
   - Hierarchical file organization (LCCN/year/month)
   - Progress tracking and resume functionality
   - Error recovery and cleanup utilities

4. **Comprehensive CLI** (`cli.py`)
   - 25+ commands for discovery, queuing, and downloading
   - Automated workflow setup
   - Progress monitoring and statistics
   - Priority-based and filtered downloads

5. **Robust Storage Layer** (`storage.py`)
   - SQLite database with 8 tables for tracking
   - Comprehensive statistics and progress tracking
   - Queue management with priorities
   - Metadata preservation and deduplication

6. **Complete Test Suite**
   - Unit tests for all major components
   - Integration tests for workflows
   - Mock API testing for reliability
   - Download functionality validation

### 📊 **Production-Ready Workflows**

**Complete Automated Workflow:**
```bash
# FIRST: Activate virtual environment
source venv/bin/activate

# End-to-end automation for historical research
python main.py setup-download-workflow \
  --start-year 1906 --end-year 1906 \
  --states "California" \
  --auto-discover --auto-enqueue \
  --max-size-gb 5.0

# Process downloads with progress tracking
python main.py process-downloads --max-items 100
```

**Large-Scale Collection:**
```bash
# FIRST: Activate virtual environment
source venv/bin/activate

# Systematic discovery and queuing
python main.py discover --max-papers 1000
python main.py create-facets --start-year 1900 --end-year 1920
python main.py auto-discover-facets --auto-enqueue --max-items 1000
python main.py download-priority --priority 1 --max-items 50
```

## Technical Architecture

### **API Integration Patterns**
- **Faceted Search Strategy**: Uses date/state facets to manage large result sets
- **Rate Limiting Compliance**: 3+ second delays with exponential backoff
- **Deep Paging Avoidance**: Automatic faceting to stay under 100,000 item limits
- **Bulk Processing**: Batch operations with progress tracking

### **Data Organization**
```
downloads/
└── {lccn}/                    # Library of Congress Control Number
    └── {year}/                # Publication year
        └── {month}/           # Publication month
            ├── {safe_item_id}.pdf           # Page PDF
            ├── {safe_item_id}.jp2           # Page image
            ├── {safe_item_id}_ocr.txt       # OCR text
            └── {safe_item_id}_metadata.json # Complete metadata
```

### **Database Schema**
- **periodicals** - Newspaper metadata and discovery tracking
- **search_facets** - Date/state facets with progress tracking
- **pages** - Individual page metadata and download status
- **download_queue** - Priority-based download management
- **periodical_issues** - Issue-level tracking
- **download_sessions** - Bulk operation progress

## Core API Endpoints (Chronicling America)

**Base URL:** `https://chroniclingamerica.loc.gov/`

**Primary Endpoints:**
- `/newspapers.json` - Newspaper catalog discovery
- `/search/pages/results/` - Faceted page search
- `/lccn/{lccn}/issues/{date}/ed-{edition}/seq-{sequence}.json` - Page metadata
- `/batches.json` - Digitization batch information

**Critical Rate Limits:**
- **Requests per minute**: 20 maximum
- **Deep paging limit**: 100,000 items per query
- **Items per page**: 1,000 maximum recommended
- **Timeout penalty**: 1 hour block on 429/CAPTCHA

## Development Guidelines

### **🚨 CRITICAL: Virtual Environment Setup**

**ALWAYS activate the virtual environment before running ANY commands:**
```bash
source venv/bin/activate  # On macOS/Linux
# OR
venv\Scripts\activate     # On Windows
```

**All CLI commands MUST be run with the venv activated:**
```bash
# ✅ CORRECT - with venv activated
source venv/bin/activate
python3 main.py --help

# ❌ WRONG - will fail with module import errors
python3 main.py --help
```

**Dependencies are installed in the venv, not globally.** If you get import errors like `ModuleNotFoundError: No module named 'click'`, you forgot to activate the venv.

### **When Working with This Codebase:**

1. **Virtual Environment**: ALWAYS activate venv first (see above)
2. **API Compliance**: Always respect LoC rate limits and guidelines
3. **Faceted Approach**: Use date/state facets for large queries
4. **Progress Tracking**: Update database status for all operations
5. **Error Handling**: Implement graceful timeout and retry logic
6. **Testing**: Maintain comprehensive test coverage

### **Key Implementation Patterns:**

```python
# Rate limiting pattern
time.sleep(self.request_delay)  # Minimum 3 seconds

# Faceted search pattern
search_params = {
    'date1': start_year,
    'date2': end_year,
    'dates_facet': f"{start_year}/{end_year}",
    'rows': min(batch_size, 1000)
}

# Progress tracking pattern
self.storage.update_facet_discovery(
    facet_id, 
    items_discovered=total_discovered,
    status='completed'
)
```

### **Testing Approach:**
- **REMEMBER**: Activate venv first: `source venv/bin/activate`
- Use `test-discovery` for small datasets first
- Always use `--dry-run` for large operations
- Monitor with `discovery-status` and `download-stats`
- Test priority downloads before bulk processing

## Maintenance and Extensions

### **When Adding Features:**
1. Follow existing patterns in each module
2. Add comprehensive tests for new functionality
3. Update CLI help text and README examples
4. Ensure rate limiting compliance
5. Add progress tracking and error handling

### **Performance Optimization:**
- Use smaller batch sizes for state searches (avoid timeouts)
- Implement smart retry logic for failed downloads
- Monitor disk usage and cleanup incomplete files
- Use priority queuing for time-sensitive research

### **Monitoring and Debugging:**
- Check `discovery-status` for overall progress
- Use `show-queue` to monitor download queue
- Review `download-stats` for performance metrics
- Enable verbose logging for troubleshooting

## Current Capabilities Summary

This project now provides a **complete, production-ready solution** for:

- ✅ **Systematic newspaper discovery** across all LoC collections
- ✅ **Automated content queuing** with intelligent prioritization  
- ✅ **Robust download processing** with multiple file formats
- ✅ **Comprehensive progress tracking** and statistics
- ✅ **Error recovery and resume** functionality
- ✅ **API compliance and rate limiting** 
- ✅ **Organized file storage** and metadata preservation
- ✅ **Command-line automation** with 25+ specialized commands

The system successfully handles large-scale historical newspaper archive collection while respecting Library of Congress API guidelines and providing researchers with organized, searchable local collections.

## Usage for Different Research Scenarios

**Digital Humanities Research:**
```bash
source venv/bin/activate
python main.py setup-download-workflow --start-year 1917 --end-year 1919 --states "New York,Illinois,California" --auto-discover --auto-enqueue --max-size-gb 50.0
```

**Historical Event Studies:**
```bash
source venv/bin/activate
python main.py setup-download-workflow --start-year 1906 --end-year 1906 --states "California" --auto-discover --auto-enqueue --max-size-gb 10.0
```

**Comparative State Analysis:**
```bash
source venv/bin/activate
python main.py create-facets --start-year 1900 --end-year 1920
python main.py auto-discover-facets --auto-enqueue --max-items 500
python main.py download-priority --queue-type facet --priority 1
```

The project is now ready for production use by researchers, digital humanities scholars, and institutions requiring systematic access to historical newspaper archives.

## Project Requirements and Goals

### **Primary Objectives**

1. **Complete Archive Download**: Download everything available through the Library of Congress Chronicling America API automatically
2. **Frequent Progress Updates**: Provide detailed, real-time progress reporting so users know the system is actively working
3. **Robust Resume Functionality**: If the process is killed, resume close to where it left off with minimal data loss
4. **Distributed Processing Support**: Enable breakdown of remaining tasks into N separate databases for distributed processing across multiple proxies/services (e.g., Cloudflare)

### **Enhanced Requirements**

#### **🎯 Complete Automation**
- **Goal**: Systematically discover and download all historical newspaper content from the LoC API
- **Scope**: All available years, states, and content types (PDF, JP2, OCR, metadata)
- **Approach**: Automated workflow that requires minimal user intervention
- **Scalability**: Handle millions of items across decades of historical content

#### **📊 Enhanced Progress Reporting**
- **Real-time Updates**: Show progress at multiple granularities (facets, batches, individual items)
- **Nested Progress Bars**: Display both high-level progress (facets completed) and detailed progress (current batch within facet)
- **Live Statistics**: Items discovered/enqueued/downloaded, error rates, throughput metrics
- **Time Estimates**: Accurate ETA calculations based on current processing rates
- **Status Monitoring**: Background monitoring commands to check progress without interrupting operations

#### **🔄 Granular Resume Capability**
- **Batch-Level Resume**: Resume from the exact batch being processed when interrupted
- **State Preservation**: Maintain all progress state across process restarts
- **Error Recovery**: Automatically retry failed operations on resume
- **Progress Validation**: Verify data integrity when resuming long-running operations
- **Incremental Checkpointing**: Save progress frequently to minimize lost work

#### **🌐 Distributed Processing Architecture**
- **Database Splitting**: Split remaining work into N independent databases
- **Work Distribution**: Allocate facets/batches across multiple processing instances
- **Result Consolidation**: Merge completed work from distributed databases back into master database
- **Proxy Support**: Enable processing through multiple IP addresses/proxies to increase throughput
- **Load Balancing**: Distribute work based on facet size and complexity
- **Coordination**: Prevent duplicate work across distributed instances

### **Implementation Priorities**

1. **High Priority - Immediate Implementation**:
   - Enhanced progress reporting with nested progress bars
   - Granular resume functionality for batch-level interruption recovery
   - Real-time monitoring commands and status displays

2. **Medium Priority - Next Phase**:
   - Database splitting and work distribution functionality
   - Distributed processing coordination mechanisms
   - Proxy rotation and load balancing support

3. **Future Enhancements**:
   - Web dashboard for monitoring distributed operations
   - Advanced scheduling and throttling controls
   - Automated error analysis and recovery strategies

### **Technical Implementation Notes**

#### **Progress Reporting Enhancements**
- Implement nested `tqdm` progress bars for multi-level visibility
- Add database polling threads for real-time progress updates
- Create background monitoring commands that don't interfere with active operations
- Include throughput metrics (items/minute, MB/minute) in progress displays

#### **Resume Functionality Improvements**
- Reduce checkpoint intervals from facet-level to batch-level (every 50-100 items)
- Add progress validation on startup to detect and recover from incomplete operations
- Implement automatic retry logic for failed batches on resume
- Preserve detailed error information for troubleshooting interrupted operations

#### **Distributed Processing Design**
- Create database export/import utilities for work distribution
- Implement work allocation algorithms based on facet characteristics
- Add coordination mechanisms to prevent work duplication
- Design result merging strategies that handle conflicts and duplicates
- Support proxy configuration and rotation for distributed instances

These requirements ensure the system can handle the massive scale of the complete LoC archive while providing excellent visibility into progress and robust recovery capabilities for long-running operations.