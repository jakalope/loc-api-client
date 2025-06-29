# Batch Discovery Monitoring

The batch discovery monitor provides real-time visibility into the progress of batch discovery operations, showing detailed information about each batch being processed.

## Features

- **Real-time Progress Tracking**: Live updates showing current batch, issues processed, and pages discovered
- **Session Management**: Track multiple batch discovery sessions with their status and progress
- **CAPTCHA Detection**: Shows when discovery is paused due to CAPTCHA cooling-off periods
- **Performance Metrics**: Displays processing rates, ETAs, and throughput statistics
- **Batch-level Details**: Individual progress for each batch within a session

## Usage

### Live Monitoring

Monitor batch discovery progress in real-time:

```bash
python monitor_batch_discovery.py --db-path data/newsagger.db
```

The monitor updates every 5 seconds by default. Press Ctrl+C to exit.

### One-time Summary

Get a snapshot of all batch discovery sessions:

```bash
python monitor_batch_discovery.py --db-path data/newsagger.db --summary
```

### Command-line Options

- `--db-path PATH`: Path to the database file (default: news_archive.db)
- `--interval SECONDS`: Refresh interval for live monitoring (default: 5)
- `--summary`: Show one-time summary instead of live monitoring

## Monitor Display

The live monitor shows four main sections:

1. **Header**: Current time and last update timestamp
2. **Sessions Table**: All batch discovery sessions with their status and statistics
3. **Active Session Panel**: Detailed information about the currently active session
4. **Recent Batches**: Progress of individual batches being processed

### Session Status

- `active`: Currently processing batches
- `captcha_blocked`: Paused due to CAPTCHA detection, waiting for cooling-off
- `completed`: Finished processing all batches
- `failed`: Stopped due to an error

### Progress Metrics

- **Batch Progress**: Current batch index out of total batches
- **Issue Progress**: Issues processed within the current batch
- **Pages Discovered**: Total pages found across all batches
- **Pages Enqueued**: Pages added to download queue (if auto-enqueue enabled)
- **Processing Rate**: Pages discovered per minute/hour
- **ETA**: Estimated time to complete remaining batches

## Demo

To see the monitor in action without running actual batch discovery:

```bash
# Terminal 1: Run the demo simulation
python demo_batch_monitor.py

# Terminal 2: Monitor the demo progress
python monitor_batch_discovery.py --db-path demo_monitor.db
```

The demo simulates:
- Processing 10 batches with varying numbers of issues
- Random page counts per issue
- Occasional CAPTCHA detections with cooling-off periods
- Real-time progress updates

## Integration with Batch Discovery

When running actual batch discovery operations, the monitor automatically tracks:

1. **Batch Processing**: Each digitization batch from the Library of Congress
2. **Issue Discovery**: Individual newspaper issues within each batch
3. **Page Extraction**: Pages found within each issue
4. **Error Handling**: CAPTCHA detections and cooling-off periods
5. **Resume Capability**: Sessions can be resumed from where they left off

Example batch discovery with monitoring:

```bash
# Terminal 1: Start batch discovery
python main.py discover-via-batches --max-batches 100 --auto-enqueue

# Terminal 2: Monitor progress
python monitor_batch_discovery.py --db-path data/newsagger.db
```

## Understanding the Output

### Sessions Table Columns

- **Session**: Name of the batch discovery session
- **Status**: Current state (active/completed/captcha_blocked)
- **Progress**: Percentage of batches completed
- **Batches**: Current batch / Total batches
- **Pages**: Total pages discovered
- **Duration**: Time elapsed since session started
- **Rate**: Pages discovered per hour

### Active Session Details

Shows real-time information about the currently processing session:
- Current batch name and progress
- Issue progress within the batch
- Total statistics (pages discovered/enqueued)
- Processing rates and ETA
- CAPTCHA status if blocked

## Tips

1. **Multiple Sessions**: The monitor can track multiple concurrent batch discovery sessions
2. **Historical Data**: Completed sessions remain in the database for analysis
3. **Performance Tuning**: Use the rate metrics to optimize batch sizes and delays
4. **CAPTCHA Monitoring**: Watch for frequent CAPTCHA blocks to adjust rate limiting
5. **Database Location**: Ensure you're monitoring the correct database file

## Troubleshooting

- **No sessions found**: Check that batch discovery has been run and the correct database path is specified
- **Stale data**: Increase refresh interval if the database is on a slow network drive
- **Missing columns**: Ensure the database schema is up to date with the latest migrations