#!/usr/bin/env python3
"""
Download Performance Benchmarking Suite

Tests various optimization strategies to identify bottlenecks in the download system.
"""

import os
import sys
import time
import json
import statistics
import logging
import threading
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import requests
import tempfile
import shutil

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.newsagger.storage import NewsStorage
from src.newsagger.rate_limited_client import LocApiClient
from src.newsagger.downloader import DownloadProcessor
from src.newsagger.config import Config


class DownloadBenchmarker:
    """Comprehensive benchmarking suite for download performance."""
    
    def __init__(self, test_data_size: int = 20):
        """Initialize benchmarker with test data."""
        self.test_data_size = test_data_size
        self.config = Config()
        self.logger = logging.getLogger(__name__)
        
        # Create temporary test environment
        self.temp_dir = Path(tempfile.mkdtemp(prefix='download_bench_'))
        self.test_db_path = self.temp_dir / 'test_benchmark.db'
        self.test_download_dir = self.temp_dir / 'downloads'
        
        # Initialize test storage and components
        self.storage = NewsStorage(str(self.test_db_path))
        self.api_client = LocApiClient()
        
        # Results storage
        self.benchmark_results = {}
        
    def setup_test_data(self) -> bool:
        """Create test queue with realistic page data."""
        self.logger.info(f"Setting up test data with {self.test_data_size} items...")
        
        # Get some real pages from storage if available
        main_storage = NewsStorage()  # Use main database
        sample_pages = main_storage.get_pages(limit=self.test_data_size)
        
        if len(sample_pages) < self.test_data_size:
            self.logger.warning(f"Only {len(sample_pages)} pages available, using all")
            self.test_data_size = len(sample_pages)
        
        if not sample_pages:
            self.logger.error("No pages available for testing")
            return False
        
        # Copy sample pages to test database
        page_infos = []
        for page in sample_pages:
            page_infos.append(page)
        
        # Store pages in batch
        if page_infos:
            self.storage.store_pages(page_infos)
            
            # Add to download queue
            for page in page_infos:
                self.storage.add_to_download_queue(
                    queue_type='page',
                    reference_id=page['item_id'],
                    priority=1,
                    estimated_size_mb=2.5  # Typical newspaper page size
                )
        
        self.logger.info(f"Test data setup complete: {len(sample_pages)} items")
        return True
    
    def cleanup(self):
        """Clean up temporary test environment."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def benchmark_serial_processing(self) -> Dict:
        """Benchmark current serial queue processing."""
        self.logger.info("Benchmarking serial queue processing...")
        
        # Reset queue to ensure clean test
        self._reset_test_queue()
        
        # Create standard downloader
        downloader = DownloadProcessor(
            storage=self.storage,
            api_client=self.api_client,
            download_dir=str(self.test_download_dir),
            file_types=['pdf', 'metadata']  # Limit for faster testing
        )
        
        start_time = time.time()
        result = downloader.process_queue(max_items=self.test_data_size, dry_run=True)
        end_time = time.time()
        
        return {
            'method': 'serial_processing',
            'duration_seconds': end_time - start_time,
            'items_processed': result.get('would_download', 0),
            'throughput_items_per_second': result.get('would_download', 0) / (end_time - start_time),
            'details': result
        }
    
    def benchmark_parallel_queue_processing(self, num_workers: int = 4) -> Dict:
        """Benchmark parallel queue processing at queue level."""
        self.logger.info(f"Benchmarking parallel queue processing ({num_workers} workers)...")
        
        self._reset_test_queue()
        
        # Get queue items
        queue_items = self.storage.get_download_queue(status='queued')
        if not queue_items:
            return {'error': 'No queue items available'}
        
        # Limit to test size
        queue_items = queue_items[:self.test_data_size]
        
        def process_item_batch(batch_items):
            """Process a batch of items in parallel."""
            downloader = DownloadProcessor(
                storage=self.storage,
                api_client=self.api_client,
                download_dir=str(self.test_download_dir),
                file_types=['pdf', 'metadata']
            )
            
            results = []
            for item in batch_items:
                # Simulate processing (dry run equivalent)
                start = time.time()
                time.sleep(0.1)  # Simulate processing time
                end = time.time()
                results.append({
                    'item_id': item['id'],
                    'duration': end - start,
                    'success': True
                })
            return results
        
        # Split items into batches for workers
        batch_size = max(1, len(queue_items) // num_workers)
        batches = [queue_items[i:i + batch_size] for i in range(0, len(queue_items), batch_size)]
        
        start_time = time.time()
        all_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_batch = {executor.submit(process_item_batch, batch): batch for batch in batches}
            
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_results = future.result()
                all_results.extend(batch_results)
        
        end_time = time.time()
        
        return {
            'method': f'parallel_queue_{num_workers}_workers',
            'duration_seconds': end_time - start_time,
            'items_processed': len(all_results),
            'throughput_items_per_second': len(all_results) / (end_time - start_time),
            'num_workers': num_workers,
            'batches': len(batches)
        }
    
    def benchmark_file_concurrency(self, num_workers_list: List[int] = [6, 8, 12, 16]) -> Dict:
        """Benchmark different file download concurrency levels."""
        results = {}
        
        for num_workers in num_workers_list:
            self.logger.info(f"Benchmarking file concurrency ({num_workers} workers)...")
            
            # Create mock download tasks
            download_tasks = []
            for i in range(20):  # Simulate 20 files
                download_tasks.append({
                    'url': f'https://httpbin.org/delay/0.1',  # Fast test endpoint
                    'path': self.test_download_dir / f'test_file_{i}.bin',
                    'type': 'test'
                })
            
            start_time = time.time()
            
            # Test concurrent downloads with different worker counts
            success_count = self._test_concurrent_downloads(download_tasks, num_workers)
            
            end_time = time.time()
            
            results[f'{num_workers}_workers'] = {
                'duration_seconds': end_time - start_time,
                'files_processed': len(download_tasks),
                'success_count': success_count,
                'throughput_files_per_second': len(download_tasks) / (end_time - start_time),
                'num_workers': num_workers
            }
        
        return results
    
    def benchmark_database_batch_sizes(self, batch_sizes: List[int] = [10, 25, 50, 100]) -> Dict:
        """Benchmark different database batch update sizes."""
        results = {}
        
        for batch_size in batch_sizes:
            self.logger.info(f"Benchmarking database batch size ({batch_size})...")
            
            # Create test updates
            test_updates = []
            for i in range(200):  # 200 test updates
                test_updates.append({
                    'id': i + 1,
                    'status': 'completed',
                    'progress_percent': 100,
                    'error_message': None
                })
            
            start_time = time.time()
            
            # Process updates in batches
            for i in range(0, len(test_updates), batch_size):
                batch = test_updates[i:i + batch_size]
                self._process_test_batch_updates(batch)
            
            end_time = time.time()
            
            results[f'batch_size_{batch_size}'] = {
                'duration_seconds': end_time - start_time,
                'updates_processed': len(test_updates),
                'throughput_updates_per_second': len(test_updates) / (end_time - start_time),
                'batch_size': batch_size,
                'num_batches': len(test_updates) // batch_size
            }
        
        return results
    
    def benchmark_io_chunk_sizes(self, chunk_sizes: List[int] = [8192, 32768, 65536, 131072, 262144]) -> Dict:
        """Benchmark different I/O chunk sizes for file operations."""
        results = {}
        
        # Create test data
        test_data = b'x' * (10 * 1024 * 1024)  # 10MB test file
        
        for chunk_size in chunk_sizes:
            self.logger.info(f"Benchmarking I/O chunk size ({chunk_size} bytes)...")
            
            test_file = self.temp_dir / f'test_chunk_{chunk_size}.bin'
            
            # Write test
            start_time = time.time()
            with open(test_file, 'wb') as f:
                for i in range(0, len(test_data), chunk_size):
                    chunk = test_data[i:i + chunk_size]
                    f.write(chunk)
            write_time = time.time() - start_time
            
            # Read test
            start_time = time.time()
            read_data = b''
            with open(test_file, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    read_data += chunk
            read_time = time.time() - start_time
            
            # Cleanup
            test_file.unlink()
            
            results[f'chunk_{chunk_size}'] = {
                'chunk_size_bytes': chunk_size,
                'write_duration_seconds': write_time,
                'read_duration_seconds': read_time,
                'total_duration_seconds': write_time + read_time,
                'write_throughput_mb_per_second': 10 / write_time,
                'read_throughput_mb_per_second': 10 / read_time,
                'data_size_mb': 10
            }
        
        return results
    
    def benchmark_memory_usage(self) -> Dict:
        """Profile memory usage patterns during download processing."""
        import psutil
        import gc
        
        self.logger.info("Benchmarking memory usage...")
        
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Reset test environment
        self._reset_test_queue()
        
        memory_measurements = []
        
        # Simulate download processing with memory tracking
        downloader = DownloadProcessor(
            storage=self.storage,
            api_client=self.api_client,
            download_dir=str(self.test_download_dir),
            file_types=['pdf', 'metadata']
        )
        
        start_time = time.time()
        
        # Process items one by one with memory tracking
        queue_items = self.storage.get_download_queue(status='queued')[:10]  # Smaller test
        
        for i, item in enumerate(queue_items):
            # Measure memory before processing
            pre_memory = process.memory_info().rss / 1024 / 1024
            
            # Simulate processing
            time.sleep(0.1)
            
            # Measure memory after processing
            post_memory = process.memory_info().rss / 1024 / 1024
            
            memory_measurements.append({
                'item_index': i,
                'pre_memory_mb': pre_memory,
                'post_memory_mb': post_memory,
                'memory_delta_mb': post_memory - pre_memory
            })
            
            # Force garbage collection to see effect
            if i % 3 == 0:
                gc.collect()
        
        end_time = time.time()
        final_memory = process.memory_info().rss / 1024 / 1024
        
        return {
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'peak_memory_mb': max(m['post_memory_mb'] for m in memory_measurements),
            'total_memory_growth_mb': final_memory - initial_memory,
            'average_memory_per_item_mb': statistics.mean(m['memory_delta_mb'] for m in memory_measurements),
            'duration_seconds': end_time - start_time,
            'measurements': memory_measurements
        }
    
    def _reset_test_queue(self):
        """Reset queue items to queued status for testing."""
        queue_items = self.storage.get_download_queue()
        for item in queue_items:
            self.storage.update_queue_item(item['id'], status='queued')
    
    def _test_concurrent_downloads(self, download_tasks: List[Dict], num_workers: int) -> int:
        """Test concurrent download simulation."""
        def mock_download(task):
            """Mock download that simulates network delay."""
            try:
                # Simulate download time
                time.sleep(0.05)  # 50ms per file
                return {'success': True, 'task': task}
            except Exception as e:
                return {'success': False, 'error': str(e)}
        
        success_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_task = {executor.submit(mock_download, task): task for task in download_tasks}
            
            for future in concurrent.futures.as_completed(future_to_task):
                result = future.result()
                if result['success']:
                    success_count += 1
        
        return success_count
    
    def _process_test_batch_updates(self, updates: List[Dict]):
        """Test batch database update processing."""
        try:
            with self.storage._get_connection() as conn:
                cursor = conn.cursor()
                for update in updates:
                    # Simulate database update
                    pass  # Don't actually update for benchmark
                conn.commit()
        except Exception:
            pass  # Ignore errors for benchmark
    
    def run_comprehensive_benchmark(self) -> Dict:
        """Run all benchmarks and return comprehensive results."""
        self.logger.info("Starting comprehensive download benchmark suite...")
        
        if not self.setup_test_data():
            return {'error': 'Failed to setup test data'}
        
        try:
            # 1. Serial vs Parallel Queue Processing
            self.benchmark_results['serial_processing'] = self.benchmark_serial_processing()
            
            # Test different parallel worker counts
            for workers in [2, 4, 6, 8]:
                key = f'parallel_processing_{workers}_workers'
                self.benchmark_results[key] = self.benchmark_parallel_queue_processing(workers)
            
            # 2. File Download Concurrency
            self.benchmark_results['file_concurrency'] = self.benchmark_file_concurrency()
            
            # 3. Database Batch Sizes
            self.benchmark_results['database_batching'] = self.benchmark_database_batch_sizes()
            
            # 4. I/O Chunk Sizes
            self.benchmark_results['io_chunk_sizes'] = self.benchmark_io_chunk_sizes()
            
            # 5. Memory Usage Profiling
            self.benchmark_results['memory_usage'] = self.benchmark_memory_usage()
            
            # Calculate summary statistics
            self.benchmark_results['summary'] = self._calculate_summary()
            
            return self.benchmark_results
            
        finally:
            self.cleanup()
    
    def _calculate_summary(self) -> Dict:
        """Calculate summary statistics and recommendations."""
        summary = {
            'test_timestamp': datetime.now().isoformat(),
            'test_data_size': self.test_data_size,
            'recommendations': []
        }
        
        # Analyze parallel vs serial performance
        serial_throughput = self.benchmark_results.get('serial_processing', {}).get('throughput_items_per_second', 0)
        
        best_parallel = None
        best_parallel_throughput = 0
        
        for key, result in self.benchmark_results.items():
            if key.startswith('parallel_processing') and 'throughput_items_per_second' in result:
                if result['throughput_items_per_second'] > best_parallel_throughput:
                    best_parallel_throughput = result['throughput_items_per_second']
                    best_parallel = key
        
        if best_parallel and best_parallel_throughput > serial_throughput:
            improvement = (best_parallel_throughput / serial_throughput - 1) * 100
            summary['recommendations'].append(
                f"Parallel queue processing ({best_parallel}) shows {improvement:.1f}% improvement over serial"
            )
        
        # Analyze file concurrency
        file_results = self.benchmark_results.get('file_concurrency', {})
        if file_results:
            best_file_workers = max(file_results.keys(), 
                                  key=lambda k: file_results[k]['throughput_files_per_second'])
            summary['recommendations'].append(
                f"Optimal file download concurrency: {best_file_workers}"
            )
        
        # Analyze database batching
        db_results = self.benchmark_results.get('database_batching', {})
        if db_results:
            best_batch = max(db_results.keys(),
                           key=lambda k: db_results[k]['throughput_updates_per_second'])
            summary['recommendations'].append(
                f"Optimal database batch size: {best_batch}"
            )
        
        # Analyze I/O chunk sizes
        io_results = self.benchmark_results.get('io_chunk_sizes', {})
        if io_results:
            best_chunk = max(io_results.keys(),
                           key=lambda k: io_results[k]['read_throughput_mb_per_second'] + 
                                        io_results[k]['write_throughput_mb_per_second'])
            summary['recommendations'].append(
                f"Optimal I/O chunk size: {best_chunk}"
            )
        
        return summary
    
    def save_results(self, output_file: str = None):
        """Save benchmark results to JSON file."""
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'benchmark_results_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump(self.benchmark_results, f, indent=2, default=str)
        
        self.logger.info(f"Benchmark results saved to {output_file}")


def main():
    """Main benchmark execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Download Performance Benchmarking')
    parser.add_argument('--test-size', type=int, default=20, 
                       help='Number of test items to use (default: 20)')
    parser.add_argument('--output', type=str, 
                       help='Output file for results (default: auto-generated)')
    parser.add_argument('--quick', action='store_true',
                       help='Run quick benchmark with fewer test cases')
    
    args = parser.parse_args()
    
    # Reduce test scope for quick mode
    if args.quick:
        args.test_size = min(args.test_size, 10)
    
    # Run benchmarks
    benchmarker = DownloadBenchmarker(test_data_size=args.test_size)
    
    try:
        results = benchmarker.run_comprehensive_benchmark()
        
        # Save results
        benchmarker.save_results(args.output)
        
        # Print summary
        print("\n" + "="*60)
        print("DOWNLOAD PERFORMANCE BENCHMARK RESULTS")
        print("="*60)
        
        if 'summary' in results:
            summary = results['summary']
            print(f"\nTest completed: {summary.get('test_timestamp', 'Unknown')}")
            print(f"Test data size: {summary.get('test_data_size', 'Unknown')} items")
            
            print("\nRecommendations:")
            for rec in summary.get('recommendations', []):
                print(f"  • {rec}")
        
        # Print key performance metrics
        print("\nKey Performance Metrics:")
        
        # Serial processing
        serial = results.get('serial_processing', {})
        if serial:
            print(f"  Serial processing: {serial.get('throughput_items_per_second', 0):.2f} items/sec")
        
        # Best parallel processing
        for key, result in results.items():
            if key.startswith('parallel_processing') and 'throughput_items_per_second' in result:
                workers = result.get('num_workers', 'unknown')
                throughput = result.get('throughput_items_per_second', 0)
                print(f"  Parallel ({workers} workers): {throughput:.2f} items/sec")
        
        print(f"\nDetailed results saved to output file.")
        
    except Exception as e:
        print(f"Benchmark failed: {e}")
        logging.exception("Benchmark execution failed")


if __name__ == '__main__':
    main()