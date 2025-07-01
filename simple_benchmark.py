#!/usr/bin/env python3
"""
Simplified Download Performance Benchmark

Tests key bottlenecks without requiring full database setup.
"""

import time
import threading
import concurrent.futures
import statistics
import json
from datetime import datetime
from typing import Dict, List
import tempfile
import os


class SimpleBenchmarker:
    """Simplified benchmarking focusing on key bottlenecks."""
    
    def __init__(self):
        self.results = {}
        
    def benchmark_serial_vs_parallel_queue(self) -> Dict:
        """Compare serial vs parallel processing patterns."""
        print("Benchmarking serial vs parallel queue processing...")
        
        # Simulate queue items (realistic processing times)
        queue_items = list(range(20))  # 20 test items
        
        def process_item(item_id):
            """Simulate processing one queue item."""
            # Simulate API call + download time
            time.sleep(0.1)  # 100ms per item (realistic for dry run)
            return {'item_id': item_id, 'success': True}
        
        # Test 1: Serial Processing
        start_time = time.time()
        serial_results = []
        for item in queue_items:
            result = process_item(item)
            serial_results.append(result)
        serial_duration = time.time() - start_time
        
        # Test 2: Parallel Processing (different worker counts)
        parallel_results = {}
        for num_workers in [2, 4, 6, 8]:
            start_time = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_item = {executor.submit(process_item, item): item for item in queue_items}
                results = []
                for future in concurrent.futures.as_completed(future_to_item):
                    result = future.result()
                    results.append(result)
            
            parallel_duration = time.time() - start_time
            
            parallel_results[f'{num_workers}_workers'] = {
                'duration_seconds': parallel_duration,
                'speedup_factor': serial_duration / parallel_duration,
                'throughput_items_per_second': len(queue_items) / parallel_duration
            }
        
        return {
            'serial': {
                'duration_seconds': serial_duration,
                'throughput_items_per_second': len(queue_items) / serial_duration
            },
            'parallel': parallel_results
        }
    
    def benchmark_file_download_concurrency(self) -> Dict:
        """Test different levels of file download concurrency."""
        print("Benchmarking file download concurrency...")
        
        # Simulate file downloads (multiple files per page item)
        file_tasks = []
        for page in range(5):  # 5 pages
            for file_type in ['pdf', 'jp2', 'metadata']:  # 3 files per page
                file_tasks.append({
                    'page': page,
                    'type': file_type,
                    'size_mb': 2.0 if file_type == 'jp2' else 0.5
                })
        
        def download_file(task):
            """Simulate downloading one file."""
            # Simulate download time based on file size
            download_time = task['size_mb'] * 0.02  # 20ms per MB
            time.sleep(download_time)
            return {'success': True, 'size_mb': task['size_mb']}
        
        results = {}
        for num_workers in [3, 6, 9, 12, 16]:
            start_time = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_task = {executor.submit(download_file, task): task for task in file_tasks}
                download_results = []
                for future in concurrent.futures.as_completed(future_to_task):
                    result = future.result()
                    download_results.append(result)
            
            duration = time.time() - start_time
            total_size = sum(r['size_mb'] for r in download_results if r['success'])
            
            results[f'{num_workers}_workers'] = {
                'duration_seconds': duration,
                'files_processed': len(download_results),
                'total_size_mb': total_size,
                'throughput_mb_per_second': total_size / duration,
                'throughput_files_per_second': len(file_tasks) / duration
            }
        
        return results
    
    def benchmark_database_batch_sizes(self) -> Dict:
        """Test different database batch update sizes."""
        print("Benchmarking database batch sizes...")
        
        # Simulate database updates
        total_updates = 200
        
        def process_batch_update(updates_batch):
            """Simulate processing a batch of database updates."""
            # Simulate database transaction overhead + per-item processing
            base_overhead = 0.01  # 10ms base transaction overhead
            per_item_time = 0.0005  # 0.5ms per item
            time.sleep(base_overhead + len(updates_batch) * per_item_time)
            return len(updates_batch)
        
        results = {}
        for batch_size in [10, 25, 50, 100]:
            start_time = time.time()
            
            # Process all updates in batches
            processed_count = 0
            for i in range(0, total_updates, batch_size):
                batch = list(range(i, min(i + batch_size, total_updates)))
                processed_count += process_batch_update(batch)
            
            duration = time.time() - start_time
            
            results[f'batch_size_{batch_size}'] = {
                'duration_seconds': duration,
                'updates_processed': processed_count,
                'throughput_updates_per_second': processed_count / duration,
                'batch_size': batch_size,
                'num_batches': (total_updates + batch_size - 1) // batch_size
            }
        
        return results
    
    def benchmark_io_chunk_sizes(self) -> Dict:
        """Test different I/O chunk sizes for file operations."""
        print("Benchmarking I/O chunk sizes...")
        
        # Create test data
        test_data_mb = 10  # 10MB test file
        test_data = b'x' * (test_data_mb * 1024 * 1024)
        
        results = {}
        chunk_sizes = [8192, 32768, 65536, 131072, 262144]  # 8KB to 256KB
        
        for chunk_size in chunk_sizes:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_path = temp_file.name
            
            try:
                # Write test
                start_time = time.time()
                with open(temp_path, 'wb') as f:
                    for i in range(0, len(test_data), chunk_size):
                        chunk = test_data[i:i + chunk_size]
                        f.write(chunk)
                write_duration = time.time() - start_time
                
                # Read test
                start_time = time.time()
                read_data = b''
                with open(temp_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        read_data += chunk
                read_duration = time.time() - start_time
                
                results[f'chunk_{chunk_size}'] = {
                    'chunk_size_bytes': chunk_size,
                    'chunk_size_kb': chunk_size / 1024,
                    'write_duration_seconds': write_duration,
                    'read_duration_seconds': read_duration,
                    'total_duration_seconds': write_duration + read_duration,
                    'write_throughput_mb_per_second': test_data_mb / write_duration,
                    'read_throughput_mb_per_second': test_data_mb / read_duration,
                    'data_size_mb': test_data_mb
                }
                
            finally:
                # Cleanup
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
        
        return results
    
    def benchmark_memory_overhead(self) -> Dict:
        """Test memory overhead of different concurrency patterns."""
        print("Benchmarking memory overhead...")
        
        try:
            import psutil
            process = psutil.Process()
        except ImportError:
            print("psutil not available, skipping memory benchmark")
            return {"error": "psutil not available"}
        
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        def memory_intensive_task(task_id):
            """Simulate a memory-intensive task."""
            # Simulate holding some data in memory
            data = [i for i in range(10000)]  # Small data structure
            time.sleep(0.01)  # Brief processing
            return len(data)
        
        results = {}
        for num_workers in [1, 4, 8, 16]:
            # Measure memory before test
            pre_memory = process.memory_info().rss / 1024 / 1024
            
            start_time = time.time()
            tasks = list(range(50))  # 50 tasks
            
            if num_workers == 1:
                # Serial execution
                for task in tasks:
                    memory_intensive_task(task)
            else:
                # Parallel execution
                with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                    future_to_task = {executor.submit(memory_intensive_task, task): task for task in tasks}
                    for future in concurrent.futures.as_completed(future_to_task):
                        future.result()
            
            duration = time.time() - start_time
            post_memory = process.memory_info().rss / 1024 / 1024
            peak_memory = post_memory  # Simplified - real implementation would track peak
            
            results[f'{num_workers}_workers'] = {
                'duration_seconds': duration,
                'pre_memory_mb': pre_memory,
                'post_memory_mb': post_memory,
                'peak_memory_mb': peak_memory,
                'memory_delta_mb': post_memory - pre_memory,
                'memory_per_worker_mb': (post_memory - pre_memory) / max(1, num_workers - 1) if num_workers > 1 else 0,
                'num_workers': num_workers
            }
        
        return results
    
    def calculate_recommendations(self) -> List[str]:
        """Analyze results and provide optimization recommendations."""
        recommendations = []
        
        # Analyze queue processing
        if 'queue_processing' in self.results:
            serial_throughput = self.results['queue_processing']['serial']['throughput_items_per_second']
            
            best_parallel = None
            best_throughput = 0
            for key, data in self.results['queue_processing']['parallel'].items():
                if data['throughput_items_per_second'] > best_throughput:
                    best_throughput = data['throughput_items_per_second']
                    best_parallel = key
            
            if best_parallel and best_throughput > serial_throughput:
                improvement = (best_throughput / serial_throughput - 1) * 100
                recommendations.append(
                    f"Use parallel queue processing with {best_parallel}: {improvement:.1f}% faster than serial"
                )
            else:
                recommendations.append("Serial queue processing is sufficient - bottleneck is elsewhere")
        
        # Analyze file concurrency
        if 'file_concurrency' in self.results:
            best_file_workers = max(
                self.results['file_concurrency'].keys(),
                key=lambda k: self.results['file_concurrency'][k]['throughput_mb_per_second']
            )
            best_throughput = self.results['file_concurrency'][best_file_workers]['throughput_mb_per_second']
            recommendations.append(
                f"Optimal file download concurrency: {best_file_workers} ({best_throughput:.1f} MB/s)"
            )
        
        # Analyze database batching
        if 'database_batching' in self.results:
            best_batch = max(
                self.results['database_batching'].keys(),
                key=lambda k: self.results['database_batching'][k]['throughput_updates_per_second']
            )
            best_throughput = self.results['database_batching'][best_batch]['throughput_updates_per_second']
            recommendations.append(
                f"Optimal database batch size: {best_batch} ({best_throughput:.0f} updates/s)"
            )
        
        # Analyze I/O chunk sizes
        if 'io_chunk_sizes' in self.results:
            best_chunk = max(
                self.results['io_chunk_sizes'].keys(),
                key=lambda k: (self.results['io_chunk_sizes'][k]['read_throughput_mb_per_second'] + 
                              self.results['io_chunk_sizes'][k]['write_throughput_mb_per_second'])
            )
            best_total_throughput = (
                self.results['io_chunk_sizes'][best_chunk]['read_throughput_mb_per_second'] +
                self.results['io_chunk_sizes'][best_chunk]['write_throughput_mb_per_second']
            )
            chunk_size_kb = self.results['io_chunk_sizes'][best_chunk]['chunk_size_kb']
            recommendations.append(
                f"Optimal I/O chunk size: {chunk_size_kb:.0f}KB ({best_total_throughput:.1f} MB/s total)"
            )
        
        return recommendations
    
    def run_all_benchmarks(self) -> Dict:
        """Run all benchmarks and return comprehensive results."""
        print("Starting comprehensive download performance benchmark...\n")
        
        self.results['queue_processing'] = self.benchmark_serial_vs_parallel_queue()
        print()
        
        self.results['file_concurrency'] = self.benchmark_file_download_concurrency()
        print()
        
        self.results['database_batching'] = self.benchmark_database_batch_sizes()
        print()
        
        self.results['io_chunk_sizes'] = self.benchmark_io_chunk_sizes()
        print()
        
        self.results['memory_overhead'] = self.benchmark_memory_overhead()
        print()
        
        # Generate recommendations
        self.results['recommendations'] = self.calculate_recommendations()
        self.results['timestamp'] = datetime.now().isoformat()
        
        return self.results
    
    def print_summary(self):
        """Print a summary of benchmark results."""
        print("=" * 60)
        print("DOWNLOAD PERFORMANCE BENCHMARK RESULTS")
        print("=" * 60)
        
        print(f"\nBenchmark completed: {self.results.get('timestamp', 'Unknown')}")
        
        print("\nKEY FINDINGS:")
        print("-" * 40)
        
        # Queue processing comparison
        if 'queue_processing' in self.results:
            serial_tput = self.results['queue_processing']['serial']['throughput_items_per_second']
            print(f"Serial queue processing:    {serial_tput:.2f} items/sec")
            
            for key, data in self.results['queue_processing']['parallel'].items():
                tput = data['throughput_items_per_second']
                speedup = data['speedup_factor']
                print(f"Parallel {key:>11}: {tput:.2f} items/sec ({speedup:.1f}x speedup)")
        
        # File concurrency sweet spot
        if 'file_concurrency' in self.results:
            print(f"\nFile Download Concurrency:")
            for key, data in self.results['file_concurrency'].items():
                mb_per_sec = data['throughput_mb_per_second']
                files_per_sec = data['throughput_files_per_second']
                print(f"  {key:>12}: {mb_per_sec:.1f} MB/s, {files_per_sec:.1f} files/s")
        
        # Database batching impact
        if 'database_batching' in self.results:
            print(f"\nDatabase Batch Size Impact:")
            for key, data in self.results['database_batching'].items():
                updates_per_sec = data['throughput_updates_per_second']
                batch_size = data['batch_size']
                print(f"  Batch size {batch_size:>3}: {updates_per_sec:.0f} updates/sec")
        
        # I/O optimization
        if 'io_chunk_sizes' in self.results:
            print(f"\nI/O Chunk Size Performance:")
            for key, data in self.results['io_chunk_sizes'].items():
                chunk_kb = data['chunk_size_kb']
                total_tput = data['read_throughput_mb_per_second'] + data['write_throughput_mb_per_second']
                print(f"  {chunk_kb:>6.0f}KB chunks: {total_tput:.1f} MB/s total throughput")
        
        print("\nRECOMMENDATIONS:")
        print("-" * 40)
        for i, rec in enumerate(self.results.get('recommendations', []), 1):
            print(f"{i}. {rec}")
    
    def save_results(self, filename: str = None):
        """Save detailed results to JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'benchmark_results_{timestamp}.json'
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\nDetailed results saved to: {filename}")


def main():
    """Run the simplified benchmark suite."""
    benchmarker = SimpleBenchmarker()
    
    try:
        results = benchmarker.run_all_benchmarks()
        benchmarker.print_summary()
        benchmarker.save_results()
        
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
    except Exception as e:
        print(f"Benchmark failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()