"""
Download Processor Module

Handles the actual downloading of files from the Library of Congress API,
processing the download queue and managing file storage.
"""

import os
import logging
import requests
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from tqdm import tqdm
import time
import mimetypes

from .storage import NewsStorage
from .rate_limited_client import LocApiClient
from .utils import retry_on_network_failure, ProgressTracker


class DownloadProcessor:
    """Processes download queue and manages file downloads."""
    
    def __init__(self, storage: NewsStorage, api_client: LocApiClient, 
                 download_dir: str = "./downloads", 
                 file_types: List[str] = None):
        self.storage = storage
        self.api_client = api_client
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # Configure which file types to download
        # Default: download all available types
        self.file_types = file_types or ['pdf', 'jp2', 'ocr', 'metadata']
        
        # Set up download session with appropriate headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Newsagger/0.1.0 (Educational Archive Tool)'
        })
    
    def process_queue(self, max_items: int = None, max_size_mb: float = None, 
                     dry_run: bool = False) -> Dict:
        """
        Process the download queue.
        Returns statistics about the download session.
        """
        self.logger.info("Starting download queue processing...")
        
        # Get queued items ordered by priority
        queue_items = self.storage.get_download_queue(status='queued')
        if not queue_items:
            self.logger.info("No items in download queue")
            return {"downloaded": 0, "errors": 0, "skipped": 0, "total_size_mb": 0}
        
        # Apply limits
        if max_items:
            queue_items = queue_items[:max_items]
        
        # Filter by size limit
        if max_size_mb:
            filtered_items = []
            total_size = 0
            for item in queue_items:
                if total_size + item['estimated_size_mb'] <= max_size_mb:
                    filtered_items.append(item)
                    total_size += item['estimated_size_mb']
                else:
                    break
            queue_items = filtered_items
        
        if dry_run:
            self.logger.info(f"DRY RUN: Would download {len(queue_items)} items")
            total_size = sum(item['estimated_size_mb'] for item in queue_items)
            return {
                "would_download": len(queue_items),
                "estimated_size_mb": total_size,
                "dry_run": True
            }
        
        # Process downloads with progress tracking
        total_size_mb = 0
        start_time = datetime.now()
        
        with ProgressTracker(total=len(queue_items), desc="Processing downloads", unit="files") as progress:
            for item in queue_items:
                try:
                    # Mark item as active
                    self.storage.update_queue_item(item['id'], status='active')
                    
                    # Process the download based on queue type
                    result = self._process_queue_item(item)
                    
                    if result['success']:
                        total_size_mb += result.get('size_mb', 0)
                        
                        # Mark as completed
                        self.storage.update_queue_item(
                            item['id'], 
                            status='completed',
                            progress_percent=100.0
                        )
                        
                        # Update progress with custom postfix for size
                        progress.update(success=True)
                        progress.set_postfix(size_mb=f"{total_size_mb:.1f}")
                    else:
                        self.storage.update_queue_item(
                            item['id'],
                            status='failed',
                            error_message=result.get('error', 'Unknown error')
                        )
                        
                        progress.update(success=False)
                        progress.set_postfix(size_mb=f"{total_size_mb:.1f}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing queue item {item['id']}: {e}")
                    self.storage.update_queue_item(
                        item['id'],
                        status='failed',
                        error_message=str(e)
                    )
                    
                    progress.update(success=False)
                    progress.set_postfix(size_mb=f"{total_size_mb:.1f}")
        
        # Get final statistics from progress tracker and build return stats
        final_stats = progress.get_stats()
        end_time = datetime.now()
        
        # Build return stats in original format for compatibility
        stats = {
            "downloaded": final_stats['success'],
            "errors": final_stats['errors'],
            "skipped": final_stats['skipped'],
            "total_size_mb": total_size_mb,
            "start_time": start_time,
            "end_time": end_time,
            "duration_minutes": (end_time - start_time).total_seconds() / 60
        }
        
        self.logger.info(f"Download processing complete: {stats['downloaded']} downloaded, "
                        f"{stats['errors']} errors, {stats['total_size_mb']:.1f} MB total")
        
        return stats
    
    def _process_queue_item(self, queue_item: Dict) -> Dict:
        """Process a single queue item based on its type."""
        queue_type = queue_item['queue_type']
        reference_id = queue_item['reference_id']
        
        try:
            if queue_type == 'page':
                return self._download_page(reference_id)
            elif queue_type == 'facet':
                return self._download_facet_content(reference_id)
            elif queue_type == 'periodical':
                return self._download_periodical(reference_id)
            else:
                return {
                    'success': False,
                    'error': f"Unknown queue type: {queue_type}"
                }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _download_page(self, item_id: str) -> Dict:
        """Download a specific newspaper page."""
        # Get page metadata from storage
        pages = self.storage.get_pages()
        page_data = None
        for page in pages:
            if page['item_id'] == item_id:
                page_data = page
                break
        
        if not page_data:
            return {
                'success': False,
                'error': f"Page {item_id} not found in storage"
            }
        
        # Check if already downloaded
        if page_data.get('downloaded'):
            self.logger.debug(f"Page {item_id} already downloaded, skipping")
            return {
                'success': True,
                'skipped': True,
                'size_mb': 0
            }
        
        # Create directory structure: downloads/lccn/year/month/
        page_date = page_data['date']
        year = page_date[:4] if len(page_date) >= 4 else 'unknown'
        month = page_date[5:7] if len(page_date) >= 7 else 'unknown'
        
        download_path = self.download_dir / page_data['lccn'] / year / month
        download_path.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        total_size = 0
        
        # Clean the item_id to make it safe for filenames
        safe_item_id = item_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        
        # Download PDF if available and requested
        if 'pdf' in self.file_types and page_data.get('pdf_url'):
            pdf_result = self._download_file(
                page_data['pdf_url'],
                download_path / f"{safe_item_id}.pdf"
            )
            if pdf_result['success']:
                downloaded_files.append(pdf_result['file_path'])
                total_size += pdf_result['size_mb']
        
        # Download JP2 image if available and requested
        if 'jp2' in self.file_types and page_data.get('jp2_url'):
            jp2_result = self._download_file(
                page_data['jp2_url'],
                download_path / f"{safe_item_id}.jp2"
            )
            if jp2_result['success']:
                downloaded_files.append(jp2_result['file_path'])
                total_size += jp2_result['size_mb']
        
        # Save OCR text if available and requested
        if 'ocr' in self.file_types and page_data.get('ocr_text'):
            text_path = download_path / f"{safe_item_id}_ocr.txt"
            try:
                with open(text_path, 'w', encoding='utf-8') as f:
                    f.write(page_data['ocr_text'])
                downloaded_files.append(str(text_path))
            except Exception as e:
                self.logger.warning(f"Failed to save OCR text for {item_id}: {e}")
        
        # Save metadata if requested
        if 'metadata' in self.file_types:
            metadata_path = download_path / f"{safe_item_id}_metadata.json"
            try:
                import json
                metadata = {
                    'item_id': page_data['item_id'],
                    'lccn': page_data['lccn'],
                    'title': page_data['title'],
                    'date': page_data['date'],
                    'edition': page_data['edition'],
                    'sequence': page_data['sequence'],
                    'page_url': page_data['page_url'],
                    'download_date': datetime.now().isoformat(),
                    'files': downloaded_files,
                    'file_types_requested': self.file_types
                }
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2)
                downloaded_files.append(str(metadata_path))
            except Exception as e:
                self.logger.warning(f"Failed to save metadata for {item_id}: {e}")
        
        if downloaded_files:
            # Mark as downloaded in storage
            self.storage.mark_page_downloaded(item_id)
            
            return {
                'success': True,
                'size_mb': total_size,
                'files': downloaded_files
            }
        else:
            return {
                'success': False,
                'error': "No files were successfully downloaded"
            }
    
    def _download_facet_content(self, facet_id: str) -> Dict:
        """Download all content from a facet."""
        try:
            facet_id_int = int(facet_id)
        except ValueError:
            return {
                'success': False,
                'error': f"Invalid facet ID: {facet_id}"
            }
        
        # Get all pages for this facet that aren't downloaded
        pages = self.storage.get_pages_for_facet(facet_id_int, downloaded=False)
        if not pages:
            return {
                'success': True,
                'skipped': True,
                'message': "All pages already downloaded or no pages found"
            }
        
        downloaded_count = 0
        total_size = 0
        errors = []
        
        for page in pages:
            try:
                result = self._download_page(page['item_id'])
                if result['success'] and not result.get('skipped'):
                    downloaded_count += 1
                    total_size += result.get('size_mb', 0)
                elif not result['success']:
                    errors.append(f"Page {page['item_id']}: {result.get('error', 'Unknown error')}")
            except Exception as e:
                errors.append(f"Page {page['item_id']}: {str(e)}")
        
        if errors and len(errors) == len(pages):
            return {
                'success': False,
                'error': f"Failed to download any pages. Errors: {'; '.join(errors[:3])}"
            }
        
        return {
            'success': True,
            'downloaded_pages': downloaded_count,
            'size_mb': total_size,
            'errors': len(errors),
            'total_pages': len(pages)
        }
    
    def _download_periodical(self, lccn: str) -> Dict:
        """Download all available content for a periodical."""
        # Get all pages for this periodical that aren't downloaded
        pages = self.storage.get_pages(lccn=lccn, downloaded_only=False)
        undownloaded_pages = [p for p in pages if not p.get('downloaded')]
        
        if not undownloaded_pages:
            return {
                'success': True,
                'skipped': True,
                'message': "All pages already downloaded or no pages found"
            }
        
        downloaded_count = 0
        total_size = 0
        errors = []
        
        for page in undownloaded_pages:
            try:
                result = self._download_page(page['item_id'])
                if result['success'] and not result.get('skipped'):
                    downloaded_count += 1
                    total_size += result.get('size_mb', 0)
                elif not result['success']:
                    errors.append(f"Page {page['item_id']}: {result.get('error', 'Unknown error')}")
            except Exception as e:
                errors.append(f"Page {page['item_id']}: {str(e)}")
        
        return {
            'success': True,
            'downloaded_pages': downloaded_count,
            'size_mb': total_size,
            'errors': len(errors),
            'total_pages': len(undownloaded_pages)
        }
    
    @retry_on_network_failure(max_attempts=3, base_delay=2.0)
    def _perform_http_download(self, url: str, local_path: Path) -> int:
        """
        Perform the actual HTTP download with retry logic.
        Returns the number of bytes downloaded.
        Raises exceptions on failure (to be caught by retry decorator).
        """
        # Use the same rate limiting as the API client
        time.sleep(self.api_client.request_delay)
        
        # Download with streaming to handle large files
        response = self.session.get(url, stream=True, timeout=120)
        response.raise_for_status()
        
        # Get file size from headers
        total_size = int(response.headers.get('content-length', 0))
        
        # Write file with progress tracking
        downloaded_size = 0
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
        
        # Verify download
        if total_size > 0 and downloaded_size != total_size:
            local_path.unlink()  # Remove incomplete file
            raise requests.exceptions.RequestException(
                f"Download incomplete: {downloaded_size}/{total_size} bytes"
            )
        
        return downloaded_size

    def _download_file(self, url: str, local_path: Path) -> Dict:
        """Download a single file from URL to local path."""
        try:
            # Check if file already exists
            if local_path.exists():
                size_mb = local_path.stat().st_size / (1024 * 1024)
                return {
                    'success': True,
                    'file_path': str(local_path),
                    'size_mb': size_mb,
                    'skipped': True
                }
            
            # Perform download with retry logic
            downloaded_size = self._perform_http_download(url, local_path)
            size_mb = downloaded_size / (1024 * 1024)
            
            return {
                'success': True,
                'file_path': str(local_path),
                'size_mb': size_mb
            }
            
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': f"Download failed: {str(e)}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}"
            }
    
    def resume_failed_downloads(self) -> Dict:
        """Resume processing of failed download queue items."""
        failed_items = self.storage.get_download_queue(status='failed')
        if not failed_items:
            self.logger.info("No failed downloads to resume")
            return {"resumed": 0}
        
        self.logger.info(f"Resuming {len(failed_items)} failed downloads...")
        
        # Reset failed items to queued status
        for item in failed_items:
            self.storage.update_queue_item(
                item['id'],
                status='queued',
                error_message=None
            )
        
        return {"resumed": len(failed_items)}
    
    def reset_stuck_downloads(self) -> Dict:
        """Reset stuck active downloads back to queued status."""
        active_items = self.storage.get_download_queue(status='active')
        if not active_items:
            self.logger.info("No stuck downloads to reset")
            return {"reset": 0}
        
        self.logger.info(f"Resetting {len(active_items)} stuck active downloads...")
        
        # Reset active items to queued status
        for item in active_items:
            self.storage.update_queue_item(
                item['id'],
                status='queued',
                error_message=None,
                progress_percent=0
            )
        
        return {"reset": len(active_items)}
    
    def get_download_stats(self) -> Dict:
        """Get comprehensive download statistics."""
        queue_stats = self.storage.get_download_queue_stats()
        storage_stats = self.storage.get_storage_stats()
        
        # Calculate disk usage
        total_disk_usage = 0
        if self.download_dir.exists():
            for file_path in self.download_dir.rglob('*'):
                if file_path.is_file():
                    total_disk_usage += file_path.stat().st_size
        
        disk_usage_mb = total_disk_usage / (1024 * 1024)
        
        return {
            'queue_stats': queue_stats,
            'storage_stats': storage_stats,
            'disk_usage_mb': round(disk_usage_mb, 2),
            'download_directory': str(self.download_dir),
            'files_on_disk': sum(1 for _ in self.download_dir.rglob('*') if _.is_file()) if self.download_dir.exists() else 0
        }
    
    def cleanup_incomplete_downloads(self) -> Dict:
        """Remove incomplete or corrupted download files."""
        cleaned_files = 0
        freed_space_mb = 0
        
        if not self.download_dir.exists():
            return {"cleaned_files": 0, "freed_space_mb": 0}
        
        # Look for files that might be incomplete
        for file_path in self.download_dir.rglob('*'):
            if file_path.is_file():
                # Check for zero-byte files
                if file_path.stat().st_size == 0:
                    self.logger.debug(f"Removing zero-byte file: {file_path}")
                    file_path.unlink()
                    cleaned_files += 1
                
                # Check for very small PDF files (likely corrupted)
                elif file_path.suffix.lower() == '.pdf' and file_path.stat().st_size < 1024:
                    self.logger.debug(f"Removing suspiciously small PDF: {file_path}")
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    freed_space_mb += size_mb
                    file_path.unlink()
                    cleaned_files += 1
        
        return {
            "cleaned_files": cleaned_files,
            "freed_space_mb": round(freed_space_mb, 2)
        }