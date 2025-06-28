"""
Data Storage Layer

Handles persistent storage of newspaper metadata and download progress
using SQLite database.
"""

import sqlite3
import logging
import json
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
from .processor import NewspaperInfo, PageInfo
from .utils import DatabaseOperationMixin


class NewsStorage(DatabaseOperationMixin):
    """SQLite-based storage for news archive data."""
    
    def __init__(self, db_path: str = "./data/newsagger.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self._init_database()
    
    def _migrate_database(self, conn):
        """Add new columns for enhanced batch-level resume functionality."""
        try:
            # Add batch tracking columns to search_facets table
            cursor = conn.cursor()
            
            # Check if columns exist before adding them
            cursor.execute("PRAGMA table_info(search_facets)")
            existing_columns = [column[1] for column in cursor.fetchall()]
            
            if 'current_page' not in existing_columns:
                cursor.execute("ALTER TABLE search_facets ADD COLUMN current_page INTEGER DEFAULT 1")
                self.logger.info("Added current_page column for batch-level resume")
            
            if 'last_batch_size' not in existing_columns:
                cursor.execute("ALTER TABLE search_facets ADD COLUMN last_batch_size INTEGER DEFAULT 100")
                self.logger.info("Added last_batch_size column for batch-level resume")
            
            if 'last_successful_batch' not in existing_columns:
                cursor.execute("ALTER TABLE search_facets ADD COLUMN last_successful_batch TIMESTAMP")
                self.logger.info("Added last_successful_batch column for batch-level resume")
            
            if 'resume_from_page' not in existing_columns:
                cursor.execute("ALTER TABLE search_facets ADD COLUMN resume_from_page INTEGER DEFAULT 1")
                self.logger.info("Added resume_from_page column for batch-level resume")
                
            conn.commit()
            
        except Exception as e:
            self.logger.warning(f"Database migration failed: {e}")
    
    def _get_connection(self):
        """Get a database connection for context manager usage."""
        return sqlite3.connect(self.db_path)
    
    def _init_database(self):
        """Initialize database tables."""
        with sqlite3.connect(self.db_path) as conn:
            # Check and add new columns for batch-level resume functionality
            self._migrate_database(conn)
            
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS newspapers (
                    lccn TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    place_of_publication TEXT,
                    start_year INTEGER,
                    end_year INTEGER,
                    frequency TEXT,
                    subject TEXT,
                    language TEXT,
                    url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS pages (
                    item_id TEXT PRIMARY KEY,
                    lccn TEXT,
                    title TEXT,
                    date TEXT,
                    edition INTEGER,
                    sequence INTEGER,
                    page_url TEXT,
                    pdf_url TEXT,
                    jp2_url TEXT,
                    ocr_text TEXT,
                    word_count INTEGER,
                    downloaded BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (lccn) REFERENCES newspapers (lccn)
                );
                
                CREATE TABLE IF NOT EXISTS download_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_name TEXT UNIQUE,
                    query_params TEXT,
                    total_expected INTEGER,
                    total_downloaded INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'active',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                );
                
                -- New table: Track available periodicals (newspapers) for discovery and download planning
                CREATE TABLE IF NOT EXISTS periodicals (
                    lccn TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    state TEXT,
                    city TEXT,
                    start_year INTEGER,
                    end_year INTEGER,
                    frequency TEXT,
                    language TEXT,
                    subject TEXT,
                    url TEXT,
                    total_issues INTEGER DEFAULT 0,
                    issues_discovered INTEGER DEFAULT 0,
                    issues_downloaded INTEGER DEFAULT 0,
                    last_discovery_scan TIMESTAMP,
                    last_download_scan TIMESTAMP,
                    discovery_complete BOOLEAN DEFAULT FALSE,
                    download_complete BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- New table: Track search facets and date ranges for systematic downloading
                CREATE TABLE IF NOT EXISTS search_facets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    facet_type TEXT NOT NULL,  -- 'date_range', 'state', 'subject', 'language'
                    facet_value TEXT NOT NULL, -- '1906/1906', 'California', etc.
                    facet_query TEXT,         -- Original search query that created this facet
                    estimated_items INTEGER DEFAULT 0,
                    actual_items INTEGER DEFAULT 0,
                    items_discovered INTEGER DEFAULT 0,
                    items_downloaded INTEGER DEFAULT 0,
                    discovery_started TIMESTAMP,
                    discovery_completed TIMESTAMP,
                    download_started TIMESTAMP,
                    download_completed TIMESTAMP,
                    status TEXT DEFAULT 'pending', -- 'pending', 'discovering', 'downloading', 'completed', 'error'
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(facet_type, facet_value, facet_query)
                );
                
                -- New table: Track specific issues (publication dates) per periodical
                CREATE TABLE IF NOT EXISTS periodical_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lccn TEXT NOT NULL,
                    issue_date TEXT NOT NULL,  -- YYYY-MM-DD format
                    edition_count INTEGER DEFAULT 0,
                    pages_count INTEGER DEFAULT 0,
                    pages_discovered INTEGER DEFAULT 0,
                    pages_downloaded INTEGER DEFAULT 0,
                    discovery_complete BOOLEAN DEFAULT FALSE,
                    download_complete BOOLEAN DEFAULT FALSE,
                    issue_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (lccn) REFERENCES periodicals (lccn),
                    UNIQUE(lccn, issue_date)
                );
                
                -- New table: Track download queues and priorities
                CREATE TABLE IF NOT EXISTS download_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_type TEXT NOT NULL, -- 'facet', 'periodical', 'issue', 'custom'
                    reference_id TEXT NOT NULL, -- ID of the facet, lccn, or custom identifier
                    priority INTEGER DEFAULT 5, -- 1=highest, 10=lowest
                    estimated_size_mb INTEGER DEFAULT 0,
                    estimated_time_hours REAL DEFAULT 0,
                    status TEXT DEFAULT 'queued', -- 'queued', 'active', 'paused', 'completed', 'failed'
                    progress_percent REAL DEFAULT 0,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_pages_lccn ON pages(lccn);
                CREATE INDEX IF NOT EXISTS idx_pages_date ON pages(date);
                CREATE INDEX IF NOT EXISTS idx_pages_downloaded ON pages(downloaded);
                CREATE INDEX IF NOT EXISTS idx_periodicals_state ON periodicals(state);
                CREATE INDEX IF NOT EXISTS idx_periodicals_discovery ON periodicals(discovery_complete);
                CREATE INDEX IF NOT EXISTS idx_periodicals_download ON periodicals(download_complete);
                CREATE INDEX IF NOT EXISTS idx_facets_status ON search_facets(status);
                CREATE INDEX IF NOT EXISTS idx_facets_type ON search_facets(facet_type);
                CREATE INDEX IF NOT EXISTS idx_issues_lccn ON periodical_issues(lccn);
                CREATE INDEX IF NOT EXISTS idx_issues_date ON periodical_issues(issue_date);
                CREATE INDEX IF NOT EXISTS idx_queue_status ON download_queue(status);
                CREATE INDEX IF NOT EXISTS idx_queue_priority ON download_queue(priority);
            """)
    
    def store_newspapers(self, newspapers: List[NewspaperInfo]) -> int:
        """Store newspaper metadata, return number of new records."""
        with sqlite3.connect(self.db_path) as conn:
            inserted = 0
            for newspaper in newspapers:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO newspapers 
                        (lccn, title, place_of_publication, start_year, end_year, 
                         frequency, subject, language, url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        newspaper.lccn,
                        newspaper.title,
                        json.dumps(newspaper.place_of_publication),
                        newspaper.start_year,
                        newspaper.end_year,
                        newspaper.frequency,
                        json.dumps(newspaper.subject),
                        json.dumps(newspaper.language),
                        newspaper.url
                    ))
                    inserted += 1
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to store newspaper {newspaper.lccn}: {e}")
            
            conn.commit()
            return inserted
    
    def store_pages(self, pages: List[PageInfo]) -> int:
        """Store page metadata, return number of new records."""
        with sqlite3.connect(self.db_path) as conn:
            inserted = 0
            for page in pages:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO pages 
                        (item_id, lccn, title, date, edition, sequence, 
                         page_url, pdf_url, jp2_url, ocr_text, word_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        page.item_id,
                        page.lccn,
                        page.title,
                        page.date,
                        page.edition,
                        page.sequence,
                        page.page_url,
                        page.pdf_url,
                        page.jp2_url,
                        page.ocr_text,
                        page.word_count
                    ))
                    inserted += 1
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to store page {page.item_id}: {e}")
            
            conn.commit()
            return inserted
    
    def get_newspapers(self, state: str = None, language: str = None) -> List[Dict]:
        """Retrieve newspapers with optional filtering."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM newspapers"
            params = []
            conditions = []
            
            if state:
                conditions.append("place_of_publication LIKE ?")
                params.append(f'%{state}%')
            
            if language:
                conditions.append("language LIKE ?")
                params.append(f'%{language}%')
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY title"
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_pages(self, lccn: str = None, date_range: Tuple[str, str] = None, 
                  downloaded_only: bool = False, limit: int = None) -> List[Dict]:
        """Retrieve pages with optional filtering."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM pages"
            params = []
            conditions = []
            
            if lccn:
                conditions.append("lccn = ?")
                params.append(lccn)
            
            if date_range:
                conditions.append("date BETWEEN ? AND ?")
                params.extend(date_range)
            
            if downloaded_only:
                conditions.append("downloaded = TRUE")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY date, sequence"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_page_downloaded(self, item_id: str):
        """Mark a page as downloaded."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE pages SET downloaded = TRUE WHERE item_id = ?", (item_id,))
            conn.commit()
    
    def create_download_session(self, session_name: str, query_params: Dict, 
                              total_expected: int) -> int:
        """Create a new download session."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO download_sessions (session_name, query_params, total_expected)
                VALUES (?, ?, ?)
            """, (session_name, json.dumps(query_params), total_expected))
            conn.commit()
            return cursor.lastrowid
    
    def update_session_progress(self, session_id: int, downloaded_count: int):
        """Update download session progress."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE download_sessions 
                SET total_downloaded = ? 
                WHERE id = ?
            """, (downloaded_count, session_id))
            conn.commit()
    
    def complete_session(self, session_id: int):
        """Mark download session as completed."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE download_sessions 
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            """, (session_id,))
            conn.commit()
    
    def get_session_stats(self, session_id: int) -> Optional[Dict]:
        """Get download session statistics."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM download_sessions WHERE id = ?
            """, (session_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_storage_stats(self) -> Dict:
        """Get overall storage statistics."""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            
            # Count newspapers
            cursor = conn.execute("SELECT COUNT(*) FROM newspapers")
            stats['total_newspapers'] = cursor.fetchone()[0]
            
            # Count pages
            cursor = conn.execute("SELECT COUNT(*) FROM pages")
            stats['total_pages'] = cursor.fetchone()[0]
            
            # Count downloaded pages
            cursor = conn.execute("SELECT COUNT(*) FROM pages WHERE downloaded = TRUE")
            stats['downloaded_pages'] = cursor.fetchone()[0]
            
            # Active sessions
            cursor = conn.execute("SELECT COUNT(*) FROM download_sessions WHERE status = 'active'")
            stats['active_sessions'] = cursor.fetchone()[0]
            
            # Database size
            stats['db_size_mb'] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
            
            return stats
    
    # ===== PERIODICAL TRACKING METHODS =====
    
    def store_periodicals(self, periodicals: List[Dict]) -> int:
        """Store periodical metadata for tracking discovery and download progress."""
        with sqlite3.connect(self.db_path) as conn:
            inserted = 0
            for periodical in periodicals:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO periodicals 
                        (lccn, title, state, city, start_year, end_year, frequency, 
                         language, subject, url, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        periodical.get('lccn'),
                        periodical.get('title'),
                        periodical.get('state'),
                        periodical.get('city'),
                        periodical.get('start_year'),
                        periodical.get('end_year'),
                        periodical.get('frequency'),
                        periodical.get('language'),
                        periodical.get('subject'),
                        periodical.get('url')
                    ))
                    inserted += 1
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to store periodical {periodical.get('lccn', 'unknown')}: {e}")
            
            conn.commit()
            return inserted
    
    def get_periodicals(self, state: str = None, discovery_complete: bool = None, 
                       download_complete: bool = None) -> List[Dict]:
        """Get periodicals with optional filtering."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM periodicals"
            params = []
            conditions = []
            
            if state:
                conditions.append("state = ?")
                params.append(state)
            
            if discovery_complete is not None:
                conditions.append("discovery_complete = ?")
                params.append(discovery_complete)
                
            if download_complete is not None:
                conditions.append("download_complete = ?")
                params.append(download_complete)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY state, title"
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def update_periodical_discovery(self, lccn: str, total_issues: int = None, 
                                  issues_discovered: int = None, complete: bool = False):
        """Update periodical discovery progress."""
        # Build update parameters
        updates = {}
        if total_issues is not None:
            updates['total_issues'] = total_issues
        if issues_discovered is not None:
            updates['issues_discovered'] = issues_discovered
        if complete:
            updates['discovery_complete'] = True
            updates['last_discovery_scan'] = 'CURRENT_TIMESTAMP'
        
        # Use mixin helper for the actual update
        self._build_dynamic_update('periodicals', 'lccn', lccn, **updates)
    
    def update_periodical_download(self, lccn: str, issues_downloaded: int = None, 
                                 complete: bool = False):
        """Update periodical download progress."""
        with sqlite3.connect(self.db_path) as conn:
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params = []
            
            if issues_downloaded is not None:
                updates.append("issues_downloaded = ?")
                params.append(issues_downloaded)
                
            if complete:
                updates.append("download_complete = TRUE")
                updates.append("last_download_scan = CURRENT_TIMESTAMP")
            
            params.append(lccn)
            
            conn.execute(f"""
                UPDATE periodicals 
                SET {', '.join(updates)}
                WHERE lccn = ?
            """, params)
            conn.commit()
    
    # ===== SEARCH FACET TRACKING METHODS =====
    
    def create_search_facet(self, facet_type: str, facet_value: str, 
                          facet_query: str = None, estimated_items: int = 0) -> int:
        """Create a new search facet for tracking."""
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.execute("""
                    INSERT INTO search_facets 
                    (facet_type, facet_value, facet_query, estimated_items)
                    VALUES (?, ?, ?, ?)
                """, (facet_type, facet_value, facet_query, estimated_items))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError:
                # Facet already exists, get its ID
                cursor = conn.execute("""
                    SELECT id FROM search_facets 
                    WHERE facet_type = ? AND facet_value = ? AND facet_query = ?
                """, (facet_type, facet_value, facet_query))
                return cursor.fetchone()[0]
    
    def get_search_facets(self, facet_type: str = None, status = None) -> List[Dict]:
        """Get search facets with optional filtering."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM search_facets"
            params = []
            conditions = []
            
            if facet_type:
                conditions.append("facet_type = ?")
                params.append(facet_type)
            
            if status:
                if isinstance(status, list):
                    # Handle list of statuses with IN clause
                    placeholders = ','.join(['?' for _ in status])
                    conditions.append(f"status IN ({placeholders})")
                    params.extend(status)
                else:
                    # Handle single status
                    conditions.append("status = ?")
                    params.append(status)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY facet_type, facet_value"
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def update_facet_discovery(self, facet_id: int, actual_items: int = None, 
                             items_discovered: int = None, status: str = None, 
                             error_message: str = None, current_page: int = None, 
                             batch_size: int = None):
        """Update facet discovery progress with batch-level tracking."""
        with sqlite3.connect(self.db_path) as conn:
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params = []
            
            if actual_items is not None:
                updates.append("actual_items = ?")
                params.append(actual_items)
                
            if items_discovered is not None:
                updates.append("items_discovered = ?")
                params.append(items_discovered)
                
            if status:
                updates.append("status = ?")
                params.append(status)
                
                if status == 'discovering' and not error_message:
                    updates.append("discovery_started = CURRENT_TIMESTAMP")
                elif status == 'completed' and not error_message:
                    updates.append("discovery_completed = CURRENT_TIMESTAMP")
                    
            if error_message is not None:
                updates.append("error_message = ?")
                params.append(error_message)
            
            # Add batch tracking updates
            if current_page is not None:
                updates.append("current_page = ?")
                params.append(current_page)
                # Update resume_from_page to current_page for potential resume
                updates.append("resume_from_page = ?")
                params.append(current_page)
            
            if batch_size is not None:
                updates.append("last_batch_size = ?")
                params.append(batch_size)
                updates.append("last_successful_batch = CURRENT_TIMESTAMP")
            
            params.append(facet_id)
            
            conn.execute(f"""
                UPDATE search_facets 
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
    
    def update_facet_download(self, facet_id: int, items_downloaded: int = None, 
                            status: str = None, error_message: str = None):
        """Update facet download progress."""
        with sqlite3.connect(self.db_path) as conn:
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params = []
            
            if items_downloaded is not None:
                updates.append("items_downloaded = ?")
                params.append(items_downloaded)
                
            if status:
                updates.append("status = ?")
                params.append(status)
                
                if status == 'downloading' and not error_message:
                    updates.append("download_started = CURRENT_TIMESTAMP")
                elif status == 'completed' and not error_message:
                    updates.append("download_completed = CURRENT_TIMESTAMP")
                    
            if error_message is not None:
                updates.append("error_message = ?")
                params.append(error_message)
            
            params.append(facet_id)
            
            conn.execute(f"""
                UPDATE search_facets 
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
    
    # ===== PERIODICAL ISSUE TRACKING METHODS =====
    
    def store_periodical_issue(self, lccn: str, issue_date: str, edition_count: int = 0, 
                             pages_count: int = 0, issue_url: str = None) -> int:
        """Store information about a specific newspaper issue."""
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.execute("""
                    INSERT INTO periodical_issues 
                    (lccn, issue_date, edition_count, pages_count, issue_url)
                    VALUES (?, ?, ?, ?, ?)
                """, (lccn, issue_date, edition_count, pages_count, issue_url))
                conn.commit()
                return cursor.lastrowid
            except sqlite3.IntegrityError:
                # Issue already exists, update it
                conn.execute("""
                    UPDATE periodical_issues 
                    SET edition_count = ?, pages_count = ?, issue_url = ?, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE lccn = ? AND issue_date = ?
                """, (edition_count, pages_count, issue_url, lccn, issue_date))
                conn.commit()
                
                # Get the existing ID
                cursor = conn.execute("""
                    SELECT id FROM periodical_issues 
                    WHERE lccn = ? AND issue_date = ?
                """, (lccn, issue_date))
                return cursor.fetchone()[0]
    
    def get_periodical_issues(self, lccn: str = None, date_range: Tuple[str, str] = None, 
                            discovery_complete: bool = None) -> List[Dict]:
        """Get periodical issues with optional filtering."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM periodical_issues"
            params = []
            conditions = []
            
            if lccn:
                conditions.append("lccn = ?")
                params.append(lccn)
            
            if date_range:
                conditions.append("issue_date BETWEEN ? AND ?")
                params.extend(date_range)
                
            if discovery_complete is not None:
                conditions.append("discovery_complete = ?")
                params.append(discovery_complete)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY lccn, issue_date"
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def update_issue_progress(self, issue_id: int, pages_discovered: int = None, 
                            pages_downloaded: int = None, discovery_complete: bool = None,
                            download_complete: bool = None):
        """Update progress for a specific newspaper issue."""
        with sqlite3.connect(self.db_path) as conn:
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params = []
            
            if pages_discovered is not None:
                updates.append("pages_discovered = ?")
                params.append(pages_discovered)
                
            if pages_downloaded is not None:
                updates.append("pages_downloaded = ?")
                params.append(pages_downloaded)
                
            if discovery_complete is not None:
                updates.append("discovery_complete = ?")
                params.append(discovery_complete)
                
            if download_complete is not None:
                updates.append("download_complete = ?")
                params.append(download_complete)
            
            params.append(issue_id)
            
            conn.execute(f"""
                UPDATE periodical_issues 
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
    
    # ===== DOWNLOAD QUEUE METHODS =====
    
    def add_to_download_queue(self, queue_type: str, reference_id: str, 
                            priority: int = 5, estimated_size_mb: int = 0, 
                            estimated_time_hours: float = 0) -> int:
        """Add item to download queue."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO download_queue 
                (queue_type, reference_id, priority, estimated_size_mb, estimated_time_hours)
                VALUES (?, ?, ?, ?, ?)
            """, (queue_type, reference_id, priority, estimated_size_mb, estimated_time_hours))
            conn.commit()
            return cursor.lastrowid
    
    def get_download_queue(self, status: str = None, limit: int = None) -> List[Dict]:
        """Get download queue items, ordered by priority."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM download_queue"
            params = []
            
            if status:
                query += " WHERE status = ?"
                params.append(status)
            
            query += " ORDER BY priority ASC, created_at ASC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def update_queue_item(self, queue_id: int, status: str = None, 
                         progress_percent: float = None, error_message: str = None):
        """Update download queue item status."""
        with sqlite3.connect(self.db_path) as conn:
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params = []
            
            if status:
                updates.append("status = ?")
                params.append(status)
                
                if status == 'active':
                    updates.append("started_at = CURRENT_TIMESTAMP")
                elif status == 'completed':
                    updates.append("completed_at = CURRENT_TIMESTAMP")
                    updates.append("progress_percent = 100")
                    
            if progress_percent is not None:
                updates.append("progress_percent = ?")
                params.append(progress_percent)
                
            if error_message is not None:
                updates.append("error_message = ?")
                params.append(error_message)
            
            params.append(queue_id)
            
            conn.execute(f"""
                UPDATE download_queue 
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
    
    # ===== ENHANCED STATISTICS METHODS =====
    
    def get_discovery_stats(self) -> Dict:
        """Get comprehensive discovery and download statistics."""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            
            # Periodical stats
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_periodicals,
                    COUNT(CASE WHEN discovery_complete = TRUE THEN 1 END) as discovered_periodicals,
                    COUNT(CASE WHEN download_complete = TRUE THEN 1 END) as downloaded_periodicals,
                    SUM(total_issues) as total_issues,
                    SUM(issues_discovered) as discovered_issues,
                    SUM(issues_downloaded) as downloaded_issues
                FROM periodicals
            """)
            row = cursor.fetchone()
            stats.update({
                'total_periodicals': row[0] or 0,
                'discovered_periodicals': row[1] or 0,
                'downloaded_periodicals': row[2] or 0,
                'total_issues': row[3] or 0,
                'discovered_issues': row[4] or 0,
                'downloaded_issues': row[5] or 0
            })
            
            # Facet stats
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_facets,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_facets,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) as error_facets,
                    SUM(estimated_items) as estimated_items,
                    SUM(actual_items) as actual_items,
                    SUM(items_discovered) as discovered_items,
                    SUM(items_downloaded) as downloaded_items
                FROM search_facets
            """)
            row = cursor.fetchone()
            stats.update({
                'total_facets': row[0] or 0,
                'completed_facets': row[1] or 0,
                'error_facets': row[2] or 0,
                'estimated_items': row[3] or 0,
                'actual_items': row[4] or 0,
                'discovered_items': row[5] or 0,
                'downloaded_items': row[6] or 0
            })
            
            # Queue stats
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_queue_items,
                    COUNT(CASE WHEN status = 'queued' THEN 1 END) as queued_items,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_items,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_items,
                    AVG(progress_percent) as avg_progress
                FROM download_queue
            """)
            row = cursor.fetchone()
            stats.update({
                'total_queue_items': row[0] or 0,
                'queued_items': row[1] or 0,
                'active_items': row[2] or 0,
                'completed_queue_items': row[3] or 0,
                'avg_queue_progress': round(row[4] or 0, 2)
            })
            
            return stats
    
    def get_search_facet(self, facet_id: int) -> Optional[Dict]:
        """Get a specific search facet by ID."""
        with sqlite3.connect(self.db_path) as conn:
            # Ensure migrations are applied for this connection
            self._migrate_database(conn)
            
            cursor = conn.cursor()
            
            # Check what columns actually exist
            cursor.execute("PRAGMA table_info(search_facets)")
            existing_columns = [column[1] for column in cursor.fetchall()]
            
            # Build SELECT query based on available columns
            base_columns = ["id", "facet_type", "facet_value", "facet_query", "estimated_items",
                           "actual_items", "items_discovered", "items_downloaded", 
                           "status", "error_message", "created_at", "updated_at"]
            
            optional_columns = ["current_page", "last_batch_size", "resume_from_page"]
            available_optional = [col for col in optional_columns if col in existing_columns]
            
            all_columns = base_columns + available_optional
            columns_str = ", ".join(all_columns)
            
            cursor.execute(f"""
                SELECT {columns_str}
                FROM search_facets 
                WHERE id = ?
            """, (facet_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Build result dict dynamically based on available columns
            result = {}
            for i, column_name in enumerate(all_columns):
                if column_name == 'facet_query':
                    result['query'] = row[i]  # Map facet_query to query
                else:
                    result[column_name] = row[i]
            
            # Set defaults for optional columns if they weren't in the result
            result.setdefault('current_page', 1)
            result.setdefault('last_batch_size', 100)
            result.setdefault('resume_from_page', 1)
            
            return result
    
    def get_pages_for_facet(self, facet_id: int, downloaded: bool = None) -> List[Dict]:
        """Get pages discovered for a specific facet."""
        # For now, this is a simple implementation
        # In a real system, you'd want to track which facet discovered which pages
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if downloaded is None:
                cursor.execute("""
                    SELECT item_id, lccn, title, date, edition, sequence,
                           page_url, pdf_url, jp2_url, downloaded
                    FROM pages
                    ORDER BY date, lccn, edition, sequence
                """)
            else:
                cursor.execute("""
                    SELECT item_id, lccn, title, date, edition, sequence,
                           page_url, pdf_url, jp2_url, downloaded
                    FROM pages
                    WHERE downloaded = ?
                    ORDER BY date, lccn, edition, sequence
                """, (downloaded,))
            
            rows = cursor.fetchall()
            return [
                {
                    'item_id': row[0],
                    'lccn': row[1],
                    'title': row[2],
                    'date': row[3],
                    'edition': row[4],
                    'sequence': row[5],
                    'page_url': row[6],
                    'pdf_url': row[7],
                    'jp2_url': row[8],
                    'downloaded': bool(row[9])
                }
                for row in rows
            ]
    
    def get_download_queue_stats(self) -> Dict:
        """Get statistics about the download queue."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get counts by status
            cursor.execute("""
                SELECT status, COUNT(*), 
                       SUM(estimated_size_mb), 
                       SUM(estimated_time_hours)
                FROM download_queue 
                GROUP BY status
            """)
            
            stats = {
                'total_items': 0,
                'total_size_mb': 0.0,
                'total_time_hours': 0.0,
                'queued': 0,
                'active': 0,
                'completed': 0,
                'failed': 0
            }
            
            for row in cursor.fetchall():
                status, count, size_mb, time_hours = row
                stats['total_items'] += count
                stats['total_size_mb'] += size_mb or 0
                stats['total_time_hours'] += time_hours or 0
                stats[status] = count
            
            return stats