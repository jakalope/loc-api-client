"""
Discovery Management Module

Coordinates the discovery and tracking of available periodicals and facets
for systematic downloading from the Library of Congress API.
"""

import logging
from typing import Dict, List, Optional, Tuple, Generator
from datetime import datetime
from .api_client import LocApiClient
from .processor import NewsDataProcessor
from .storage import NewsStorage


class DiscoveryManager:
    """Manages discovery of available content and tracks download progress."""
    
    def __init__(self, api_client: LocApiClient, processor: NewsDataProcessor, 
                 storage: NewsStorage):
        self.api_client = api_client
        self.processor = processor
        self.storage = storage
        self.logger = logging.getLogger(__name__)
    
    def discover_all_periodicals(self, max_newspapers: int = None) -> int:
        """
        Discover all available periodicals and store them for tracking.
        Returns the number of new periodicals discovered.
        """
        self.logger.info("Starting periodical discovery...")
        
        discovered_count = 0
        
        if max_newspapers:
            # Limited discovery
            for newspaper in self.api_client.get_newspapers_with_details(max_newspapers):
                periodical_data = self._convert_newspaper_to_periodical(newspaper)
                stored = self.storage.store_periodicals([periodical_data])
                discovered_count += stored
        else:
            # Full discovery - get all newspapers
            batch_size = 100
            for newspapers_batch in self._get_newspapers_in_batches(batch_size):
                periodicals = [self._convert_newspaper_to_periodical(n) for n in newspapers_batch]
                stored = self.storage.store_periodicals(periodicals)
                discovered_count += stored
                
                if discovered_count % 500 == 0:
                    self.logger.info(f"Discovered {discovered_count} periodicals so far...")
        
        self.logger.info(f"Periodical discovery completed. Found {discovered_count} periodicals.")
        return discovered_count
    
    def _convert_newspaper_to_periodical(self, newspaper: Dict) -> Dict:
        """Convert API newspaper response to periodical tracking format."""
        return {
            'lccn': newspaper.get('lccn'),
            'title': newspaper.get('title'),
            'state': newspaper.get('state'),
            'city': self._extract_city(newspaper.get('place_of_publication', [])),
            'start_year': self._parse_year(newspaper.get('start_year')),
            'end_year': self._parse_year(newspaper.get('end_year')),
            'frequency': newspaper.get('frequency'),
            'language': self._extract_primary_language(newspaper.get('language', [])),
            'subject': self._extract_primary_subject(newspaper.get('subject', [])),
            'url': newspaper.get('url')
        }
    
    def _get_newspapers_in_batches(self, batch_size: int) -> Generator[List[Dict], None, None]:
        """Get all newspapers in batches to avoid memory issues."""
        batch = []
        for newspaper in self.api_client.get_all_newspapers():
            batch.append(newspaper)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:  # Yield final batch
            yield batch
    
    def discover_periodical_issues(self, lccn: str) -> int:
        """
        Discover all issues for a specific periodical.
        Returns the number of issues discovered.
        """
        self.logger.info(f"Discovering issues for periodical {lccn}...")
        
        try:
            # Get detailed newspaper info including issues
            newspaper_detail = self.api_client.get_newspaper_issues(lccn)
            issues = newspaper_detail.get('issues', [])
            
            issues_count = 0
            for issue in issues:
                issue_date = self._parse_issue_date(issue.get('date_issued'))
                if issue_date:
                    issue_id = self.storage.store_periodical_issue(
                        lccn=lccn,
                        issue_date=issue_date,
                        edition_count=1,  # Will be updated during page discovery
                        pages_count=0,    # Will be updated during page discovery
                        issue_url=issue.get('url')
                    )
                    issues_count += 1
            
            # Update periodical discovery progress
            self.storage.update_periodical_discovery(
                lccn=lccn,
                total_issues=issues_count,
                issues_discovered=issues_count,
                complete=True
            )
            
            self.logger.info(f"Discovered {issues_count} issues for {lccn}")
            return issues_count
            
        except Exception as e:
            self.logger.error(f"Failed to discover issues for {lccn}: {e}")
            return 0
    
    def create_date_range_facets(self, start_year: int, end_year: int, 
                               facet_size_years: int = 1) -> List[int]:
        """
        Create date range facets for systematic downloading.
        Returns list of facet IDs created.
        """
        facet_ids = []
        
        for year in range(start_year, end_year + 1, facet_size_years):
            facet_end_year = min(year + facet_size_years - 1, end_year)
            facet_value = f"{year}/{facet_end_year}" if year != facet_end_year else f"{year}/{year}"
            
            # Estimate items for this date range
            try:
                estimate = self.api_client.estimate_download_size((str(year), str(facet_end_year)))
                estimated_items = estimate.get('total_pages', 0)
            except Exception:
                estimated_items = 0
            
            facet_id = self.storage.create_search_facet(
                facet_type='date_range',
                facet_value=facet_value,
                facet_query='',  # Empty query for broad date search
                estimated_items=estimated_items
            )
            facet_ids.append(facet_id)
            
            self.logger.info(f"Created date facet {facet_value} with ~{estimated_items} estimated items")
        
        return facet_ids
    
    def create_state_facets(self, states: List[str] = None) -> List[int]:
        """
        Create state-based facets for systematic downloading.
        Returns list of facet IDs created.
        """
        if states is None:
            # Get all states from discovered periodicals
            periodicals = self.storage.get_periodicals()
            states = list(set(p['state'] for p in periodicals if p['state']))
        
        facet_ids = []
        for state in states:
            # Get periodicals count for estimation
            state_periodicals = self.storage.get_periodicals(state=state)
            estimated_items = len(state_periodicals) * 1000  # Rough estimate
            
            facet_id = self.storage.create_search_facet(
                facet_type='state',
                facet_value=state,
                facet_query='',
                estimated_items=estimated_items
            )
            facet_ids.append(facet_id)
            
            self.logger.info(f"Created state facet {state} with ~{estimated_items} estimated items")
        
        return facet_ids
    
    def populate_download_queue(self, priority_states: List[str] = None, 
                              priority_date_ranges: List[str] = None) -> int:
        """
        Populate the download queue with discovered facets and periodicals.
        Returns the number of items added to the queue.
        """
        queue_items_added = 0
        
        # Add high-priority date ranges (e.g., significant historical events)
        if priority_date_ranges:
            for date_range in priority_date_ranges:
                facets = self.storage.get_search_facets(facet_type='date_range')
                for facet in facets:
                    if facet['facet_value'] in priority_date_ranges:
                        queue_id = self.storage.add_to_download_queue(
                            queue_type='facet',
                            reference_id=str(facet['id']),
                            priority=1,  # Highest priority
                            estimated_size_mb=facet['estimated_items'] * 2,  # 2MB per item estimate
                            estimated_time_hours=facet['estimated_items'] * 3 / 3600  # 3 seconds per item
                        )
                        queue_items_added += 1
        
        # Add high-priority states
        if priority_states:
            for state in priority_states:
                facets = self.storage.get_search_facets(facet_type='state')
                for facet in facets:
                    if facet['facet_value'] in priority_states:
                        queue_id = self.storage.add_to_download_queue(
                            queue_type='facet',
                            reference_id=str(facet['id']),
                            priority=2,  # High priority
                            estimated_size_mb=facet['estimated_items'] * 2,
                            estimated_time_hours=facet['estimated_items'] * 3 / 3600
                        )
                        queue_items_added += 1
        
        # Add remaining completed facets with normal priority
        completed_facets = self.storage.get_search_facets(status='completed')
        for facet in completed_facets:
            # Check if not already in queue
            existing_queue = self.storage.get_download_queue()
            facet_ref = str(facet['id'])
            if not any(q['reference_id'] == facet_ref and q['queue_type'] == 'facet' 
                      for q in existing_queue):
                queue_id = self.storage.add_to_download_queue(
                    queue_type='facet',
                    reference_id=facet_ref,
                    priority=5,  # Normal priority
                    estimated_size_mb=facet['actual_items'] * 2,
                    estimated_time_hours=facet['actual_items'] * 3 / 3600
                )
                queue_items_added += 1
        
        # Add high-value periodicals
        discovered_periodicals = self.storage.get_periodicals(discovery_complete=True)
        for periodical in discovered_periodicals:
            # Prioritize based on historical significance, size, etc.
            priority = self._calculate_periodical_priority(periodical)
            
            queue_id = self.storage.add_to_download_queue(
                queue_type='periodical',
                reference_id=periodical['lccn'],
                priority=priority,
                estimated_size_mb=periodical['total_issues'] * 50,  # 50MB per issue estimate
                estimated_time_hours=periodical['total_issues'] * 30 / 3600  # 30 seconds per issue
            )
            queue_items_added += 1
        
        self.logger.info(f"Added {queue_items_added} items to download queue")
        return queue_items_added
    
    def get_discovery_summary(self) -> Dict:
        """Get a comprehensive summary of discovery progress."""
        stats = self.storage.get_discovery_stats()
        
        # Add additional context
        undiscovered_periodicals = self.storage.get_periodicals(discovery_complete=False)
        ready_facets = self.storage.get_search_facets(status='completed')
        next_queue_items = self.storage.get_download_queue(status='queued', limit=5)
        
        summary = {
            'discovery_stats': stats,
            'undiscovered_periodicals_count': len(undiscovered_periodicals),
            'ready_facets_count': len(ready_facets),
            'next_downloads': [
                {
                    'type': item['queue_type'],
                    'reference': item['reference_id'],
                    'priority': item['priority'],
                    'estimated_size_mb': item['estimated_size_mb'],
                    'estimated_time_hours': item['estimated_time_hours']
                }
                for item in next_queue_items
            ]
        }
        
        return summary
    
    # Helper methods
    
    def _extract_city(self, place_list: List[str]) -> Optional[str]:
        """Extract city name from place of publication list."""
        if not place_list:
            return None
        
        # Take first place and extract city (before first comma)
        place = place_list[0] if isinstance(place_list, list) else str(place_list)
        return place.split(',')[0].strip() if ',' in place else place.strip()
    
    def _extract_primary_language(self, language_list: List[str]) -> Optional[str]:
        """Extract primary language from language list."""
        if not language_list:
            return None
        return language_list[0] if isinstance(language_list, list) else str(language_list)
    
    def _extract_primary_subject(self, subject_list: List[str]) -> Optional[str]:
        """Extract primary subject from subject list."""
        if not subject_list:
            return None
        return subject_list[0] if isinstance(subject_list, list) else str(subject_list)
    
    def _parse_year(self, year_str: Optional[str]) -> Optional[int]:
        """Parse year string to integer."""
        if not year_str:
            return None
        try:
            # Extract 4-digit year
            import re
            match = re.search(r'\b(\d{4})\b', str(year_str))
            return int(match.group(1)) if match else None
        except (ValueError, AttributeError):
            return None
    
    def _parse_issue_date(self, date_str: str) -> Optional[str]:
        """Parse issue date to YYYY-MM-DD format."""
        if not date_str:
            return None
        
        try:
            # Handle various date formats from LOC API
            if len(date_str) == 10 and date_str.count('-') == 2:
                return date_str  # Already in YYYY-MM-DD format
            elif len(date_str) == 8 and date_str.isdigit():
                # YYYYMMDD format
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                # Try to parse with datetime
                from datetime import datetime
                parsed = datetime.strptime(date_str, '%Y-%m-%d')
                return parsed.strftime('%Y-%m-%d')
        except Exception:
            self.logger.warning(f"Could not parse date: {date_str}")
            return None
    
    def _calculate_periodical_priority(self, periodical: Dict) -> int:
        """Calculate download priority for a periodical (1=highest, 10=lowest)."""
        priority = 5  # Default priority
        
        # Prioritize by date range (more recent = higher priority)
        if periodical.get('end_year'):
            if periodical['end_year'] >= 1950:
                priority -= 1  # Recent news
            elif periodical['end_year'] >= 1900:
                priority -= 0  # 20th century
            else:
                priority += 1  # Historical
        
        # Prioritize by frequency (daily papers = higher priority)
        if periodical.get('frequency'):
            freq = periodical['frequency'].lower()
            if 'daily' in freq:
                priority -= 1
            elif 'weekly' in freq:
                priority += 0
            else:
                priority += 1
        
        # Prioritize by size (more issues = higher value)
        if periodical.get('total_issues', 0) > 1000:
            priority -= 1
        
        return max(1, min(10, priority))  # Clamp between 1-10