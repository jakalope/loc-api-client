#!/usr/bin/env python3
"""
Debug Batch-to-LCCN Mapping

Shows the mapping between batches and LCCNs, and checks what's in downloads.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.storage import NewsStorage
from newsagger.rate_limited_client import LocApiClient
from newsagger.batch_utils import BatchMapper

def main():
    """Debug the mapping."""
    db_path = '/home/jake/loc/data/newsagger.db'
    downloads_dir = '/home/jake/loc/downloads'
    
    storage = NewsStorage(db_path)
    api_client = LocApiClient()
    mapper = BatchMapper(storage, api_client)
    
    print("🔍 Debugging batch-to-LCCN mapping...")
    
    # Check what's in downloads directory
    downloads_path = Path(downloads_dir)
    if downloads_path.exists():
        download_lccns = [item.name for item in downloads_path.iterdir() if item.is_dir()]
        print(f"\n📁 LCCNs in downloads directory: {len(download_lccns)}")
        for lccn in sorted(download_lccns):
            lccn_path = downloads_path / lccn
            size_mb = sum(f.stat().st_size for f in lccn_path.rglob('*') if f.is_file()) / (1024*1024)
            print(f"  {lccn}: {size_mb:.0f} MB")
    
    # Check batch metadata for known batches
    batches = ['nbu_bluejay_ver02', 'dlc_ferguson_ver01', 'scu_asparagus_ver01', 'dlc_evans_ver01', 'vi_alpina_ver01']
    
    print(f"\n🎯 Checking {len(batches)} batches for LCCN mapping...")
    
    for batch_name in batches:
        print(f"\n📦 {batch_name}:")
        metadata = mapper.get_batch_metadata(batch_name)
        
        if metadata and 'error' not in metadata:
            lccns = metadata.get('lccns', set())
            print(f"  LCCNs in batch: {len(lccns)}")
            
            matching_downloads = []
            for lccn in lccns:
                if lccn in download_lccns:
                    lccn_path = downloads_path / lccn
                    size_mb = sum(f.stat().st_size for f in lccn_path.rglob('*') if f.is_file()) / (1024*1024)
                    matching_downloads.append((lccn, size_mb))
            
            if matching_downloads:
                print(f"  ✅ Found {len(matching_downloads)} matching downloads:")
                for lccn, size_mb in matching_downloads:
                    print(f"    {lccn}: {size_mb:.0f} MB")
            else:
                print(f"  ❌ No matching downloads found")
                print(f"  Batch LCCNs: {', '.join(list(lccns)[:5])}")
        else:
            error = metadata.get('error', 'Unknown error') if metadata else 'Failed to get metadata'
            print(f"  ❌ Error: {error}")

if __name__ == '__main__':
    main()