#!/usr/bin/env python3
"""
Data acquisition script entry point.

This script runs the data acquisition pipeline modules in the correct sequence.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline.acquisition.download_counties import main as download_counties
from src.pipeline.acquisition.download_raw_data import main as download_raw_data


def main():
    """Run the complete data acquisition pipeline."""
    print("\n" + "="*70)
    print("🌍 STARTING DATA ACQUISITION PIPELINE")
    print("="*70)
    
    try:
        # Step 1: Download counties data
        print("\n🏛️  Step 1: Downloading county metadata...")
        download_counties()
        
        # Step 2: Download raw data from all sources
        print("\n🌐 Step 2: Downloading raw data from all sources...")
        download_raw_data()
        
        print("\n" + "="*70)
        print("🎉 DATA ACQUISITION PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Data acquisition pipeline failed: {e}")
        print("="*70)
        return 1


if __name__ == "__main__":
    sys.exit(main())