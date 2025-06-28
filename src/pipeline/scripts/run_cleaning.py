#!/usr/bin/env python3
"""
Data cleaning script entry point.

This script runs the data cleaning pipeline modules in the correct sequence.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline.cleaning.convert_xlsx_to_csvs import main as convert_xlsx


def main():
    """Run the complete data cleaning pipeline."""
    print("\n" + "="*70)
    print("🧹 STARTING DATA CLEANING PIPELINE")
    print("="*70)
    
    try:
        # Step 1: Convert XLSX files to CSV
        print("\n📄 Step 1: Converting XLSX files to CSV...")
        result = convert_xlsx()
        
        if result != 0:
            print("⚠️  XLSX conversion completed with warnings")
        
        # Note: Additional cleaning steps would go here
        # For now, we only have the XLSX conversion step migrated
        
        print("\n" + "="*70)
        print("🎉 DATA CLEANING PIPELINE COMPLETED!")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Data cleaning pipeline failed: {e}")
        print("="*70)
        return 1


if __name__ == "__main__":
    sys.exit(main())