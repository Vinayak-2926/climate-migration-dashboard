#!/usr/bin/env python3
"""
Database update script entry point.

This script uploads processed data to the PostgreSQL database.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline.database.update_database import main as update_database


def main():
    """Run the database update process."""
    print("\n" + "="*70)
    print("💾 STARTING DATABASE UPDATE")
    print("="*70)
    
    try:
        # Upload all processed data to PostgreSQL
        update_database()
        
        print("\n" + "="*70)
        print("🎉 DATABASE UPDATE COMPLETED!")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Database update failed: {e}")
        print("="*70)
        return 1


if __name__ == "__main__":
    sys.exit(main())