#!/usr/bin/env python3
"""
Full pipeline execution script.

This script runs the complete data processing pipeline in the correct sequence,
providing comprehensive logging and error handling.
"""

import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.shared.config import PipelineConfig
from src.pipeline.scripts.run_acquisition import main as run_acquisition
from src.pipeline.scripts.run_cleaning import main as run_cleaning
from src.pipeline.scripts.run_database_update import main as run_database_update


def validate_environment():
    """Validate the pipeline environment and configuration."""
    try:
        config = PipelineConfig()
        env_info = config.get_env_info()
        
        print("🔧 Environment Configuration:")
        for key, value in env_info.items():
            print(f"   {key}: {value}")
        
        return True
    except Exception as e:
        print(f"❌ Environment validation failed: {e}")
        return False


def run_pipeline_step(step_name: str, step_function):
    """Run a pipeline step with timing and error handling."""
    print(f"\n{'='*70}")
    print(f"🚀 STARTING: {step_name}")
    print('='*70)
    
    start_time = time.time()
    
    try:
        result = step_function()
        end_time = time.time()
        duration = end_time - start_time
        
        if result == 0:
            print(f"\n✅ COMPLETED: {step_name} ({duration:.2f}s)")
            return True
        else:
            print(f"\n⚠️  WARNING: {step_name} completed with warnings ({duration:.2f}s)")
            return True  # Continue pipeline even with warnings
            
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"\n❌ FAILED: {step_name} ({duration:.2f}s)")
        print(f"Error: {e}")
        return False


def main():
    """Run the complete data processing pipeline."""
    print("\n" + "="*70)
    print("🌍 CLIMATE MIGRATION DASHBOARD - FULL DATA PIPELINE")
    print("="*70)
    
    # Validate environment
    if not validate_environment():
        print("\n❌ Pipeline aborted due to environment validation failure")
        return 1
    
    total_start_time = time.time()
    
    # Pipeline steps
    steps = [
        ("Data Acquisition", run_acquisition),
        ("Data Cleaning", run_cleaning),
        # Note: Analysis steps would be added here as they're migrated
        ("Database Update", run_database_update),
    ]
    
    completed_steps = 0
    
    # Execute pipeline steps
    for step_name, step_function in steps:
        if not run_pipeline_step(step_name, step_function):
            print(f"\n💥 PIPELINE FAILED at step: {step_name}")
            print("="*70)
            return 1
        completed_steps += 1
    
    # Pipeline completed successfully
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    print("\n" + "="*70)
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"📊 Steps completed: {completed_steps}/{len(steps)}")
    print(f"⏱️  Total time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
    print("="*70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())