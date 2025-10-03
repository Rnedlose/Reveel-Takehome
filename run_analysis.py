#!/usr/bin/env python3
"""
Standalone script to run analysis queries and generate reports.
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.database import DatabaseManager
from src.analysis import AnalysisEngine
from loguru import logger


def main():
    """Run analysis queries and print formatted report."""
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    logger.add("pipeline.log", rotation="10 MB", level="INFO")
    
    # Initialize database and analysis engine
    db_manager = DatabaseManager()
    db_manager.connect()
    
    analysis_engine = AnalysisEngine(db_manager)
    
    # Run all analyses
    results = analysis_engine.run_all_analyses()
    
    # Print formatted report
    analysis_engine.print_analysis_report(results)
    
    # Clean up
    db_manager.disconnect()


if __name__ == '__main__':
    main()