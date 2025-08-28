"""
Main application for the TTBW system using the refactored modular structure.
"""

import logging
import sys
from database.database_manager import DatabaseManager
from database.player_manager import PlayerManager
from database.history_manager import HistoryManager
from ranking.ranking_processor import RankingProcessor
from reports.report_generator import ReportGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main application entry point."""
    try:
        logger.info("Starting TTBW system...")
        
        # Initialize database manager
        db_manager = DatabaseManager("ttbw_players.db", "config.yaml")
        logger.info("Database manager initialized")
        
        # Initialize managers
        player_manager = PlayerManager(db_manager)
        history_manager = HistoryManager(db_manager)
        ranking_processor = RankingProcessor("ttbw_players.db", "config.yaml")
        report_generator = ReportGenerator(db_manager)
        
        logger.info("All managers initialized")
        
        # Example operations
        logger.info("Loading players from database...")
        ranking_processor.load_players_from_database()
        
        # Get database statistics
        stats = db_manager.get_database_stats()
        logger.info(f"Database statistics: {stats}")
        
        # Get player statistics
        player_stats = ranking_processor.get_player_statistics()
        logger.info(f"Player statistics: {player_stats}")
        
        # Generate some reports
        logger.info("Generating reports...")
        report_results = report_generator.generate_all_reports("reports")
        logger.info(f"Generated reports: {report_results}")
        
        # Clean up duplicate history if any
        duplicates_removed = history_manager.cleanup_duplicate_history()
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate history records")
        
        logger.info("TTBW system completed successfully")
        
    except Exception as e:
        logger.error(f"Error in TTBW system: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
