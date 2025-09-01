"""
Main application for the TTBW system using the refactored modular structure.
"""

import logging
import sys

from setuptools.command.setopt import config_file

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


def main(config_file: str = "config.yaml") -> None:
    """Main application entry point."""
    try:
        logger.info("Starting TTBW system...")
        
        # Initialize database manager
        db_manager = DatabaseManager("ttbw_players.db", config_file)
        logger.info("Database manager initialized")
        
        # Initialize managers
        player_manager = PlayerManager(db_manager)
        history_manager = HistoryManager(db_manager)
        ranking_processor = RankingProcessor("ttbw_players.db", config_file)
        report_generator = ReportGenerator(db_manager, ranking_processor)
        
        logger.info("All managers initialized")
        
        # Load players from database
        logger.info("Loading players from database...")
        ranking_processor.load_players_from_database()
        
        # Load tournament participants and process results
        logger.info("Loading tournament participants...")
        try:
            # Load tournament participants from XML files and web API
            ranking_processor.load_tournament_participants()
            logger.info("Tournament participants loaded")
            
            # Process tournament results
            logger.info("Processing tournament results...")
            ranking_processor.process_tournament_results()
            logger.info("Tournament results processed")
            
        except Exception as e:
            logger.warning(f"Could not load tournament data: {e}")
            logger.warning("Reports will be generated without tournament results")
        
        # Get database statistics
        stats = db_manager.get_database_stats()
        logger.info(f"Database statistics: {stats}")
        
        # Get player statistics
        try:
            player_stats = ranking_processor.get_player_statistics()
            logger.info(f"Player statistics: {player_stats}")
        except Exception as e:
            logger.warning(f"Could not get player statistics: {e}")
        
        # Generate some reports
        logger.info("Generating reports...")
        report_results = report_generator.generate_all_reports("reports")
        logger.info(f"Generated reports: {report_results}")
        
        # Show unmatched players if any
        unmatched_players = ranking_processor.get_unmatched_players()
        if unmatched_players:
            logger.info(f"Found {len(unmatched_players)} unmatched players during tournament processing")
        
        # Clean up duplicate history if any
        duplicates_removed = history_manager.cleanup_duplicate_history()
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate history records")
        
        logger.info("TTBW system completed successfully")
        
    except Exception as e:
        logger.error(f"Error in TTBW system: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    config_file = "config_rem25.yaml"
    main(config_file)
