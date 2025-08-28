"""
History management for the TTBW database system.
"""

import sqlite3
import logging
from typing import List, Dict, Any
from models.player import PlayerRecord

logger = logging.getLogger(__name__)


class HistoryManager:
    """Manages player history operations."""
    
    def __init__(self, database_manager):
        self.db_manager = database_manager
    
    def cleanup_duplicate_history(self) -> int:
        """
        Clean up duplicate history records.
        Returns the number of duplicates removed.
        """
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            # Find and remove duplicate history records
            cursor.execute("""
                DELETE FROM player_history 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM player_history 
                    GROUP BY interne_lizenznr, first_name, last_name, club, gender, district,
                             birth_year, age_class, region, COALESCE(qttr, ''), COALESCE(club_number, ''),
                             verband, change_type, COALESCE(previous_club, ''), COALESCE(previous_district, '')
                )
            """)
            
            duplicates_removed = cursor.rowcount
            conn.commit()
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate history records")
            else:
                logger.info("No duplicate history records found")
            
            return duplicates_removed
    
    def get_player_history(self, interne_lizenznr: str) -> List[Dict[str, Any]]:
        """Get complete history for a specific player."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT change_type, first_name, last_name, club, gender, district,
                       birth_year, age_class, region, qttr, club_number, verband,
                       previous_club, previous_district, changed_at
                FROM player_history
                WHERE interne_lizenznr = ?
                ORDER BY changed_at DESC
            """, (interne_lizenznr,))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'change_type': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'club': row[3],
                    'gender': row[4],
                    'district': row[5],
                    'birth_year': row[6],
                    'age_class': row[7],
                    'region': row[8],
                    'qttr': row[9],
                    'club_number': row[10],
                    'verband': row[11],
                    'previous_club': row[12],
                    'previous_district': row[13],
                    'changed_at': row[14]
                })
            
            return history
    
    def get_recent_changes(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent changes across all players."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT interne_lizenznr, change_type, first_name, last_name, club,
                       previous_club, changed_at
                FROM player_history
                ORDER BY changed_at DESC
                LIMIT ?
            """, (limit,))
            
            changes = []
            for row in cursor.fetchall():
                changes.append({
                    'interne_lizenznr': row[0],
                    'change_type': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'club': row[4],
                    'previous_club': row[5],
                    'changed_at': row[6]
                })
            
            return changes
    
    def get_changes_by_type(self, change_type: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get changes of a specific type (INSERT, UPDATE)."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT interne_lizenznr, first_name, last_name, club, district,
                       previous_club, previous_district, changed_at
                FROM player_history
                WHERE change_type = ?
                ORDER BY changed_at DESC
                LIMIT ?
            """, (change_type, limit))
            
            changes = []
            for row in cursor.fetchall():
                changes.append({
                    'interne_lizenznr': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'club': row[3],
                    'district': row[4],
                    'previous_club': row[5],
                    'previous_district': row[6],
                    'changed_at': row[7]
                })
            
            return changes
    
    def get_club_changes(self, club_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all changes related to a specific club."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT interne_lizenznr, change_type, first_name, last_name,
                       club, previous_club, changed_at
                FROM player_history
                WHERE club = ? OR previous_club = ?
                ORDER BY changed_at DESC
                LIMIT ?
            """, (club_name, club_name, limit))
            
            changes = []
            for row in cursor.fetchall():
                changes.append({
                    'interne_lizenznr': row[0],
                    'change_type': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'club': row[4],
                    'previous_club': row[5],
                    'changed_at': row[6]
                })
            
            return changes
    
    def get_district_changes(self, district_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all changes related to a specific district."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT interne_lizenznr, change_type, first_name, last_name,
                       district, previous_district, changed_at
                FROM player_history
                WHERE district = ? OR previous_district = ?
                ORDER BY changed_at DESC
                LIMIT ?
            """, (district_name, district_name, limit))
            
            changes = []
            for row in cursor.fetchall():
                changes.append({
                    'interne_lizenznr': row[0],
                    'change_type': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'district': row[4],
                    'previous_district': row[5],
                    'changed_at': row[6]
                })
            
            return changes
    
    def get_fuzzy_matches(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent fuzzy matches for reporting."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT tournament_name, tournament_first, tournament_last, tournament_club,
                       db_first, db_last, db_club, match_timestamp
                FROM fuzzy_matches
                ORDER BY match_timestamp DESC
                LIMIT ?
            """, (limit,))
            
            matches = []
            for row in cursor.fetchall():
                matches.append({
                    'tournament_name': row[0],
                    'tournament_first': row[1],
                    'tournament_last': row[2],
                    'tournament_club': row[3],
                    'db_first': row[4],
                    'db_last': row[5],
                    'db_club': row[6],
                    'match_timestamp': row[7]
                })
            
            return matches
    
    def get_history_statistics(self) -> Dict[str, Any]:
        """Get statistics about player history."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            # Total history records
            cursor.execute("SELECT COUNT(*) FROM player_history")
            total_records = cursor.fetchone()[0]
            
            # Records by change type
            cursor.execute("""
                SELECT change_type, COUNT(*) 
                FROM player_history 
                GROUP BY change_type
            """)
            changes_by_type = dict(cursor.fetchall())
            
            # Recent activity (last 30 days)
            cursor.execute("""
                SELECT COUNT(*) FROM player_history 
                WHERE changed_at > datetime('now', '-30 days')
            """)
            recent_activity = cursor.fetchone()[0]
            
            # Most active clubs
            cursor.execute("""
                SELECT club, COUNT(*) as change_count
                FROM player_history
                GROUP BY club
                ORDER BY change_count DESC
                LIMIT 10
            """)
            most_active_clubs = [{'club': row[0], 'changes': row[1]} for row in cursor.fetchall()]
            
            # Most active districts
            cursor.execute("""
                SELECT district, COUNT(*) as change_count
                FROM player_history
                GROUP BY district
                ORDER BY change_count DESC
                LIMIT 10
            """)
            most_active_districts = [{'district': row[0], 'changes': row[1]} for row in cursor.fetchall()]
            
            return {
                'total_records': total_records,
                'changes_by_type': changes_by_type,
                'recent_activity_30_days': recent_activity,
                'most_active_clubs': most_active_clubs,
                'most_active_districts': most_active_districts
            }
    
    def export_history_to_csv(self, output_file: str, start_date: str = None, end_date: str = None) -> int:
        """
        Export player history to CSV file.
        Returns the number of records exported.
        """
        import pandas as pd
        
        with sqlite3.connect(self.db_manager.db_path) as conn:
            # Build query with optional date filtering
            query = """
                SELECT interne_lizenznr, first_name, last_name, club, gender, district,
                       birth_year, age_class, region, qttr, club_number, verband,
                       change_type, previous_club, previous_district, changed_at
                FROM player_history
            """
            
            params = []
            if start_date:
                query += " WHERE changed_at >= ?"
                params.append(start_date)
                if end_date:
                    query += " AND changed_at <= ?"
                    params.append(end_date)
            elif end_date:
                query += " WHERE changed_at <= ?"
                params.append(end_date)
            
            query += " ORDER BY changed_at DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if not df.empty:
                df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"Exported {len(df)} history records to {output_file}")
                return len(df)
            else:
                logger.info("No history records found for export")
                return 0
    
    def clear_old_history(self, days_to_keep: int = 365) -> int:
        """
        Clear old history records older than specified days.
        Returns the number of records removed.
        """
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM player_history 
                WHERE changed_at < datetime('now', '-{} days')
            """.format(days_to_keep))
            
            records_removed = cursor.rowcount
            conn.commit()
            
            if records_removed > 0:
                logger.info(f"Removed {records_removed} history records older than {days_to_keep} days")
            else:
                logger.info(f"No history records older than {days_to_keep} days found")
            
            return records_removed
