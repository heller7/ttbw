"""
Player management for the TTBW database system.
"""

import sqlite3
import pandas as pd
import logging
from typing import List, Optional, Tuple, Dict
from models.player import PlayerRecord
from utils.name_utils import NameUtils

logger = logging.getLogger(__name__)


class PlayerManager:
    """Manages player-related database operations."""
    
    def __init__(self, database_manager):
        self.db_manager = database_manager
        self.config = database_manager.config
    
    def load_players_from_csv(self, csv_file: str) -> int:
        """
        Load players from CSV file and update database.
        Returns the number of players processed.
        """
        try:
            df = pd.read_csv(csv_file, delimiter=';', encoding='latin1')
            logger.info(f"Loaded CSV with {len(df)} rows")

            players_processed = 0
            for index, row in df.iterrows():
                if self._process_csv_row(row):
                    players_processed += 1

            logger.info(f"Processed {players_processed} players from CSV")
            return players_processed

        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            return 0
    
    def _process_csv_row(self, row: pd.Series) -> bool:
        """Process a single CSV row and update database."""
        try:
            # Extract values from the row
            verband = row.get('Verband', '')
            district = row.get('Region', '')
            club = row.get('VereinName', '')
            club_number = row.get('VereinNr', '')
            title = row.get('Anrede', '')
            last_name = row.get('Nachname', '')
            first_name = row.get('Vorname', '')
            birth_date = row.get('Geburtsdatum', '')
            interne_lizenznr = row.get('InterneNr', '')
            
            # Skip if essential fields are missing
            if pd.isna(last_name) or pd.isna(first_name) or pd.isna(interne_lizenznr) or pd.isna(birth_date):
                return False
            
            # Skip if not TTBW
            if verband != 'TTBW':
                return False
            
            # Extract birth year from birth date (assuming format DD.MM.YYYY)
            try:
                if isinstance(birth_date, str) and '.' in birth_date:
                    birth_year = int(birth_date.split('.')[-1])
                else:
                    birth_year = int(birth_date)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse birth date '{birth_date}' for player {first_name} {last_name}")
                return False
            
            # Determine age class
            age_class = self._calculate_age_class(birth_year)
            
            # Determine gender
            gender = "Jungen" if title == "Herr" else "Mädchen"
            
            # Create player record
            player_record = PlayerRecord(
                interne_lizenznr=str(interne_lizenznr),
                first_name=str(first_name),
                last_name=str(last_name),
                club=str(club),
                gender=gender,
                district=str(district),
                birth_year=birth_year,
                age_class=age_class,
                region=self._get_region_from_district(district),
                club_number=str(club_number) if not pd.isna(club_number) else None,
                verband=verband
            )
            
            # Update database
            self._update_player_in_database(player_record)
            return True
            
        except Exception as e:
            logger.error(f"Error processing row {row.get('InterneNr', 'unknown')}: {e}")
            return False
    
    def _calculate_age_class(self, birth_year: int) -> int:
        """Calculate age class based on birth year from config."""
        age_class_mapping = self.config.get('age_classes', {})
        default_age_class = age_class_mapping.get(self.config.get('default_birth_year', 2014), 11)
        return age_class_mapping.get(birth_year, default_age_class)
    
    def _get_region_from_district(self, district: str) -> int:
        """Get region number from district name from config."""
        if district is None:
            return 1  # Default fallback for None
        
        districts_config = self.config.get('districts', {})

        # Try to find the district in the config
        for district_name, district_info in districts_config.items():
            if district_name.lower() == district.lower():
                return district_info.get('region', 1)

        # If no exact match, try partial matching
        for district_name, district_info in districts_config.items():
            if (district_name.lower() in district.lower() or
                    district.lower() in district_name.lower()):
                return district_info.get('region', 1)

        # Default to region 1 if no match found
        return 1
    
    def _is_player_age_eligible(self, birth_year: int) -> bool:
        """Check if player's birth year is within the eligible age range for tournament processing."""
        age_classes = self.config.get('age_classes', {})
        if not age_classes:
            return True  # If no age classes defined, accept all players
        
        # Find the oldest birth year that has an age class defined
        oldest_eligible_birth_year = min(age_classes.keys())
        
        # Player is eligible if their birth year is >= oldest eligible birth year
        # (i.e., not older than the oldest defined age class)
        return birth_year >= oldest_eligible_birth_year
    
    def _update_player_in_database(self, player_record: PlayerRecord) -> None:
        """Update player record in database, tracking changes."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()

            # Check if player exists
            cursor.execute("""
                SELECT * FROM current_players WHERE interne_lizenznr = ?
            """, (player_record.interne_lizenznr,))

            existing_player = cursor.fetchone()

            if existing_player:
                # Player exists, check for changes
                if self._has_changes(existing_player, player_record):
                    # Record the change in history
                    self._record_change(cursor, existing_player, player_record, 'UPDATE')

                    # Update current record
                    cursor.execute("""
                        UPDATE current_players SET
                            first_name = ?, last_name = ?, club = ?, gender = ?,
                            district = ?, birth_year = ?, age_class = ?, region = ?,
                            qttr = ?, club_number = ?, verband = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE interne_lizenznr = ?
                    """, (
                        player_record.first_name, player_record.last_name, player_record.club,
                        player_record.gender, player_record.district, player_record.birth_year,
                        player_record.age_class, player_record.region, player_record.qttr,
                        player_record.club_number, player_record.verband,
                        player_record.interne_lizenznr
                    ))
                    logger.info(f"Updated player {player_record.first_name} {player_record.last_name}")
                else:
                    logger.debug(f"No changes for player {player_record.first_name} {player_record.last_name}")
            else:
                # New player
                cursor.execute("""
                    INSERT INTO current_players (
                        interne_lizenznr, first_name, last_name, club, gender, district,
                        birth_year, age_class, region, qttr, club_number, verband
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    player_record.interne_lizenznr, player_record.first_name, player_record.last_name,
                    player_record.club, player_record.gender, player_record.district,
                    player_record.birth_year, player_record.age_class, player_record.region,
                    player_record.qttr, player_record.club_number, player_record.verband
                ))

                # Record the insertion
                self._record_change(cursor, None, player_record, 'INSERT')
                logger.info(f"Added new player {player_record.first_name} {player_record.last_name}")

            conn.commit()
    
    def _has_changes(self, existing_player: Tuple, new_record: PlayerRecord) -> bool:
        """Check if there are changes between existing and new player record."""
        # existing_player is a tuple from database query
        # Compare relevant fields (excluding timestamps and auto-increment fields)
        return (
                existing_player[1] != new_record.first_name or  # first_name
                existing_player[2] != new_record.last_name or  # last_name
                existing_player[3] != new_record.club or  # club
                existing_player[4] != new_record.gender or  # gender
                existing_player[5] != new_record.district or  # district
                existing_player[6] != new_record.birth_year or  # birth_year
                existing_player[7] != new_record.age_class or  # age_class
                existing_player[8] != new_record.region or  # region
                existing_player[9] != new_record.qttr or  # qttr
                existing_player[10] != new_record.club_number  # club_number
        )
    
    def _record_change(self, cursor: sqlite3.Cursor, old_record: Optional[Tuple], new_record: PlayerRecord, change_type: str) -> None:
        """Record a change in the player_history table."""
        try:
            # Check if this exact change is already recorded
            if change_type == 'UPDATE' and old_record:
                cursor.execute("""
                    SELECT COUNT(*) FROM player_history 
                    WHERE interne_lizenznr = ? AND change_type = 'UPDATE' 
                    AND previous_club = ? AND club = ?
                    AND changed_at > datetime('now', '-1 minute')
                """, (new_record.interne_lizenznr, old_record[3], new_record.club))
                
                if cursor.fetchone()[0] > 0:
                    logger.debug(f"Skipping duplicate change record for {new_record.first_name} {new_record.last_name}")
                    return

            # Record the change
            cursor.execute("""
                INSERT INTO player_history (
                    interne_lizenznr, first_name, last_name, club, gender, district,
                    birth_year, age_class, region, qttr, club_number, verband,
                    change_type, previous_club, previous_district
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                new_record.interne_lizenznr, new_record.first_name, new_record.last_name,
                new_record.club, new_record.gender, new_record.district,
                new_record.birth_year, new_record.age_class, new_record.region,
                new_record.qttr, new_record.club_number, new_record.verband,
                change_type,
                old_record[3] if old_record else None,  # previous_club
                old_record[5] if old_record else None   # previous_district
            ))
            
        except Exception as e:
            logger.error(f"Error recording change: {e}")
    
    def find_player_by_name_and_club(self, first_name: str, last_name: str,
                                     club: str, club_number: Optional[str] = None) -> Optional[str]:
        """
        Find a player by name and club information.
        Returns the interne_lizenznr if found, None otherwise.
        Only returns players who are age-eligible.
        """
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()

            # Try to find by exact name and club match (with age eligibility check)
            cursor.execute("""
                SELECT interne_lizenznr, birth_year FROM current_players 
                WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                AND LOWER(TRIM(club)) = LOWER(TRIM(?))
            """, (first_name, last_name, club))

            result = cursor.fetchone()
            if result:
                player_id, birth_year = result
                # Check age eligibility before returning
                if self._is_player_age_eligible(birth_year):
                    return player_id
                else:
                    logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # If club number is provided, try matching by name and club number
            if club_number:
                # First try matching by name and club number
                cursor.execute("""
                    SELECT interne_lizenznr, birth_year FROM current_players 
                    WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                    AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                    AND club_number = ?
                """, (first_name, last_name, club_number))

                result = cursor.fetchone()
                if result:
                    player_id, birth_year = result
                    # Check age eligibility before returning
                    if self._is_player_age_eligible(birth_year):
                        return player_id
                    else:
                        logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")
                
                # If club number looks like a license ID, try matching by license ID
                if len(club_number) >= 8:  # License IDs are typically 8+ characters
                    cursor.execute("""
                        SELECT interne_lizenznr, first_name, last_name, club, birth_year FROM current_players 
                        WHERE interne_lizenznr = ?
                    """, (club_number,))

                    result = cursor.fetchone()
                    if result:
                        player_id, db_first_name, db_last_name, club_name, birth_year = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"LICENSE ID MATCH: Found player by license ID {club_number}: {db_first_name} {db_last_name} (tournament: {first_name} {last_name})")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {db_last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {db_first_name} {db_last_name} (birth year {birth_year}) is too old for age classes")

            # Try fuzzy matching by name only (in case club has changed)
            cursor.execute("""
                SELECT interne_lizenznr, club, birth_year FROM current_players 
                WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
            """, (first_name, last_name))

            results = cursor.fetchall()
            if len(results) == 1:
                player_id, club_name, birth_year = results[0]
                # Check age eligibility before returning
                if self._is_player_age_eligible(birth_year):
                    return player_id
                else:
                    logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")
            elif len(results) > 1:
                # Multiple matches, find the first age-eligible one
                for player_id, club_name, birth_year in results:
                    if self._is_player_age_eligible(birth_year):
                        logger.debug(f"Multiple players found for {first_name} {last_name}, using age-eligible one: {player_id}")
                        return player_id
                
                # If no age-eligible players found, log warning
                logger.warning(f"Multiple players found for {first_name} {last_name}, but none are age-eligible: {results}")

            # Try fuzzy name matching with common variants
            first_name_variants = NameUtils.get_name_variants(first_name)
            last_name_variants = NameUtils.get_name_variants(last_name)
            
            # Try matching with first name variants
            for variant in first_name_variants:
                if variant != first_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, club, birth_year, first_name FROM current_players 
                        WHERE LOWER(TRIM(first_name)) = ? 
                        AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                        AND LOWER(TRIM(club)) = LOWER(TRIM(?))
                    """, (variant, last_name, club))

                    result = cursor.fetchone()
                    if result:
                        player_id, club_name, birth_year, db_first_name = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"FUZZY MATCH: Tournament '{first_name}' matched to DB '{db_first_name}' for {first_name} {last_name} from {club}")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # Try matching with last name variants
            for variant in last_name_variants:
                if variant != last_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, club, birth_year, last_name FROM current_players 
                        WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                        AND LOWER(TRIM(last_name)) = ?
                        AND LOWER(TRIM(club)) = LOWER(TRIM(?))
                    """, (first_name, variant, club))

                    result = cursor.fetchone()
                    if result:
                        player_id, club_name, birth_year, db_last_name = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"FUZZY MATCH: Tournament '{last_name}' matched to DB '{db_last_name}' for {first_name} {last_name} from {club}")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{first_name} {db_last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=first_name,
                                db_last=db_last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # Try fuzzy matching by name variants only (in case club has changed)
            for variant in first_name_variants:
                if variant != first_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, club, birth_year, first_name FROM current_players 
                        WHERE LOWER(TRIM(first_name)) = ? 
                        AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                    """, (variant, last_name))

                    results = cursor.fetchall()
                    if len(results) == 1:
                        player_id, club_name, birth_year, db_first_name = results[0]
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"FUZZY MATCH: Tournament '{first_name}' matched to DB '{db_first_name}' for {first_name} {last_name} (club: tournament={club}, DB={club_name})")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # Check if the club exists in the database at all
            cursor.execute("""
                SELECT COUNT(*) FROM current_players WHERE LOWER(TRIM(club)) = LOWER(TRIM(?))
            """, (club,))
            
            club_exists = cursor.fetchone()[0] > 0
            
            if not club_exists:
                logger.warning(f"CLUB NOT FOUND: Club '{club}' is not in the database - likely not part of considered regions")
                return None

            return None
    
    def _log_fuzzy_match(self, tournament_name: str, db_name: str, tournament_club: str, db_club: str,
                         tournament_first: str, tournament_last: str, db_first: str, db_last: str) -> None:
        """Log a fuzzy match for reporting purposes."""
        try:
            with sqlite3.connect(self.db_manager.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO fuzzy_matches (
                        tournament_name, tournament_first, tournament_last, tournament_club,
                        db_first, db_last, db_club
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (tournament_name, tournament_first, tournament_last, tournament_club,
                      db_first, db_last, db_club))
                conn.commit()
        except Exception as e:
            logger.error(f"Error logging fuzzy match: {e}")
    
    def club_exists(self, club_name: str) -> bool:
        """Check if a club exists in the database."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM current_players WHERE LOWER(TRIM(club)) = LOWER(TRIM(?))
            """, (club_name,))
            return cursor.fetchone()[0] > 0
    
    def get_all_current_players(self) -> List[PlayerRecord]:
        """Get all current players from the database."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT interne_lizenznr, first_name, last_name, club, gender, district,
                       birth_year, age_class, region, qttr, club_number, verband
                FROM current_players
                ORDER BY last_name, first_name
            """)
            
            players = []
            for row in cursor.fetchall():
                player = PlayerRecord(
                    interne_lizenznr=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    club=row[3],
                    gender=row[4],
                    district=row[5],
                    birth_year=row[6],
                    age_class=row[7],
                    region=row[8],
                    qttr=row[9],
                    club_number=row[10],
                    verband=row[11]
                )
                players.append(player)
            
            return players
    
    def get_player_by_lizenznr(self, interne_lizenznr: str) -> Optional[PlayerRecord]:
        """Get a player by their license number."""
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT interne_lizenznr, first_name, last_name, club, gender, district,
                       birth_year, age_class, region, qttr, club_number, verband
                FROM current_players
                WHERE interne_lizenznr = ?
            """, (interne_lizenznr,))
            
            row = cursor.fetchone()
            if row:
                return PlayerRecord(
                    interne_lizenznr=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    club=row[3],
                    gender=row[4],
                    district=row[5],
                    birth_year=row[6],
                    age_class=row[7],
                    region=row[8],
                    qttr=row[9],
                    club_number=row[10],
                    verband=row[11]
                )
            return None
