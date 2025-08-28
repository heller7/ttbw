#!/usr/bin/env python3
"""
TTBW Ranking Computation Script

This script processes table tennis tournament data from various sources and generates
regional ranking reports. It handles players, tournament results, and QTTR ratings.
"""

import os
import re
import csv
import yaml
import pandas as pd
import requests
import sqlite3
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from ttbw_database import TTBWDatabase, PlayerRecord

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TournamentConfig:
    """Configuration for a tournament including ID and point values."""
    tournament_id: int
    points: int


@dataclass
class DistrictConfig:
    """Configuration for a district including region and short name."""
    region: int
    short_name: str


@dataclass
class Player:
    """Player information and tournament results."""
    id: str
    first_name: str
    last_name: str
    club: str
    gender: str
    district: str
    birth_year: int
    age_class: int
    region: int
    qttr: Optional[int] = None
    points: float = 0.0
    tournaments: Dict[str, Dict[str, int]] = None

    def __post_init__(self):
        if self.tournaments is None:
            self.tournaments = {}


class RankingProcessor:
    """Main processor for TTBW ranking data."""

    def __init__(self, config_file: str = "config_jgrl25.yaml"):
        self.config = self._load_config(config_file)
        self.tournaments = self._initialize_tournaments()
        self.districts = self._initialize_districts()
        self.players: Dict[str, Player] = {}
        self.regions: Dict[int, Dict[str, Dict[str, int]]] = {}
        self.qttr_ratings: Dict[str, int] = {}
        self.session = requests.Session()
        self._initialize_regions()

        # Initialize database
        self.db = TTBWDatabase(config_file=config_file)

        # Track unmatched players during tournament processing
        self.unmatched_players: List[Dict[str, Any]] = []

        # Create output directory if it doesn't exist
        os.makedirs(self.config['output']['folder'], exist_ok=True)

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Configuration file '{config_file}' not found..")
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}.")

    def _initialize_regions(self) -> None:
        """Initialisiere die Regionen anhand der DistrictConfigs."""
        for district in self.districts.values():
            if district.region not in self.regions:
                self.regions[district.region] = {}

    def _initialize_tournaments(self) -> Dict[str, TournamentConfig]:
        """Initialize tournament configurations from config."""
        tournaments = {}
        for name, config in self.config['tournaments'].items():
            tournaments[name] = TournamentConfig(
                tournament_id=config['tournament_id'],
                points=config['points']
            )
        return tournaments

    def _initialize_districts(self) -> Dict[str, DistrictConfig]:
        """Initialize district configurations from config."""
        districts = {}
        for name, config in self.config['districts'].items():
            districts[name] = DistrictConfig(
                region=config['region'],
                short_name=config['short_name']
            )
        return districts

    @staticmethod
    def replace_umlauts(text: str) -> str:
        """Replace German umlauts with their ASCII equivalents."""
        replacements = {
            'ö': 'oe', 'ä': 'ae', 'ü': 'ue', 'ß': 'ss',
            'Ö': 'Oe', 'Ä': 'Ae', 'Ü': 'Ue'
        }
        for umlaut, replacement in replacements.items():
            text = text.replace(umlaut, replacement)
        return text

    def load_qttr_ratings(self) -> None:
        """Load QTTR ratings from files starting with 'QTTR_'."""
        qttr_files_found = 0
        qttr_ratings_loaded = 0

        for filename in os.listdir('.'):
            if filename.startswith('QTTR_'):
                qttr_files_found += 1
                ratings_in_file = self._process_qttr_file(filename)
                qttr_ratings_loaded += ratings_in_file

        print(f"Found {qttr_files_found} QTTR files, loaded {qttr_ratings_loaded} ratings")

    def _process_qttr_file(self, filename: str) -> int:
        """Process a single QTTR file. Returns the number of ratings loaded."""
        ratings_loaded = 0
        try:
            with open(filename, encoding='latin1') as f:
                for line in f:
                    match = re.match(r'\d+\s*\t\d+\s*\t(.*?)\t(.*?)\t(\d+)', line)
                    if match:
                        player_name, club, qttr_value = match.groups()
                        key = re.sub(r'\s+', '', player_name + club)
                        self.qttr_ratings[key] = int(qttr_value)
                        ratings_loaded += 1
            print(f"Loaded {ratings_loaded} ratings from {filename}")
        except Exception as e:
            print(f"Error loading QTTR file {filename}: {e}")

        return ratings_loaded

    def load_players(self) -> None:
        """Load player data from CSV file into database."""
        try:
            # Load players from CSV into database
            players_loaded = self.db.load_players_from_csv('Spielberechtigungen_2025_08.csv')
            print(f"Loaded {players_loaded} players from  file into database")

            # Clean up any duplicate history records
            print("Cleaning up duplicate history records...")
            duplicates_removed = self.db.cleanup_duplicate_history()
            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicate history records")
            else:
                print("No duplicate history records found")

            # Load all current players from database into memory for processing
            self._load_players_from_database()

        except FileNotFoundError:
            print("Warning: Spielberechtigungen.csv not found!")
        except Exception as e:
            print(f"Error loading file: {e}")
            import traceback
            traceback.print_exc()

    def _load_players_from_database(self) -> None:
        """Load all current players from database into memory for processing."""
        try:
            db_players = self.db.get_all_current_players()
            print(f"Loading {len(db_players)} players from database into memory")

            for db_player in db_players:
                # Convert database player to Player object
                player = Player(
                    id=db_player.interne_lizenznr,
                    first_name=db_player.first_name,
                    last_name=db_player.last_name,
                    club=db_player.club,
                    gender=db_player.gender,
                    district=db_player.district,
                    birth_year=db_player.birth_year,
                    age_class=db_player.age_class,
                    region=db_player.region,
                    qttr=db_player.qttr
                )
                self.players[db_player.interne_lizenznr] = player

            print(f"Successfully loaded {len(self.players)} players from database")

        except Exception as e:
            print(f"Error loading players from database: {e}")
            import traceback
            traceback.print_exc()

    def _process_player_row(self, row: pd.Series) -> bool:
        """Process a single row from the DataFrame. Returns True if player was processed."""
        try:
            # Extract values from the row
            verband = row.get('Verband', '')
            district = row.get('Region', '')
            club = row.get('VereinName', '')
            title = row.get('Anrede', '')
            last_name = row.get('Nachname', '')
            first_name = row.get('Vorname', '')
            birth_date = row.get('Geburtsdatum', '')
            player_id = row.get('LizenzNr', '')

            # Skip if essential fields are missing
            if pd.isna(last_name) or pd.isna(first_name) or pd.isna(player_id) or pd.isna(birth_date):
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
                print(f"Could not parse birth date '{birth_date}' for player {first_name} {last_name}")
                return False

            # Get age class
            age_class = self.config['age_classes'].get(birth_year,
                                                       self.config['age_classes'][self.config['default_birth_year']])

            # Determine gender
            gender = "Jungen" if title == "Herr" else "Mädchen"

            # Create key for QTTR lookup
            qttr_key = re.sub(r'\s+', '', first_name + last_name + club)

            # Find district configuration
            district_config = None
            district_lower = district.lower().strip()

            # Try exact match first
            for dist_name, dist_config in self.districts.items():
                if dist_name.lower() == district_lower:
                    district_config = dist_config
                    break

            # Try partial match if exact match failed
            if district_config is None:
                for dist_name, dist_config in self.districts.items():
                    if (dist_name.lower() in district_lower or
                            district_lower in dist_name.lower() or
                            any(word in district_lower for word in dist_name.lower().split())):
                        district_config = dist_config
                        print(f"Matched district '{district}' to '{dist_name}'")
                        break

            # If no match found, use the first district as fallback
            if district_config is None:
                print(f"Warning: Could not find district for '{district}', using fallback")
                district_config = list(self.districts.values())[0]

            player = Player(
                id=str(player_id),
                first_name=str(first_name),
                last_name=str(last_name),
                club=str(club),
                gender=gender,
                district=district_config.short_name,
                birth_year=birth_year,
                age_class=age_class,
                region=district_config.region,
                qttr=self.qttr_ratings.get(qttr_key)
            )

            self.players[str(player_id)] = player
            return True

        except Exception as e:
            print(f"Error processing row {row.get('LizenzNr', 'unknown')}: {e}")
            return False

    def load_tournament_participants(self) -> None:
        """Load tournament participants from XML files and web API."""
        for tournament_name in sorted(self.tournaments.keys()):
            self._load_tournament_data(tournament_name)

    def _load_tournament_data(self, tournament_name: str) -> None:
        """Load data for a specific tournament."""
        tournament = self.tournaments[tournament_name]

        # Initialize participants and competitions attributes
        tournament.participants = {}
        tournament.competitions = {}

        # Load participants from XML file if it exists
        xml_filename = self.replace_umlauts(f"{tournament_name}_Turnierteilnehmer.xml")
        if os.path.exists(xml_filename):
            print(f"Loading participants from XML file: {xml_filename}")
            self._load_participants_from_xml(tournament_name, xml_filename)
            print(f"Loaded {len(tournament.participants)} participants from XML")
        else:
            print(f"No XML file found for {tournament_name}, will use data matching")

        # Load competitions from web API
        print(f"Loading competitions for {tournament_name} from API...")
        self._load_competitions_from_api(tournament_name)
        print(f"Found {len(tournament.competitions)} competitions")

    def _load_participants_from_xml(self, tournament_name: str, filename: str) -> None:
        """Load tournament participants from XML file."""
        tournament = self.tournaments[tournament_name]

        try:
            with open(filename, encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    pattern = r'<person licence-nr="(\d+)" lastname="(.*?)" club-name="(.*?)".*?firstname="(.*?)".*?club-nr="(\d+)"'
                    match = re.search(pattern, line)
                    if match:
                        player_id, last_name, club, first_name, club_number = match.groups()
                        name_club_id = self.replace_umlauts(f"{first_name}{last_name}{club_number}")
                        tournament.participants[name_club_id] = player_id
        except Exception as e:
            print(f"Error loading XML file {filename}: {e}")
            tournament.participants = {}

    def _load_competitions_from_api(self, tournament_name: str) -> None:
        """Load competitions for a tournament from the web API."""
        tournament = self.tournaments[tournament_name]
        federation = self.config['api']['federation_arge'] if tournament_name.startswith("BaWü") else \
            self.config['api']['federation_ttbw']
        url = f"{self.config['api']['nuliga_base_url']}{self.config['api']['tournament_base_url']}{tournament.tournament_id}&{federation}"

        response = self.session.get(url)
        content = response.text

        tournament.competitions = {}
        pattern = r'<td>\s*<b>(\S+ \d+) Einzel</b>.*?<td> ja<.*?<a href=".*?competition=(\d+)">Teilnehmer'

        for match in re.finditer(pattern, content, re.DOTALL):
            competition_name, competition_id = match.groups()
            tournament.competitions[int(competition_id)] = competition_name

    def process_tournament_results(self) -> None:
        """Process results for all tournaments."""
        for tournament_name in sorted(self.tournaments.keys()):
            tournament = self.tournaments[tournament_name]
            if hasattr(tournament, 'competitions'):
                for competition_id, competition_name in sorted(tournament.competitions.items()):
                    self._process_competition_results(tournament_name, competition_id, competition_name)

    def _process_competition_results(self, tournament_name: str, competition_id: int, competition_name: str) -> None:
        """Process results for a specific competition."""
        url = f"{self.config['api']['nuliga_base_url']}{self.config['api']['competition_base_url']}{self.config['api']['federation_arge']}&competition={competition_id}"
        response = self.session.get(url)
        content = response.text

        pattern = r'<td>(\d+) </td>\s*<td>\s*(.*?), (.*?)\s*</td>\s*<td>\s*(.*?) \((\d+)\)'

        matches_found = 0
        players_matched = 0

        for match in re.finditer(pattern, content, re.DOTALL):
            position, last_name, first_name, club, club_number = match.groups()
            position = int(position)
            matches_found += 1

            # Try to find the player by matching name and club
            player_id = self._find_player_by_name_and_club(first_name, last_name, club, club_number)

            if player_id:
                players_matched += 1
                self._update_player_results(player_id, tournament_name, competition_name, position)
            else:
                print(f"Could not match player: {first_name} {last_name} from {club} (club #{club_number})")

                # Track unmatched player for reporting
                self.unmatched_players.append({
                    'first_name': first_name,
                    'last_name': last_name,
                    'club': club,
                    'club_number': club_number,
                    'tournament': tournament_name,
                    'competition': competition_name,
                    'position': position
                })

        if matches_found > 0:
            print(f"Competition {competition_name}: Found {matches_found} results, matched {players_matched} players")

    def _find_player_by_name_and_club(self, first_name: str, last_name: str, club: str, club_number: str) -> Optional[
        str]:
        """Find a player by matching name and club information using database."""
        # First try to find by exact match using the XML participants if available
        for tournament_name, tournament in self.tournaments.items():
            if hasattr(tournament, 'participants'):
                name_club_id = self.replace_umlauts(f"{first_name}{last_name}{club_number}")
                if name_club_id in tournament.participants:
                    return tournament.participants[name_club_id]

        # Use database for better matching (includes historical club changes)
        player_id = self.db.find_player_by_name_and_club(first_name, last_name, club, club_number)
        if player_id:
            return player_id

        # Fallback to in-memory matching if database didn't find anything
        for player_id, player in self.players.items():
            # Try different matching strategies
            if (self._normalize_name(player.first_name) == self._normalize_name(first_name) and
                    self._normalize_name(player.last_name) == self._normalize_name(last_name) and
                    self._normalize_club(player.club) == self._normalize_club(club)):
                return player_id

            # Also try matching with club number if available
            if club_number and hasattr(player, 'club_number'):
                if (self._normalize_name(player.first_name) == self._normalize_name(first_name) and
                        self._normalize_name(player.last_name) == self._normalize_name(last_name) and
                        str(player.club_number) == club_number):
                    return player_id

        return None

    def _normalize_name(self, name: str) -> str:
        """Normalize a name for comparison by removing spaces and converting to lowercase."""
        return re.sub(r'\s+', '', name.lower())

    def _normalize_club(self, club: str) -> str:
        """Normalize a club name for comparison by removing spaces and converting to lowercase."""
        return re.sub(r'\s+', '', club.lower())

    def _update_player_results(self, player_id: str, tournament_name: str, competition_name: str,
                               position: int) -> None:
        """Update player results and points."""
        player = self.players[player_id]
        tournament = self.tournaments[tournament_name]

        # Check if player is age-eligible for current config before processing
        if not self.db._is_player_age_eligible(player.birth_year):
            logger.debug(
                f"Skipping tournament results for player {player.first_name} {player.last_name} - birth year {player.birth_year} is too old for current age classes")
            return

        # Update regional classification
        competition_key = f"{player.gender} {player.age_class}"
        if player.region not in self.regions:
            self.regions[player.region] = {}
        if competition_key not in self.regions[player.region]:
            self.regions[player.region][competition_key] = {}
        self.regions[player.region][competition_key][player_id] = 1

        # Calculate and update points
        points = (100 - position) * tournament.points
        if player.qttr:
            points += player.qttr / 1000
        player.points += points

        # Update tournament results
        if tournament_name not in player.tournaments:
            player.tournaments[tournament_name] = {}
        player.tournaments[tournament_name][competition_name] = position

    def generate_regional_reports(self) -> None:
        """Generate CSV reports for each region."""
        total_players_processed = 0

        for region in sorted(self.regions.keys()):
            players_in_region = sum(len(competitions) for competitions in self.regions[region].values())
            total_players_processed += players_in_region
            print(f"Region {region}: {players_in_region} players")
            self._generate_region_report(region)

        print(f"Total players processed: {total_players_processed}")
        print(f"Total players loaded: {len(self.players)}")

    def _generate_region_report(self, region: int) -> None:
        """Generate a CSV report for a specific region."""
        filename = f"{self.config['output']['folder']}/region{region}.csv"

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=self.config['output']['csv_delimiter'])
            writer.writerow([
                "Altersklasse", "Nachname", "Vorname", "Verein", "Jahrgang", "Bezirk",
                "BaWü_TOP1216_15-19", "BaWü_TOP12_13", "BaWü_JGRL", "Region_JGRL", "Region-EM", "QTTR"
            ])

            for competition in sorted(self.regions[region]):
                self._write_competition_results(writer, region, competition)

    def _write_competition_results(self, writer: csv.writer, region: int, competition: str) -> None:
        """Write results for a specific competition to the CSV."""
        # Group players by points for sorting
        players_by_points = defaultdict(dict)
        for player_id in self.regions[region][competition]:
            player = self.players[player_id]
            players_by_points[player.points][player_id] = player

        # Write players sorted by points (descending)
        for points in sorted(players_by_points.keys(), reverse=True):
            for player_id in sorted(players_by_points[points]):
                player = players_by_points[points][player_id]
                row = self._create_player_row(player, competition)
                writer.writerow(row)

    def _create_player_row(self, player: Player, competition: str) -> List[str]:
        """Create a CSV row for a player."""
        row = [
            competition, player.last_name, player.first_name, player.club,
            player.birth_year, player.district
        ]

        # Add tournament results
        results = ['-', '-', '-', '-', '-']
        for tournament_name, competitions in player.tournaments.items():
            for competition_name, position in competitions.items():
                result = f"{position}. {competition_name}"
                if tournament_name == "BaWü_TOP1216_15-19":
                    results[0] = result
                elif tournament_name == "BaWü_TOP12_13":
                    results[1] = result
                elif "BaWü_JGRL" in tournament_name:
                    results[2] = result
                elif "Region" in tournament_name and "JGRL" in tournament_name:
                    results[3] = result
                elif "Region" in tournament_name and "EM" in tournament_name:
                    results[4] = result  # Other tournaments

        row.extend(results)
        row.append(str(player.qttr) if player.qttr else "?")

        return row

    def generate_all_players_report(self) -> None:
        """Generate a comprehensive CSV report with all players across all regions."""
        filename = f"{self.config['output']['folder']}/all_players.csv"

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=self.config['output']['csv_delimiter'])
            writer.writerow([
                "Region", "Altersklasse", "Nachname", "Vorname", "Verein", "Jahrgang", "Bezirk",
                "Geschlecht", "QTTR", "Tournament_Count", "Total_Points"
            ])

            # Get all players from database
            all_players = self.db.get_all_current_players()

            # Sort players by region, then by last name, then by first name
            sorted_players = sorted(all_players, key=lambda p: (p.region, p.last_name, p.first_name))

            for player_record in sorted_players:
                # Get the corresponding Player object if it exists
                player = self.players.get(player_record.interne_lizenznr)

                # Calculate tournament count and total points
                tournament_count = len(player.tournaments) if player else 0
                total_points = player.points if player else 0.0

                # Determine age eligibility for current config
                is_age_eligible = self.db._is_player_age_eligible(player_record.birth_year)
                age_class_display = f"{player_record.age_class}{'*' if not is_age_eligible else ''}"

                row = [
                    player_record.region,
                    age_class_display,
                    player_record.last_name,
                    player_record.first_name,
                    player_record.club,
                    player_record.birth_year,
                    player_record.district,
                    player_record.gender,
                    str(player_record.qttr) if player_record.qttr else "?",
                    tournament_count,
                    f"{total_points:.2f}"
                ]
                writer.writerow(row)

        print(f"Generated comprehensive player report: {filename}")

    def generate_unmatched_players_report(self) -> None:
        """Generate a CSV report with players that couldn't be matched during tournament processing."""
        filename = f"{self.config['output']['folder']}/unmatched_players.csv"

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=self.config['output']['csv_delimiter'])
            writer.writerow([
                "Region", "Altersklasse", "Nachname", "Vorname", "Verein", "Jahrgang", "Bezirk",
                "Geschlecht", "QTTR", "Age_Eligible", "Reason"
            ])

            # Get all players from database
            all_players = self.db.get_all_current_players()

            unmatched_count = 0
            for player_record in all_players:
                # Check if player participated in any tournaments
                player = self.players.get(player_record.interne_lizenznr)
                participated_in_tournaments = player is not None and len(player.tournaments) > 0

                if not participated_in_tournaments:
                    unmatched_count += 1

                    # Determine age eligibility
                    is_age_eligible = self.db._is_player_age_eligible(player_record.birth_year)

                    # Determine reason for not being matched
                    if not is_age_eligible:
                        reason = "Too old for current age classes"
                    else:
                        reason = "No tournament participation"

                    row = [
                        player_record.region,
                        player_record.age_class,
                        player_record.last_name,
                        player_record.first_name,
                        player_record.club,
                        player_record.birth_year,
                        player_record.district,
                        player_record.gender,
                        str(player_record.qttr) if player_record.qttr else "?",
                        "Yes" if is_age_eligible else "No",
                        reason
                    ]
                    writer.writerow(row)

        print(f"Generated unmatched players report: {filename} ({unmatched_count} players)")

        # Also generate a detailed report of tournament-specific unmatched players
        if self.unmatched_players:
            tournament_unmatched_filename = f"{self.config['output']['folder']}/tournament_unmatched_players.csv"

            with open(tournament_unmatched_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=self.config['output']['csv_delimiter'])
                writer.writerow([
                    "Tournament", "Competition", "Position", "First_Name", "Last_Name",
                    "Club", "Club_Number", "Possible_Reasons"
                ])

                for unmatched in self.unmatched_players:
                    # Try to find potential reasons why this player couldn't be matched
                    possible_reasons = []

                    # First check if the club exists in the database at all
                    club_exists = self.db.club_exists(unmatched['club'])

                    db_players = self.db.get_all_current_players()

                    if not club_exists:
                        possible_reasons.append(
                            f"Club '{unmatched['club']}' not in database - not part of considered regions")
                    else:
                        # Check if player exists in database but with different club
                        for db_player in db_players:
                            if (db_player.first_name.lower() == unmatched['first_name'].lower() and
                                    db_player.last_name.lower() == unmatched['last_name'].lower()):
                                if db_player.club != unmatched['club']:
                                    possible_reasons.append(
                                        f"Club mismatch: DB has '{db_player.club}' vs tournament '{unmatched['club']}'")
                                if not self.db._is_player_age_eligible(db_player.birth_year):
                                    possible_reasons.append("Player too old for current age classes")
                                break
                        else:
                            possible_reasons.append("Player not found in database")

                    row = [
                        unmatched['tournament'],
                        unmatched['competition'],
                        unmatched['position'],
                        unmatched['first_name'],
                        unmatched['last_name'],
                        unmatched['club'],
                        unmatched['club_number'],
                        "; ".join(possible_reasons) if possible_reasons else "Unknown"
                    ]
                    writer.writerow(row)

            print(
                f"Generated tournament unmatched players report: {tournament_unmatched_filename} ({len(self.unmatched_players)} entries)")

    def generate_fuzzy_matches_report(self) -> None:
        """Generate a CSV report with all fuzzy matches that occurred during processing."""
        filename = f"{self.config['output']['folder']}/fuzzy_matches.csv"

        fuzzy_matches = self.db.get_fuzzy_matches_summary()

        if not fuzzy_matches:
            print("No fuzzy matches occurred during processing.")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=self.config['output']['csv_delimiter'])
            writer.writerow([
                "Tournament", "Tournament_First_Name", "Tournament_Last_Name", "Tournament_Club",
                "DB_First_Name", "DB_Last_Name", "DB_Club", "Old_Club", "Current_Club", "Match_Type"
            ])

            for match in fuzzy_matches:
                # Determine match type
                if match['tournament_first'] != match['db_first']:
                    match_type = "First Name Variant"
                elif match['tournament_last'] != match['db_last']:
                    match_type = "Last Name Variant"
                else:
                    match_type = "Name Variant"

                row = [
                    match['tournament_name'],
                    match['tournament_first'],
                    match['tournament_last'],
                    match['tournament_club'],
                    match['db_first'],
                    match['db_last'],
                    match['db_club'],
                    match.get('old_club', ''),
                    match.get('current_club', ''),
                    match_type
                ]
                writer.writerow(row)

        print(f"Generated fuzzy matches report: {filename} ({len(fuzzy_matches)} matches)")

    def _show_database_stats(self) -> None:
        """Display database statistics."""
        try:
            stats = self.db.get_database_stats()
            print("\nDatabase Statistics:")
            print(f"  Current players: {stats['current_players']}")
            print(f"  History records: {stats['history_records']}")
            if stats.get('oldest_eligible_birth_year'):
                print(f"  Oldest eligible birth year: {stats['oldest_eligible_birth_year']}")
                print(f"  Players eligible for tournaments: {stats['eligible_for_tournaments']}")
                print(f"  Players too old for tournaments: {stats['too_old_for_tournaments']}")

            # Show some example history records if available
            if stats['history_records'] > 0:
                print("\nExample of recent changes:")
                # Get a few recent history records
                with sqlite3.connect(self.db.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT interne_lizenznr, first_name, last_name, club, change_type, changed_at, previous_club
                        FROM player_history 
                        ORDER BY changed_at DESC 
                        LIMIT 5
                    """)

                    for row in cursor.fetchall():
                        lizenznr, first_name, last_name, club, change_type, changed_at, previous_club = row
                        if change_type == 'UPDATE' and previous_club and previous_club != club:
                            print(f"    {first_name} {last_name}: {previous_club} → {club} ({changed_at})")
                        elif change_type == 'INSERT':
                            print(f"    {first_name} {last_name}: New player at {club} ({changed_at})")

        except Exception as e:
            print(f"Error showing database stats: {e}")

    def run(self) -> None:
        """Execute the complete computation process."""
        print("Loading QTTR ratings...")
        self.load_qttr_ratings()

        print("Loading players...")
        self.load_players()

        print("Loading tournament participants...")
        self.load_tournament_participants()

        print("Processing tournament results...")
        self.process_tournament_results()

        print("Generating regional reports...")
        self.generate_regional_reports()

        print("Generating comprehensive player report...")
        self.generate_all_players_report()

        print("Generating unmatched players report...")
        self.generate_unmatched_players_report()

        print("Generating fuzzy matches report...")
        self.generate_fuzzy_matches_report()

        # Show database statistics
        self._show_database_stats()

        print("Rankings computed successfully!")


def main():
    """Main entry point for the script."""
    import sys

    config_file = "config_rem25.yaml"
    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    print(f"Using configuration file: {config_file}")
    processor = RankingProcessor(config_file)
    processor.run()


if __name__ == "__main__":
    main()
