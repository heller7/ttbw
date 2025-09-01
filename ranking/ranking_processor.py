"""
Ranking processor for the TTBW system.
"""

import requests
import re
import os
import logging
from typing import Dict, List, Optional, Tuple
from models.player import Player
from models.tournament import TournamentConfig
from database.database_manager import DatabaseManager
from database.player_manager import PlayerManager

logger = logging.getLogger(__name__)


class RankingProcessor:
    """Processes tournament data and computes rankings."""
    
    def __init__(self, db_path: str = "ttbw_players.db", config_file: str = "config.yaml"):
        self.db = DatabaseManager(db_path, config_file)
        self.player_manager = PlayerManager(self.db)
        self.players: Dict[str, Player] = {}
        self.tournaments: Dict[str, TournamentConfig] = {}
        self.regions: Dict[int, Dict[str, Dict[str, int]]] = {}
        self.session = requests.Session()
        self.unmatched_players = []
        self._load_tournament_config()
        self._load_region_config()
    
    def _load_tournament_config(self) -> None:
        """Load tournament configuration from config."""
        tournament_config = self.db.config.get('tournaments', {})
        for tournament_name, tournament_info in tournament_config.items():
            if isinstance(tournament_info, dict):
                # New format: tournament_name -> {tournament_id: X, points: Y}
                tournament_id = tournament_info.get('tournament_id')
                points = tournament_info.get('points', 1)
            else:
                # Old format: tournament_name -> points
                tournament_id = tournament_name
                points = tournament_info
            
            # Try to convert tournament_id to int if it's numeric
            try:
                tournament_id_int = int(tournament_id)
                self.tournaments[tournament_name] = TournamentConfig(
                    tournament_id=tournament_id_int,
                    points=points
                )
            except (ValueError, TypeError):
                # If tournament_id is not numeric, use the tournament name as key
                # and create a dummy tournament ID
                logger.warning(f"Tournament ID '{tournament_id}' is not numeric, using tournament name as key")
                self.tournaments[tournament_name] = TournamentConfig(
                    tournament_id=hash(tournament_name) % 1000000,  # Create a numeric ID from hash
                    points=points
                )
    
    def _load_region_config(self) -> None:
        """Load region configuration from config."""
        districts_config = self.db.config.get('districts', {})
        for district_name, district_info in districts_config.items():
            region = district_info.get('region', 1)
            if region not in self.regions:
                self.regions[region] = {}
    
    def load_players_from_database(self) -> None:
        """Load all players from database into memory."""
        player_records = self.player_manager.get_all_current_players()
        
        for record in player_records:
            player = Player(
                id=record.interne_lizenznr,
                first_name=record.first_name,
                last_name=record.last_name,
                club=record.club,
                gender=record.gender,
                district=record.district,
                birth_year=record.birth_year,
                age_class=record.age_class,
                region=record.region,
                qttr=record.qttr
            )
            self.players[player.id] = player
        
        logger.info(f"Loaded {len(self.players)} players from database")
    
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
        xml_filename = self._replace_umlauts(f"{tournament_name}_Turnierteilnehmer.xml")
        if os.path.exists(xml_filename):
            logger.info(f"Loading participants from XML file: {xml_filename}")
            self._load_participants_from_xml(tournament_name, xml_filename)
            logger.info(f"Loaded {len(tournament.participants)} participants from XML")
        else:
            logger.info(f"No XML file found for {tournament_name}, will use data matching")
        
        # Load competitions from web API
        logger.info(f"Loading competitions for {tournament_name} from API...")
        self._load_competitions_from_api(tournament_name)
        logger.info(f"Found {len(tournament.competitions)} competitions")
    
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
                        name_club_id = self._replace_umlauts(f"{first_name}{last_name}{club_number}")
                        tournament.participants[name_club_id] = player_id
        except Exception as e:
            logger.error(f"Error loading XML file {filename}: {e}")
            tournament.participants = {}
    
    def _load_competitions_from_api(self, tournament_name: str) -> None:
        """Load competitions for a tournament from the web API."""
        tournament = self.tournaments[tournament_name]
        
        # Determine federation based on tournament name
        api_config = self.db.config.get('api', {})
        federation = api_config.get('federation_arge') if tournament_name.startswith("BaWü") else api_config.get('federation_ttbw')
        
        if not federation:
            logger.warning(f"No federation configuration found for {tournament_name}")
            return
        
        url = f"{api_config.get('nuliga_base_url')}{api_config.get('tournament_base_url')}{tournament.tournament_id}&{federation}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            content = response.text
            
            tournament.competitions = {}
            pattern = r'<td>\s*<b>(\S+ \d+) Einzel</b>.*?<td> ja<.*?<a href=".*?competition=(\d+)">Teilnehmer'
            
            for match in re.finditer(pattern, content, re.DOTALL):
                competition_name, competition_id = match.groups()
                tournament.competitions[int(competition_id)] = competition_name
                
        except Exception as e:
            logger.error(f"Error loading competitions for {tournament_name}: {e}")
            tournament.competitions = {}
    
    def process_tournament_results(self) -> None:
        """Process results for all tournaments."""
        for tournament_name in sorted(self.tournaments.keys()):
            tournament = self.tournaments[tournament_name]
            if hasattr(tournament, 'competitions'):
                for competition_id, competition_name in sorted(tournament.competitions.items()):
                    self._process_competition_results(tournament_name, competition_id, competition_name)
    
    def _process_competition_results(self, tournament_name: str, competition_id: int, competition_name: str) -> None:
        """Process results for a specific competition."""
        api_config = self.db.config.get('api', {})
        federation = api_config.get('federation_arge') if tournament_name.startswith("BaWü") else api_config.get('federation_ttbw')
        
        if not federation:
            logger.warning(f"No federation configuration found for {tournament_name}")
            return
        
        url = f"{api_config.get('nuliga_base_url')}{api_config.get('competition_base_url')}{federation}&competition={competition_id}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
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
                    logger.warning(f"Could not match player: {first_name} {last_name} from {club} (club #{club_number})")
                    
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
                logger.info(f"Competition {competition_name}: Found {matches_found} results, matched {players_matched} players")
                
        except Exception as e:
            logger.error(f"Error processing competition {competition_id}: {e}")
    
    def _find_player_by_name_and_club(self, first_name: str, last_name: str, club: str, club_number: str) -> Optional[str]:
        """Find a player by matching name and club information."""
        # First try to find by exact match using the XML participants if available
        for tournament_name, tournament in self.tournaments.items():
            if hasattr(tournament, 'participants'):
                name_club_id = self._replace_umlauts(f"{first_name}{last_name}{club_number}")
                if name_club_id in tournament.participants:
                    return tournament.participants[name_club_id]
        
        # Use database for better matching (includes historical club changes)
        player_id = self.player_manager.find_player_by_name_and_club(first_name, last_name, club, club_number)
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
    
    def _replace_umlauts(self, text: str) -> str:
        """Replace German umlauts with their ASCII equivalents."""
        replacements = {
            'ö': 'oe', 'ä': 'ae', 'ü': 'ue', 'ß': 'ss',
            'Ö': 'Oe', 'Ä': 'Ae', 'Ü': 'Ue'
        }
        for umlaut, replacement in replacements.items():
            text = text.replace(umlaut, replacement)
        return text
    
    def _update_player_results(self, player_id: str, tournament_name: str, competition_name: str, position: int) -> None:
        """Update player results and points."""
        if player_id not in self.players:
            return
        
        player = self.players[player_id]
        tournament = self.tournaments[tournament_name]
        
        # Check if player is age-eligible for current config before processing
        age_classes = self.db.config.get('age_classes', {})
        oldest_eligible_birth_year = min(age_classes.keys()) if age_classes else 2000
        if player.birth_year < oldest_eligible_birth_year:
            logger.debug(f"Skipping tournament results for player {player.first_name} {player.last_name} - birth year {player.birth_year} is too old for current age classes")
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
    
    def get_unmatched_players(self) -> List[Dict]:
        """Get list of unmatched players for reporting."""
        return self.unmatched_players
    
    def get_player_ranking(self, region: Optional[int] = None, age_class: Optional[int] = None,
                          gender: Optional[str] = None) -> List[Player]:
        """Get ranking of players based on filters."""
        filtered_players = []
        
        for player in self.players.values():
            if region is not None and player.region != region:
                continue
            if age_class is not None and player.age_class != age_class:
                continue
            if gender is not None and player.gender != gender:
                continue
            
            filtered_players.append(player)
        
        # Sort by points (descending), then by name
        filtered_players.sort(key=lambda p: (-p.points, p.last_name, p.first_name))
        
        return filtered_players
    
    def get_region_ranking(self, region: int) -> List[Player]:
        """Get ranking for a specific region."""
        return self.get_player_ranking(region=region)
    
    def get_age_class_ranking(self, age_class: int, gender: Optional[str] = None) -> List[Player]:
        """Get ranking for a specific age class."""
        return self.get_player_ranking(age_class=age_class, gender=gender)
    
    def get_gender_ranking(self, gender: str, region: Optional[int] = None) -> List[Player]:
        """Get ranking for a specific gender."""
        return self.get_player_ranking(gender=gender, region=region)
    
    def get_top_players(self, limit: int = 10, region: Optional[int] = None,
                       age_class: Optional[int] = None, gender: Optional[str] = None) -> List[Player]:
        """Get top N players based on filters."""
        ranking = self.get_player_ranking(region, age_class, gender)
        return ranking[:limit]
    
    def get_player_statistics(self) -> Dict[str, any]:
        """Get overall player statistics."""
        if not self.players:
            return {}
        
        total_players = len(self.players)
        total_points = sum(p.points for p in self.players.values())
        avg_points = total_points / total_players if total_players > 0 else 0
        
        # Count by region
        region_counts = {}
        for player in self.players.values():
            region_counts[player.region] = region_counts.get(player.region, 0) + 1
        
        # Count by age class
        age_class_counts = {}
        for player in self.players.values():
            age_class_counts[player.age_class] = age_class_counts.get(player.age_class, 0) + 1
        
        # Count by gender
        gender_counts = {}
        for player in self.players.values():
            gender_counts[player.gender] = gender_counts.get(player.gender, 0) + 1
        
        return {
            'total_players': total_players,
            'total_points': total_points,
            'average_points': round(avg_points, 2),
            'region_distribution': region_counts,
            'age_class_distribution': age_class_counts,
            'gender_distribution': gender_counts
        }
    
    def export_ranking_to_csv(self, output_file: str, region: Optional[int] = None,
                             age_class: Optional[int] = None, gender: Optional[str] = None) -> int:
        """
        Export ranking to CSV file.
        Returns the number of players exported.
        """
        import pandas as pd
        
        ranking = self.get_player_ranking(region, age_class, gender)
        
        if not ranking:
            logger.warning("No players found for export")
            return 0
        
        # Prepare data for export
        data = []
        for i, player in enumerate(ranking, 1):
            data.append({
                'Rank': i,
                'ID': player.id,
                'First Name': player.first_name,
                'Last Name': player.last_name,
                'Club': player.club,
                'Gender': player.gender,
                'District': player.district,
                'Age Class': player.age_class,
                'Region': player.region,
                'QTTR': player.qttr,
                'Points': player.points,
                'Tournaments': len(player.tournaments)
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Exported {len(ranking)} players to {output_file}")
        return len(ranking)
