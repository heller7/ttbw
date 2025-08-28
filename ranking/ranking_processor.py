"""
Ranking processor for the TTBW system.
"""

import requests
import re
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
        self.regions: Dict[int, List[str]] = {}
        self._load_tournament_config()
        self._load_region_config()
    
    def _load_tournament_config(self) -> None:
        """Load tournament configuration from config."""
        tournament_config = self.db.config.get('tournaments', {})
        for tournament_id, points in tournament_config.items():
            self.tournaments[str(tournament_id)] = TournamentConfig(
                tournament_id=int(tournament_id),
                points=points
            )
    
    def _load_region_config(self) -> None:
        """Load region configuration from config."""
        districts_config = self.db.config.get('districts', {})
        for district_name, district_info in districts_config.items():
            region = district_info.get('region', 1)
            if region not in self.regions:
                self.regions[region] = []
            self.regions[region].append(district_name)
    
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
    
    def load_competitions_from_api(self, api_url: str) -> List[Dict[str, str]]:
        """
        Load competition data from external API.
        Returns list of competition dictionaries.
        """
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            # Extract competition IDs using regex
            content = response.text
            competition_pattern = r'competition_id["\']?\s*:\s*["\']?(\d+)["\']?'
            competitions = re.findall(competition_pattern, content)
            
            logger.info(f"Found {len(competitions)} competitions from API")
            return [{'id': comp_id} for comp_id in competitions]
            
        except requests.RequestException as e:
            logger.error(f"Error loading competitions from API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error loading competitions: {e}")
            return []
    
    def process_tournament_results(self, tournament_data: List[Dict[str, str]]) -> None:
        """
        Process tournament results and update player points.
        tournament_data should contain player results with fields:
        - first_name, last_name, club, club_number, tournament_id, result
        """
        for result in tournament_data:
            first_name = result.get('first_name', '')
            last_name = result.get('last_name', '')
            club = result.get('club', '')
            club_number = result.get('club_number')
            tournament_id = result.get('tournament_id', '')
            result_value = result.get('result', 0)
            
            if not all([first_name, last_name, club, tournament_id]):
                logger.warning(f"Incomplete tournament result: {result}")
                continue
            
            # Find player in database
            player_id = self.player_manager.find_player_by_name_and_club(
                first_name, last_name, club, club_number
            )
            
            if player_id and player_id in self.players:
                self._update_player_results(player_id, tournament_id, result_value)
            else:
                logger.warning(f"Player not found: {first_name} {last_name} from {club}")
    
    def _update_player_results(self, player_id: str, tournament_id: str, result: int) -> None:
        """Update player's tournament results and points."""
        if player_id not in self.players:
            return
        
        player = self.players[player_id]
        
        # Initialize tournament results if not exists
        if tournament_id not in player.tournaments:
            player.tournaments[tournament_id] = {}
        
        # Update result
        player.tournaments[tournament_id]['result'] = result
        
        # Calculate points based on tournament configuration
        if tournament_id in self.tournaments:
            tournament_config = self.tournaments[tournament_id]
            points = tournament_config.points
            
            # Simple point calculation (can be enhanced)
            if result == 1:  # First place
                player.points += points
            elif result == 2:  # Second place
                player.points += points * 0.8
            elif result == 3:  # Third place
                player.points += points * 0.6
            elif result <= 8:  # Top 8
                player.points += points * 0.4
            elif result <= 16:  # Top 16
                player.points += points * 0.2
            
            logger.debug(f"Updated {player.first_name} {player.last_name} with {points} points for result {result}")
    
    def get_player_ranking(self, region: Optional[int] = None, age_class: Optional[int] = None,
                          gender: Optional[str] = None) -> List[Player]:
        """
        Get player ranking based on optional filters.
        Returns sorted list of players by points.
        """
        filtered_players = []
        
        for player in self.players.values():
            # Apply filters
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
