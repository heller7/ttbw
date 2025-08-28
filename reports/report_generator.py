"""
Report generator for the TTBW system.
"""

import os
import pandas as pd
import logging
from typing import Dict, List, Optional
from models.player import Player
from database.database_manager import DatabaseManager
from database.player_manager import PlayerManager
from database.history_manager import HistoryManager

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates various reports for the TTBW system."""
    
    def __init__(self, database_manager: DatabaseManager):
        self.db_manager = database_manager
        self.player_manager = PlayerManager(database_manager)
        self.history_manager = HistoryManager(database_manager)
        self.regions: Dict[int, List[str]] = {}
        self._load_region_config()
    
    def _load_region_config(self) -> None:
        """Load region configuration from config."""
        districts_config = self.db_manager.config.get('districts', {})
        for district_name, district_info in districts_config.items():
            region = district_info.get('region', 1)
            if region not in self.regions:
                self.regions[region] = []
            self.regions[region].append(district_name)
    
    def generate_player_report(self, output_file: str, region: Optional[int] = None,
                             age_class: Optional[int] = None, gender: Optional[str] = None) -> int:
        """
        Generate a comprehensive player report.
        Returns the number of players in the report.
        """
        players = self.player_manager.get_all_current_players()
        
        # Apply filters
        filtered_players = []
        for player in players:
            if region is not None and player.region != region:
                continue
            if age_class is not None and player.age_class != age_class:
                continue
            if gender is not None and player.gender != gender:
                continue
            filtered_players.append(player)
        
        if not filtered_players:
            logger.warning("No players found for report generation")
            return 0
        
        # Prepare data for export
        data = []
        for player in filtered_players:
            data.append({
                'ID': player.interne_lizenznr,
                'First Name': player.first_name,
                'Last Name': player.last_name,
                'Club': player.club,
                'Gender': player.gender,
                'District': player.district,
                'Birth Year': player.birth_year,
                'Age Class': player.age_class,
                'Region': player.region,
                'QTTR': player.qttr,
                'Club Number': player.club_number or '',
                'Verband': player.verband
            })
        
        # Create DataFrame and export
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Generated player report with {len(filtered_players)} players: {output_file}")
        return len(filtered_players)
    
    def generate_region_report(self, region: int, output_file: str) -> int:
        """Generate a report for a specific region."""
        return self.generate_player_report(output_file, region=region)
    
    def generate_age_class_report(self, age_class: int, gender: Optional[str] = None,
                                output_file: str = None) -> int:
        """Generate a report for a specific age class."""
        if output_file is None:
            gender_suffix = f"_{gender}" if gender else ""
            output_file = f"age_class_{age_class}{gender_suffix}_report.csv"
        
        return self.generate_player_report(output_file, age_class=age_class, gender=gender)
    
    def generate_gender_report(self, gender: str, region: Optional[int] = None,
                             output_file: str = None) -> int:
        """Generate a report for a specific gender."""
        if output_file is None:
            region_suffix = f"_region_{region}" if region else ""
            output_file = f"{gender.lower()}{region_suffix}_report.csv"
        
        return self.generate_player_report(output_file, gender=gender, region=region)
    
    def generate_club_report(self, club_name: str, output_file: str = None) -> int:
        """Generate a report for a specific club."""
        if output_file is None:
            # Sanitize club name for filename
            safe_club_name = "".join(c for c in club_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_club_name = safe_club_name.replace(' ', '_')
            output_file = f"club_{safe_club_name}_report.csv"
        
        players = self.player_manager.get_all_current_players()
        club_players = [p for p in players if p.club.lower() == club_name.lower()]
        
        if not club_players:
            logger.warning(f"No players found for club: {club_name}")
            return 0
        
        # Prepare data for export
        data = []
        for player in club_players:
            data.append({
                'ID': player.interne_lizenznr,
                'First Name': player.first_name,
                'Last Name': player.last_name,
                'Gender': player.gender,
                'District': player.district,
                'Birth Year': player.birth_year,
                'Age Class': player.age_class,
                'Region': player.region,
                'QTTR': player.qttr,
                'Club Number': player.club_number or ''
            })
        
        # Create DataFrame and export
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Generated club report for {club_name} with {len(club_players)} players: {output_file}")
        return len(club_players)
    
    def generate_district_report(self, district_name: str, output_file: str = None) -> int:
        """Generate a report for a specific district."""
        if output_file is None:
            # Sanitize district name for filename
            safe_district_name = "".join(c for c in district_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_district_name = safe_district_name.replace(' ', '_')
            output_file = f"district_{safe_district_name}_report.csv"
        
        players = self.player_manager.get_all_current_players()
        district_players = [p for p in players if p.district.lower() == district_name.lower()]
        
        if not district_players:
            logger.warning(f"No players found for district: {district_name}")
            return 0
        
        # Prepare data for export
        data = []
        for player in district_players:
            data.append({
                'ID': player.interne_lizenznr,
                'First Name': player.first_name,
                'Last Name': player.last_name,
                'Club': player.club,
                'Gender': player.gender,
                'Birth Year': player.birth_year,
                'Age Class': player.age_class,
                'Region': player.region,
                'QTTR': player.qttr,
                'Club Number': player.club_number or ''
            })
        
        # Create DataFrame and export
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Generated district report for {district_name} with {len(district_players)} players: {output_file}")
        return len(district_players)
    
    def generate_fuzzy_matches_report(self, output_file: str = "fuzzy_matches_report.csv") -> int:
        """Generate a report of fuzzy matches for quality control."""
        fuzzy_matches = self.history_manager.get_fuzzy_matches()
        
        if not fuzzy_matches:
            logger.info("No fuzzy matches found for report")
            return 0
        
        # Prepare data for export
        data = []
        for match in fuzzy_matches:
            data.append({
                'Tournament Name': match['tournament_name'],
                'Tournament First Name': match['tournament_first'],
                'Tournament Last Name': match['tournament_last'],
                'Tournament Club': match['tournament_club'],
                'Database First Name': match['db_first'],
                'Database Last Name': match['db_last'],
                'Database Club': match['db_club'],
                'Match Timestamp': match['match_timestamp']
            })
        
        # Create DataFrame and export
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Generated fuzzy matches report with {len(fuzzy_matches)} matches: {output_file}")
        return len(fuzzy_matches)
    
    def generate_history_report(self, output_file: str = "player_history_report.csv",
                              start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
        """Generate a report of player history changes."""
        return self.history_manager.export_history_to_csv(output_file, start_date, end_date)
    
    def generate_statistics_report(self, output_file: str = "statistics_report.csv") -> int:
        """Generate a comprehensive statistics report."""
        players = self.player_manager.get_all_current_players()
        history_stats = self.history_manager.get_history_statistics()
        
        if not players:
            logger.warning("No players found for statistics report")
            return 0
        
        # Calculate player statistics
        total_players = len(players)
        
        # Count by region
        region_counts = {}
        for player in players:
            region_counts[player.region] = region_counts.get(player.region, 0) + 1
        
        # Count by age class
        age_class_counts = {}
        for player in players:
            age_class_counts[player.age_class] = age_class_counts.get(player.age_class, 0) + 1
        
        # Count by gender
        gender_counts = {}
        for player in players:
            gender_counts[player.gender] = gender_counts.get(player.gender, 0) + 1
        
        # Count by district
        district_counts = {}
        for player in players:
            district_counts[player.district] = district_counts.get(player.district, 0) + 1
        
        # Prepare data for export
        data = []
        
        # Overall statistics
        data.append({
            'Category': 'Overall',
            'Subcategory': 'Total Players',
            'Count': total_players,
            'Percentage': '100%'
        })
        
        # Region statistics
        for region, count in region_counts.items():
            percentage = (count / total_players * 100) if total_players > 0 else 0
            data.append({
                'Category': 'Region',
                'Subcategory': f'Region {region}',
                'Count': count,
                'Percentage': f'{percentage:.1f}%'
            })
        
        # Age class statistics
        for age_class, count in age_class_counts.items():
            percentage = (count / total_players * 100) if total_players > 0 else 0
            data.append({
                'Category': 'Age Class',
                'Subcategory': f'Age Class {age_class}',
                'Count': count,
                'Percentage': f'{percentage:.1f}%'
            })
        
        # Gender statistics
        for gender, count in gender_counts.items():
            percentage = (count / total_players * 100) if total_players > 0 else 0
            data.append({
                'Category': 'Gender',
                'Subcategory': gender,
                'Count': count,
                'Percentage': f'{percentage:.1f}%'
            })
        
        # District statistics
        for district, count in district_counts.items():
            percentage = (count / total_players * 100) if total_players > 0 else 0
            data.append({
                'Category': 'District',
                'Subcategory': district,
                'Count': count,
                'Percentage': f'{percentage:.1f}%'
            })
        
        # History statistics
        if history_stats:
            data.append({
                'Category': 'History',
                'Subcategory': 'Total History Records',
                'Count': history_stats.get('total_records', 0),
                'Percentage': 'N/A'
            })
            
            data.append({
                'Category': 'History',
                'Subcategory': 'Recent Activity (30 days)',
                'Count': history_stats.get('recent_activity_30_days', 0),
                'Percentage': 'N/A'
            })
        
        # Create DataFrame and export
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Generated statistics report: {output_file}")
        return len(data)
    
    def generate_all_reports(self, output_directory: str = "reports") -> Dict[str, int]:
        """Generate all available reports in the specified directory."""
        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        
        report_results = {}
        
        # Generate main player report
        main_report = os.path.join(output_directory, "all_players_report.csv")
        report_results['all_players'] = self.generate_player_report(main_report)
        
        # Generate region reports
        for region in self.regions.keys():
            region_report = os.path.join(output_directory, f"region_{region}_report.csv")
            report_results[f'region_{region}'] = self.generate_region_report(region, region_report)
        
        # Generate age class reports
        age_classes = [11, 13, 15, 19]
        for age_class in age_classes:
            # Boys
            boys_report = os.path.join(output_directory, f"age_class_{age_class}_boys_report.csv")
            report_results[f'age_class_{age_class}_boys'] = self.generate_age_class_report(
                age_class, gender="Jungen", output_file=boys_report
            )
            
            # Girls
            girls_report = os.path.join(output_directory, f"age_class_{age_class}_girls_report.csv")
            report_results[f'age_class_{age_class}_girls'] = self.generate_age_class_report(
                age_class, gender="Mädchen", output_file=girls_report
            )
        
        # Generate gender reports
        gender_report = os.path.join(output_directory, "gender_report.csv")
        report_results['gender'] = self.generate_gender_report("Jungen", output_file=gender_report)
        
        # Generate fuzzy matches report
        fuzzy_report = os.path.join(output_directory, "fuzzy_matches_report.csv")
        report_results['fuzzy_matches'] = self.generate_fuzzy_matches_report(fuzzy_report)
        
        # Generate statistics report
        stats_report = os.path.join(output_directory, "statistics_report.csv")
        report_results['statistics'] = self.generate_statistics_report(stats_report)
        
        logger.info(f"Generated all reports in directory: {output_directory}")
        return report_results
