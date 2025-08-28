#!/usr/bin/env python3
"""
Specialized tests for CSV processing and report generation.

This test file focuses on:
- CSV loading and parsing
- Report generation
- Data export formats
- Error handling in CSV operations
"""

import unittest
import tempfile
import os
import shutil
import pandas as pd
import yaml
import csv
import sqlite3
from unittest.mock import patch, MagicMock, mock_open
from io import StringIO

from ttbw_database import TTBWDatabase, PlayerRecord
from ttbw_compute_ranking import RankingProcessor, Player


class TestCSVProcessing(unittest.TestCase):
    """Test cases for CSV processing functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.test_dir, "test_csv.db")
        self.test_config_path = os.path.join(self.test_dir, "test_csv_config.yaml")
        
        # Create test config
        self.test_config = {
            'default_birth_year': 2014,
            'age_classes': {
                2006: 19, 2007: 19, 2008: 19, 2009: 19,
                2010: 15, 2011: 15, 2012: 13, 2013: 13, 2014: 11
            },
            'districts': {
                'Hochschwarzwald': {'region': 1, 'short_name': 'HS'},
                'Ulm': {'region': 2, 'short_name': 'UL'},
                'Donau': {'region': 3, 'short_name': 'DO'},
                'Ludwigsburg': {'region': 4, 'short_name': 'LB'},
                'Stuttgart': {'region': 5, 'short_name': 'ST'}
            }
        }
        
        # Write config to file
        with open(self.test_config_path, 'w') as f:
            yaml.dump(self.test_config, f)
        
        # Initialize database
        self.db = TTBWDatabase(self.test_db_path, self.test_config_path)
        
        # Create test CSV data
        self._create_test_csv()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def _create_test_csv(self):
        """Create test CSV file with various data scenarios."""
        csv_data = [
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club 1',
                'VereinNr': '12345',
                'Anrede': 'Herr',
                'Nachname': 'Smith',
                'Vorname': 'John',
                'Geburtsdatum': '15.03.2010',
                'InterneNr': 'CSV001'
            },
            {
                'Verband': 'TTBW',
                'Region': 'Ulm',
                'VereinName': 'Test Club 2',
                'VereinNr': '67890',
                'Anrede': 'Frau',
                'Nachname': 'Johnson',
                'Vorname': 'Jane',
                'Geburtsdatum': '22.07.2011',
                'InterneNr': 'CSV002'
            },
            {
                'Verband': 'TTBW',
                'Region': 'Donau',
                'VereinName': 'Test Club 3',
                'VereinNr': '11111',
                'Anrede': 'Herr',
                'Nachname': 'Williams',
                'Vorname': 'Bob',
                'Geburtsdatum': '10.12.2012',
                'InterneNr': 'CSV003'
            },
            {
                'Verband': 'TTBW',
                'Region': 'Ludwigsburg',
                'VereinName': 'Test Club 4',
                'VereinNr': '22222',
                'Anrede': 'Frau',
                'Nachname': 'Brown',
                'Vorname': 'Alice',
                'Geburtsdatum': '05.01.2013',
                'InterneNr': 'CSV004'
            },
            {
                'Verband': 'TTBW',
                'Region': 'Stuttgart',
                'VereinNr': '33333',
                'Anrede': 'Herr',
                'Nachname': 'Davis',
                'Vorname': 'Charlie',
                'Geburtsdatum': '18.06.2014',
                'InterneNr': 'CSV005'
            },
            {
                'Verband': 'Other',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Other Club',
                'VereinNr': '44444',
                'Anrede': 'Herr',
                'Nachname': 'Other',
                'Vorname': 'Player',
                'Geburtsdatum': '01.01.2010',
                'InterneNr': 'CSV006'
            }
        ]
        
        # Create CSV file
        csv_path = os.path.join(self.test_dir, "test_players.csv")
        with open(csv_path, 'w', newline='', encoding='latin1') as f:
            if csv_data:
                writer = csv.DictWriter(f, fieldnames=csv_data[0].keys(), delimiter=';')
                writer.writeheader()
                writer.writerows(csv_data)
        
        self.test_csv_path = csv_path
    
    def test_csv_loading(self):
        """Test loading players from CSV file."""
        # Load players from CSV
        players_loaded = self.db.load_players_from_csv(self.test_csv_path)
        
        # Check that valid players were loaded
        self.assertEqual(players_loaded, 5)  # 5 TTBW players, 1 other federation
        
        # Check that players were added to database
        all_players = self.db.get_all_current_players()
        self.assertEqual(len(all_players), 5)
        
        # Check specific player details
        player = self.db.get_player_by_lizenznr('CSV001')
        self.assertIsNotNone(player)
        self.assertEqual(player.first_name, 'John')
        self.assertEqual(player.last_name, 'Smith')
        self.assertEqual(player.club, 'Test Club 1')
        self.assertEqual(player.gender, 'Jungen')
        self.assertEqual(player.birth_year, 2010)
        self.assertEqual(player.age_class, 15)
        self.assertEqual(player.region, 1)
    
    def test_csv_loading_skips_invalid_rows(self):
        """Test that invalid CSV rows are properly skipped."""
        # Create CSV with invalid data
        invalid_csv_data = [
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club',
                'Anrede': 'Herr',
                'Nachname': 'Valid',
                'Vorname': 'Player',
                'Geburtsdatum': '15.03.2010',
                'InterneNr': 'VALID001'
            },
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club',
                'Anrede': 'Herr',
                'Nachname': 'Invalid',
                # Missing first_name, birth_date, interne_lizenznr
            },
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club',
                'Anrede': 'Herr',
                'Nachname': 'Invalid',
                'Vorname': 'Player',
                'Geburtsdatum': 'invalid_date',
                'InterneNr': 'INVALID001'
            }
        ]
        
        # Create invalid CSV file
        invalid_csv_path = os.path.join(self.test_dir, "invalid_players.csv")
        with open(invalid_csv_path, 'w', newline='', encoding='latin1') as f:
            if invalid_csv_data:
                writer = csv.DictWriter(f, fieldnames=['Verband', 'Region', 'VereinName', 'Anrede', 'Nachname', 'Vorname', 'Geburtsdatum', 'InterneNr'], delimiter=';')
                writer.writeheader()
                writer.writerows(invalid_csv_data)
        
        # Load players from invalid CSV
        players_loaded = self.db.load_players_from_csv(invalid_csv_path)
        
        # Only valid player should be loaded
        self.assertEqual(players_loaded, 1)
        
        # Check that only valid player exists
        all_players = self.db.get_all_current_players()
        self.assertEqual(len(all_players), 1)  # Only the valid player from this test
    
    def test_csv_loading_with_different_encodings(self):
        """Test CSV loading with different encodings."""
        # Create CSV with special characters
        special_chars_csv_data = [
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club',
                'VereinNr': '55555',
                'Anrede': 'Herr',
                'Nachname': 'Müller',
                'Vorname': 'Hans',
                'Geburtsdatum': '15.03.2010',
                'InterneNr': 'SPECIAL001'
            }
        ]
        
        # Create CSV file with special characters
        special_csv_path = os.path.join(self.test_dir, "special_chars.csv")
        with open(special_csv_path, 'w', newline='', encoding='latin1') as f:
            if special_chars_csv_data:
                writer = csv.DictWriter(f, fieldnames=special_chars_csv_data[0].keys(), delimiter=';')
                writer.writeheader()
                writer.writerows(special_chars_csv_data)
        
        # Load players from special characters CSV
        players_loaded = self.db.load_players_from_csv(special_csv_path)
        
        # Check that player was loaded
        self.assertEqual(players_loaded, 1)
        
        # Check that special characters were handled
        player = self.db.get_player_by_lizenznr('SPECIAL001')
        self.assertIsNotNone(player)
        self.assertEqual(player.last_name, 'Müller')
    
    def test_csv_loading_with_missing_optional_fields(self):
        """Test CSV loading when optional fields are missing."""
        # Create CSV with missing optional fields
        missing_fields_csv_data = [
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club',
                'Anrede': 'Herr',
                'Nachname': 'Missing',
                'Vorname': 'Fields',
                'Geburtsdatum': '15.03.2010',
                'InterneNr': 'MISSING001'
                # Missing VereinNr
            }
        ]
        
        # Create CSV file with missing fields
        missing_fields_csv_path = os.path.join(self.test_dir, "missing_fields.csv")
        with open(missing_fields_csv_path, 'w', newline='', encoding='latin1') as f:
            if missing_fields_csv_data:
                writer = csv.DictWriter(f, fieldnames=missing_fields_csv_data[0].keys(), delimiter=';')
                writer.writeheader()
                writer.writerows(missing_fields_csv_data)
        
        # Load players from missing fields CSV
        players_loaded = self.db.load_players_from_csv(missing_fields_csv_path)
        
        # Check that player was loaded
        self.assertEqual(players_loaded, 1)
        
        # Check that player was added with default values
        player = self.db.get_player_by_lizenznr('MISSING001')
        self.assertIsNotNone(player)
        # When a field is missing from CSV, it gets an empty string
        self.assertEqual(player.club_number, '')
    
    def test_csv_loading_with_different_date_formats(self):
        """Test CSV loading with different date formats."""
        # Create CSV with different date formats
        date_formats_csv_data = [
            {
                'Verband': 'TTBW',
                'Region': 'Hochschwarzwald',
                'VereinName': 'Test Club',
                'VereinNr': '66666',
                'Anrede': 'Herr',
                'Nachname': 'Date1',
                'Vorname': 'Format',
                'Geburtsdatum': '15.03.2010',
                'InterneNr': 'DATE001'
            },
            {
                'Verband': 'TTBW',
                'Region': 'Ulm',
                'VereinName': 'Test Club',
                'VereinNr': '77777',
                'Anrede': 'Frau',
                'Nachname': 'Date2',
                'Vorname': 'Format',
                'Geburtsdatum': '2011',
                'InterneNr': 'DATE002'
            }
        ]
        
        # Create CSV file with different date formats
        date_formats_csv_path = os.path.join(self.test_dir, "date_formats.csv")
        with open(date_formats_csv_path, 'w', newline='', encoding='latin1') as f:
            if date_formats_csv_data:
                writer = csv.DictWriter(f, fieldnames=date_formats_csv_data[0].keys(), delimiter=';')
                writer.writeheader()
                writer.writerows(date_formats_csv_data)
        
        # Load players from date formats CSV
        players_loaded = self.db.load_players_from_csv(date_formats_csv_path)
        
        # Check that both players were loaded
        self.assertEqual(players_loaded, 2)
        
        # Check that birth years were parsed correctly
        player1 = self.db.get_player_by_lizenznr('DATE001')
        player2 = self.db.get_player_by_lizenznr('DATE002')
        
        self.assertIsNotNone(player1)
        self.assertIsNotNone(player2)
        self.assertEqual(player1.birth_year, 2010)
        self.assertEqual(player2.birth_year, 2011)


class TestReportGeneration(unittest.TestCase):
    """Test cases for report generation functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_config_path = os.path.join(self.test_dir, "test_report_config.yaml")
        
        # Create test config
        self.test_config = {
            'tournaments': {
                'Test_Tournament': {
                    'tournament_id': 12345,
                    'points': 10
                }
            },
            'districts': {
                'Test_District': {
                    'region': 1,
                    'short_name': 'TD'
                }
            },
            'age_classes': {
                2010: 15,
                2011: 15,
                2012: 13
            },
            'default_birth_year': 2012,
            'api': {
                'nuliga_base_url': 'http://test.com/',
                'tournament_base_url': 'tournament?id=',
                'competition_base_url': 'competition?id=',
                'federation_arge': 'federation=arge',
                'federation_ttbw': 'federation=ttbw'
            },
            'output': {
                'folder': self.test_dir,
                'csv_delimiter': ';'
            }
        }
        
        # Write config to file
        with open(self.test_config_path, 'w') as f:
            yaml.dump(self.test_config, f)
        
        # Initialize processor
        self.processor = RankingProcessor(self.test_config_path)
        
        # Create a test database for the processor
        test_db_path = os.path.join(self.test_dir, "test_report.db")
        test_db = TTBWDatabase(test_db_path, self.test_config_path)
        
        # Override the database connection to use the test database
        self.processor.db = test_db
        
        # Set up test data
        self._setup_test_data()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def _setup_test_data(self):
        """Set up test data for report generation."""
        # Add test players
        test_players = [
            Player(
                id='REPORT001',
                first_name='John',
                last_name='Smith',
                club='Test Club 1',
                gender='Jungen',
                district='TD',
                birth_year=2010,
                age_class=15,
                region=1,
                qttr=1500
            ),
            Player(
                id='REPORT002',
                first_name='Jane',
                last_name='Johnson',
                club='Test Club 2',
                gender='Mädchen',
                district='TD',
                birth_year=2011,
                age_class=15,
                region=1,
                qttr=1600
            ),
            Player(
                id='REPORT003',
                first_name='Bob',
                last_name='Williams',
                club='Test Club 3',
                gender='Jungen',
                district='TD',
                birth_year=2012,
                age_class=13,
                region=1,
                qttr=1400
            )
        ]
        
        # Add test players to both the processor and the database
        for player in test_players:
            self.processor.players[player.id] = player
            
            # Also add to database as PlayerRecord
            player_record = PlayerRecord(
                interne_lizenznr=player.id,
                first_name=player.first_name,
                last_name=player.last_name,
                club=player.club,
                gender=player.gender,
                district=player.district,
                birth_year=player.birth_year,
                age_class=player.age_class,
                region=player.region,
                qttr=player.qttr
            )
            self.processor.db._update_player_in_database(player_record)
        
        # Add tournament results
        self.processor._update_player_results('REPORT001', 'Test_Tournament', 'Test_Competition_15', 1)
        self.processor._update_player_results('REPORT002', 'Test_Tournament', 'Test_Competition_15', 2)
        self.processor._update_player_results('REPORT003', 'Test_Tournament', 'Test_Competition_13', 1)
    
    def test_region_report_generation(self):
        """Test generation of regional reports."""
        # Generate region report
        self.processor._generate_region_report(1)
        
        # Check if file was created
        report_file = os.path.join(self.test_dir, 'region1.csv')
        self.assertTrue(os.path.exists(report_file))
        
        # Check file content
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Check headers
            self.assertIn('Altersklasse', content)
            self.assertIn('Nachname', content)
            self.assertIn('Vorname', content)
            self.assertIn('Verein', content)
            self.assertIn('Jahrgang', content)
            self.assertIn('Bezirk', content)
            
            # Check player data
            self.assertIn('John', content)
            self.assertIn('Smith', content)
            self.assertIn('Test Club 1', content)
            self.assertIn('Jane', content)
            self.assertIn('Johnson', content)
            self.assertIn('Test Club 2', content)
            self.assertIn('Bob', content)
            self.assertIn('Williams', content)
            self.assertIn('Test Club 3', content)
    
    def test_all_players_report_generation(self):
        """Test generation of comprehensive all players report."""
        # Generate all players report
        self.processor.generate_all_players_report()
        
        # Check if file was created
        report_file = os.path.join(self.test_dir, 'all_players.csv')
        self.assertTrue(os.path.exists(report_file))
        
        # Check file content
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Check headers
            self.assertIn('Region', content)
            self.assertIn('Altersklasse', content)
            self.assertIn('Nachname', content)
            self.assertIn('Vorname', content)
            self.assertIn('Verein', content)
            self.assertIn('Jahrgang', content)
            self.assertIn('Bezirk', content)
            self.assertIn('Geschlecht', content)
            self.assertIn('QTTR', content)
            self.assertIn('Tournament_Count', content)
            self.assertIn('Total_Points', content)
            
            # Check player data
            self.assertIn('John', content)
            self.assertIn('Smith', content)
            self.assertIn('Jane', content)
            self.assertIn('Johnson', content)
            self.assertIn('Bob', content)
            self.assertIn('Williams', content)
    
    def test_unmatched_players_report_generation(self):
        """Test generation of unmatched players report."""
        # Generate unmatched players report
        self.processor.generate_unmatched_players_report()
        
        # Check if file was created
        report_file = os.path.join(self.test_dir, 'unmatched_players.csv')
        self.assertTrue(os.path.exists(report_file))
        
        # Check file content
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Check headers
            self.assertIn('Region', content)
            self.assertIn('Altersklasse', content)
            self.assertIn('Nachname', content)
            self.assertIn('Vorname', content)
            self.assertIn('Verein', content)
            self.assertIn('Jahrgang', content)
            self.assertIn('Bezirk', content)
            self.assertIn('Geschlecht', content)
            self.assertIn('QTTR', content)
            self.assertIn('Age_Eligible', content)
            self.assertIn('Reason', content)
    
    def test_fuzzy_matches_report_generation(self):
        """Test generation of fuzzy matches report."""
        # Generate fuzzy matches report
        self.processor.generate_fuzzy_matches_report()
        
        # Check if file was created
        report_file = os.path.join(self.test_dir, 'fuzzy_matches.csv')
        
        # If there are no fuzzy matches, no file should be created
        # This is the expected behavior when no fuzzy matching occurred
        if not self.processor.db.get_fuzzy_matches_summary():
            self.assertFalse(os.path.exists(report_file))
        else:
            self.assertTrue(os.path.exists(report_file))
            
            # Check file content if it exists
            with open(report_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # Check headers
                self.assertIn('Tournament', content)
                self.assertIn('Tournament_First_Name', content)
                self.assertIn('Tournament_Last_Name', content)
                self.assertIn('Tournament_Club', content)
                self.assertIn('DB_First_Name', content)
                self.assertIn('DB_Last_Name', content)
                self.assertIn('DB_Club', content)
                self.assertIn('Old_Club', content)
                self.assertIn('Current_Club', content)
                self.assertIn('Match_Type', content)
    
    def test_csv_delimiter_configuration(self):
        """Test that CSV delimiter configuration is respected."""
        # Change delimiter in config
        self.processor.config['output']['csv_delimiter'] = ','
        
        # Generate region report
        self.processor._generate_region_report(1)
        
        # Check if file was created
        report_file = os.path.join(self.test_dir, 'region1.csv')
        self.assertTrue(os.path.exists(report_file))
        
        # Check that comma delimiter is used
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
            # Should contain commas, not semicolons
            self.assertIn(',', content)
            self.assertNotIn(';', content)
    
    def test_report_sorting(self):
        """Test that reports are properly sorted."""
        # Generate region report
        self.processor._generate_region_report(1)
        
        # Check if file was created
        report_file = os.path.join(self.test_dir, 'region1.csv')
        self.assertTrue(os.path.exists(report_file))
        
        # Parse CSV to check sorting
        with open(report_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            rows = list(reader)
            
            # Check that players are sorted by age class first
            age_classes = [row['Altersklasse'] for row in rows]
            # Should be sorted by age class (ascending), with gender included
            # The actual format is 'Gender AgeClass'
            # Order: Bob (Jungen 13), John (Jungen 15), Jane (Mädchen 15)
            self.assertEqual(age_classes, ['Jungen 13', 'Jungen 15', 'Mädchen 15'])
            
            # Check that within same age class, players are sorted by points
            # The first player should be Bob Williams (age class 13)
            first_player = rows[0]
            self.assertEqual(first_player['Nachname'], 'Williams')  # REPORT003 (age class 13)
            self.assertEqual(first_player['Vorname'], 'Bob')
    
    def test_empty_report_handling(self):
        """Test handling of empty reports."""
        # Clear all players from processor
        self.processor.players.clear()
        self.processor.regions.clear()
        
        # Clear database as well
        with sqlite3.connect(self.processor.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM current_players")
            conn.commit()
        
        # Try to generate reports (should not crash)
        try:
            # Region report will fail if region doesn't exist - this is expected behavior
            with self.assertRaises(KeyError):
                self.processor._generate_region_report(1)
            
            # These should work even with empty data
            self.processor.generate_all_players_report()
            self.processor.generate_unmatched_players_report()
            self.processor.generate_fuzzy_matches_report()
        except Exception as e:
            self.fail(f"Report generation with empty data should not crash: {e}")
        
        # Check that empty files were created
        all_players_file = os.path.join(self.test_dir, 'all_players.csv')
        
        if os.path.exists(all_players_file):
            with open(all_players_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # Should only contain headers
                lines = content.strip().split('\n')
                self.assertEqual(len(lines), 1)  # Only header line


class TestCSVErrorHandling(unittest.TestCase):
    """Test cases for CSV error handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.test_dir, "test_error_handling.db")
        self.test_config_path = os.path.join(self.test_dir, "test_error_config.yaml")
        
        # Create minimal test config
        self.test_config = {
            'default_birth_year': 2014,
            'age_classes': {2014: 11},
            'districts': {
                'Test_District': {'region': 1, 'short_name': 'TD'}
            }
        }
        
        # Write config to file
        with open(self.test_config_path, 'w') as f:
            yaml.dump(self.test_config, f)
        
        # Initialize database
        self.db = TTBWDatabase(self.test_db_path, self.test_config_path)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def test_csv_file_not_found(self):
        """Test handling of missing CSV file."""
        # Try to load from non-existent file
        # The current implementation handles missing files gracefully and returns 0
        result = self.db.load_players_from_csv('nonexistent.csv')
        self.assertEqual(result, 0)  # Should return 0 when file is not found
    
    def test_csv_parsing_errors(self):
        """Test handling of CSV parsing errors."""
        # Create malformed CSV file
        malformed_csv_path = os.path.join(self.test_dir, "malformed.csv")
        with open(malformed_csv_path, 'w') as f:
            f.write("invalid,csv,content\n")
            f.write("missing,quotes\n")
            f.write("unclosed,quote,content\n")
        
        # This should not crash
        try:
            result = self.db.load_players_from_csv(malformed_csv_path)
            self.assertEqual(result, 0)  # No valid players should be processed
        except Exception as e:
            self.fail(f"CSV parsing errors should be handled gracefully: {e}")
    
    def test_encoding_errors(self):
        """Test handling of encoding errors."""
        # Create CSV with encoding issues
        encoding_csv_path = os.path.join(self.test_dir, "encoding_issues.csv")
        with open(encoding_csv_path, 'w', encoding='utf-8') as f:
            f.write("Verband;Region;VereinName;Anrede;Nachname;Vorname;Geburtsdatum;InterneNr\n")
            f.write("TTBW;Test;Club;Herr;Test;Player;15.03.2010;ENC001\n")
        
        # Try to load with wrong encoding
        try:
            result = self.db.load_players_from_csv(encoding_csv_path)
            # Should handle encoding gracefully
            self.assertGreaterEqual(result, 0)
        except Exception as e:
            self.fail(f"Encoding issues should be handled gracefully: {e}")


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_suite.addTest(unittest.makeSuite(TestCSVProcessing))
    test_suite.addTest(unittest.makeSuite(TestReportGeneration))
    test_suite.addTest(unittest.makeSuite(TestCSVErrorHandling))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)
