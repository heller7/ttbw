#!/usr/bin/env python3
"""
Test script for TTBW Database functionality.
"""

from ttbw_database import TTBWDatabase
import os

def main():
    """Test the database functionality."""
    print("Testing TTBW Database functionality...")
    
    # Initialize database
    db = TTBWDatabase("test_ttbw.db", "config.yaml")
    
    # Check if CSV file exists
    if not os.path.exists("Spielberechtigungen.csv"):
        print("Error: Spielberechtigungen.csv not found!")
        print("Please make sure the CSV file is in the current directory.")
        return
    
    # Load players from CSV
    print("\nLoading players from CSV...")
    players_loaded = db.load_players_from_csv("Spielberechtigungen.csv")
    print(f"Loaded {players_loaded} players")
    
    # Show database statistics
    print("\nDatabase Statistics:")
    stats = db.get_database_stats()
    print(f"  Current players: {stats['current_players']}")
    print(f"  History records: {stats['history_records']}")
    if stats['oldest_eligible_birth_year']:
        print(f"  Oldest eligible birth year: {stats['oldest_eligible_birth_year']}")
        print(f"  Players eligible for tournaments: {stats['eligible_for_tournaments']}")
        print(f"  Players too old for tournaments: {stats['too_old_for_tournaments']}")
    
    # Get some example players
    print("\nExample players:")
    all_players = db.get_all_current_players()
    for i, player in enumerate(all_players[:5]):  # Show first 5 players
        print(f"  {i+1}. {player.first_name} {player.last_name} - {player.club} (Region {player.region})")
    
    # Test player search
    if all_players:
        test_player = all_players[0]
        print(f"\nTesting search for: {test_player.first_name} {test_player.last_name}")
        
        found_id = db.find_player_by_name_and_club(
            test_player.first_name, 
            test_player.last_name, 
            test_player.club
        )
        
        if found_id:
            print(f"  Found player with ID: {found_id}")
            
            # Show player history
            history = db.get_player_history(found_id)
            if history:
                print(f"  Player history: {len(history)} records")
                for record in history[:3]:  # Show first 3 history records
                    print(f"    {record['change_type']} at {record['changed_at']}")
        else:
            print("  Player not found!")
    
    # Test running the script again to see change tracking
    print("\n" + "="*50)
    print("Running CSV import again to test change tracking...")
    
    players_loaded_second = db.load_players_from_csv("Spielberechtigungen.csv")
    print(f"Second run loaded {players_loaded_second} players")
    
    # Show updated statistics
    print("\nUpdated Database Statistics:")
    stats_updated = db.get_database_stats()
    print(f"  Current players: {stats_updated['current_players']}")
    print(f"  History records: {stats_updated['history_records']}")
    if stats_updated['oldest_eligible_birth_year']:
        print(f"  Oldest eligible birth year: {stats_updated['oldest_eligible_birth_year']}")
        print(f"  Players eligible for tournaments: {stats_updated['eligible_for_tournaments']}")
        print(f"  Players too old for tournaments: {stats_updated['too_old_for_tournaments']}")
    
    print("\nDatabase test completed!")
    print("Note: The test database file 'test_ttbw.db' has been created.")
    print("You can inspect it using SQLite tools or delete it if not needed.")

if __name__ == "__main__":
    main()
