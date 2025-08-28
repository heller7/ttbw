#!/usr/bin/env python3
"""Test script to verify enhanced fuzzy matching CSV format."""

from ttbw_database import TTBWDatabase

def test_enhanced_csv_format():
    """Test that the enhanced fuzzy matching CSV includes old club information."""
    try:
        # Initialize database without config to avoid encoding issues
        db = TTBWDatabase()
        print("Database initialized successfully")
        
        print("\n" + "="*80)
        print("TESTING ENHANCED FUZZY MATCHING CSV FORMAT")
        print("="*80)
        
        # Test the Frieda case to generate a fuzzy match
        print(f"\n🔍 Testing: Frieda Löwe from Tischtennis Schönbuch")
        print("-" * 60)
        
        result = db.find_player_by_name_and_club('Frieda', 'Löwe', 'Tischtennis Schönbuch')
        
        if result:
            print(f"✅ SUCCESS: Found player ID: {result}")
            
            # Get player details from current table
            player = db.get_player_by_lizenznr(result)
            if player:
                print(f"   Current DB: {player.first_name} {player.last_name} from {player.club}")
                print(f"   Birth year: {player.birth_year}, Gender: {player.gender}")
                print(f"   District: {player.district}")
        else:
            print(f"❌ FAILED: No match found")
        
        # Show enhanced fuzzy matches summary
        print("\n" + "="*80)
        print("ENHANCED FUZZY MATCHES SUMMARY")
        print("="*80)
        fuzzy_matches = db.get_fuzzy_matches_summary()
        if fuzzy_matches:
            for i, match in enumerate(fuzzy_matches, 1):
                print(f"{i}. Tournament: {match['tournament_first']} {match['tournament_last']} from {match['tournament_club']}")
                print(f"   DB: {match['db_first']} {match['db_last']} from {match['db_club']}")
                print(f"   Old Club: {match.get('old_club', 'N/A')}")
                print(f"   Current Club: {match.get('current_club', 'N/A')}")
                print()
        else:
            print("No fuzzy matches recorded yet")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_csv_format()
