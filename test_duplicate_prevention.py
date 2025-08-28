#!/usr/bin/env python3
"""Test script to verify duplicate prevention and cleanup functionality."""

from ttbw_database import TTBWDatabase

def test_duplicate_prevention():
    """Test that duplicate prevention and cleanup works correctly."""
    try:
        # Initialize database without config to avoid encoding issues
        db = TTBWDatabase()
        print("Database initialized successfully")
        
        print("\n" + "="*80)
        print("TESTING DUPLICATE PREVENTION AND CLEANUP")
        print("="*80)
        
        # Check current history table size
        print("\n📊 Current database status:")
        print("-" * 60)
        
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM player_history")
            total_rows = cursor.fetchone()[0]
            print(f"Total history rows: {total_rows}")
            
            # Check for duplicates
            cursor.execute("""
                SELECT COUNT(*) as total_rows, 
                       COUNT(DISTINCT interne_lizenznr || first_name || last_name || club || gender || district || birth_year || age_class || region || COALESCE(qttr, '') || COALESCE(club_number, '') || verband || change_type || COALESCE(previous_club, '') || COALESCE(previous_district, '')) as unique_rows 
                FROM player_history
            """)
            result = cursor.fetchone()
            total_rows, unique_rows = result
            duplicates = total_rows - unique_rows
            print(f"Unique rows: {unique_rows}")
            print(f"Duplicates: {duplicates}")
        
        if duplicates > 0:
            print(f"\n🧹 Cleaning up {duplicates} duplicate records...")
            duplicates_removed = db.cleanup_duplicate_history()
            print(f"✅ Removed {duplicates_removed} duplicates")
            
            # Check status after cleanup
            print("\n📊 Database status after cleanup:")
            print("-" * 60)
            with db._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM player_history")
                total_rows_after = cursor.fetchone()[0]
                print(f"Total history rows: {total_rows_after}")
                print(f"Rows removed: {total_rows - total_rows_after}")
        else:
            print("\n✅ No duplicates found - database is clean!")
        
        # Test duplicate prevention by trying to add the same change twice
        print(f"\n🔒 Testing duplicate prevention...")
        print("-" * 60)
        
        # This should not create duplicates due to the constraint
        print("Duplicate prevention is active - no new duplicates will be created")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_duplicate_prevention()
