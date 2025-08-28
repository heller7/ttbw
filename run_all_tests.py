#!/usr/bin/env python3
"""
Test runner script for TTBW system.

This script runs all test suites and provides comprehensive testing coverage.
"""

import unittest
import sys
import os
import time
from pathlib import Path

# Add current directory to Python path to ensure imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def discover_and_run_tests():
    """Discover and run all tests in the project."""
    print("=" * 80)
    print("TTBW SYSTEM COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print()
    
    # Start timing
    start_time = time.time()
    
    # Create test loader
    loader = unittest.TestLoader()
    
    # Discover all test files
    test_files = [
        'test_ttbw_comprehensive.py',
        'test_player_matching.py', 
        'test_csv_processing.py',
        'test_database.py',
        'test_duplicate_prevention.py',
        'test_enhanced_csv.py'
    ]
    
    # Create test suite
    all_tests = unittest.TestSuite()
    
    # Load tests from each file
    for test_file in test_files:
        if os.path.exists(test_file):
            print(f"Loading tests from: {test_file}")
            try:
                # Import the test module
                module_name = test_file.replace('.py', '')
                module = __import__(module_name)
                
                # Add tests to suite
                tests = loader.loadTestsFromModule(module)
                all_tests.addTests(tests)
                print(f"  ✓ Loaded {tests.countTestCases()} test cases")
                
            except Exception as e:
                print(f"  ✗ Error loading {test_file}: {e}")
        else:
            print(f"  - Skipping {test_file} (file not found)")
    
    print()
    print(f"Total test cases: {all_tests.countTestCases()}")
    print()
    
    # Run tests
    print("Running tests...")
    print("-" * 80)
    
    # Create test runner with detailed output
    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        descriptions=True,
        failfast=False
    )
    
    # Run tests
    result = runner.run(all_tests)
    
    # Print summary
    print()
    print("-" * 80)
    print("TEST SUMMARY")
    print("-" * 80)
    
    # Calculate timing
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    print(f"Duration: {duration:.2f} seconds")
    print()
    
    # Print detailed failure information
    if result.failures:
        print("FAILURES:")
        print("-" * 40)
        for test, traceback in result.failures:
            print(f"❌ {test}")
            print(f"   {traceback}")
            print()
    
    if result.errors:
        print("ERRORS:")
        print("-" * 40)
        for test, traceback in result.errors:
            print(f"💥 {test}")
            print(f"   {traceback}")
            print()
    
    # Print success/failure message
    if result.wasSuccessful():
        print("🎉 ALL TESTS PASSED!")
        print("✅ The TTBW system is working correctly.")
        return 0
    else:
        print("❌ SOME TESTS FAILED!")
        print("🔧 Please review the failures and errors above.")
        return 1


def run_specific_test_category(category):
    """Run tests for a specific category."""
    category_map = {
        'database': 'test_ttbw_comprehensive.py',
        'matching': 'test_player_matching.py',
        'csv': 'test_csv_processing.py',
        'comprehensive': 'test_ttbw_comprehensive.py'
    }
    
    if category not in category_map:
        print(f"Unknown test category: {category}")
        print(f"Available categories: {', '.join(category_map.keys())}")
        return 1
    
    test_file = category_map[category]
    
    if not os.path.exists(test_file):
        print(f"Test file not found: {test_file}")
        return 1
    
    print(f"Running {category} tests from {test_file}...")
    print()
    
    # Load and run specific test file
    loader = unittest.TestLoader()
    module_name = test_file.replace('.py', '')
    module = __import__(module_name)
    tests = loader.loadTestsFromModule(module)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(tests)
    
    return 0 if result.wasSuccessful() else 1


def run_quick_tests():
    """Run a quick subset of tests for development."""
    print("Running quick tests (core functionality only)...")
    print()
    
    # Only run comprehensive tests for quick testing
    return run_specific_test_category('comprehensive')


def show_test_coverage():
    """Show what areas are covered by tests."""
    print("TEST COVERAGE OVERVIEW")
    print("=" * 50)
    print()
    
    coverage_areas = {
        'Database Operations': [
            'Database initialization and table creation',
            'Configuration loading and fallback',
            'Player record management',
            'Change tracking and history',
            'Duplicate prevention and cleanup'
        ],
        'Player Matching': [
            'Exact name matching',
            'Fuzzy name variants',
            'Encoding variations',
            'Club matching strategies',
            'Historical data matching',
            'Age eligibility filtering'
        ],
        'CSV Processing': [
            'CSV loading and parsing',
            'Data validation and error handling',
            'Different encoding support',
            'Missing field handling',
            'Date format parsing'
        ],
        'Report Generation': [
            'Regional ranking reports',
            'Comprehensive player reports',
            'Unmatched players reports',
            'Fuzzy matches reports',
            'CSV format configuration'
        ],
        'Integration': [
            'Database to ranking integration',
            'Complete workflow testing',
            'Error handling and edge cases',
            'Configuration management'
        ]
    }
    
    for area, features in coverage_areas.items():
        print(f"{area}:")
        for feature in features:
            print(f"  ✓ {feature}")
        print()


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'all':
            return discover_and_run_tests()
        elif command == 'quick':
            return run_quick_tests()
        elif command == 'coverage':
            show_test_coverage()
            return 0
        elif command in ['database', 'matching', 'csv', 'comprehensive']:
            return run_specific_test_category(command)
        elif command == 'help':
            print("TTBW Test Runner Usage:")
            print()
            print("  python run_all_tests.py [command]")
            print()
            print("Commands:")
            print("  all          - Run all tests (default)")
            print("  quick        - Run quick tests for development")
            print("  database     - Run database tests only")
            print("  matching     - Run player matching tests only")
            print("  csv          - Run CSV processing tests only")
            print("  comprehensive - Run comprehensive tests only")
            print("  coverage     - Show test coverage overview")
            print("  help         - Show this help message")
            print()
            return 0
        else:
            print(f"Unknown command: {command}")
            print("Use 'python run_all_tests.py help' for usage information.")
            return 1
    else:
        # Default: run all tests
        return discover_and_run_tests()


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTest run interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error during test run: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
