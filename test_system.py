#!/usr/bin/env python3
"""
Test script for the SofaScore Odds Alert System

This script tests various components of the system to ensure everything is working correctly.
"""

import logging
import sys
from datetime import datetime

from config import Config
from database import db_manager
from odds_utils import fractional_to_decimal, process_event_odds
from sofascore_api import api_client
from repository import EventRepository, OddsRepository
from alert_system import alert_engine

def setup_logging():
    """Setup logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def test_odds_conversion():
    """Test fractional to decimal odds conversion"""
    print("\n=== Testing Odds Conversion ===")
    
    test_cases = [
        ("3/5", 1.60),
        ("7/2", 4.50),
        ("1/1", 2.00),
        ("2/1", 3.00),
        ("1/2", 1.50),
        ("5/1", 6.00),
    ]
    
    for fractional, expected in test_cases:
        result = fractional_to_decimal(fractional)
        if result:
            decimal_value = float(result)
            status = "✓" if abs(decimal_value - expected) < 0.01 else "✗"
            print(f"{status} {fractional} → {decimal_value} (expected: {expected})")
        else:
            print(f"✗ {fractional} → Failed to convert")

def test_database_connection():
    """Test database connection and table creation"""
    print("\n=== Testing Database Connection ===")
    
    try:
        # Test connection
        if db_manager.test_connection():
            print("✓ Database connection successful")
        else:
            print("✗ Database connection failed")
            return False
        
        # Create tables
        db_manager.create_tables()
        print("✓ Database tables created successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Database error: {e}")
        return False

def test_api_connection():
    """Test SofaScore API connection"""
    print("\n=== Testing API Connection ===")
    
    try:
        # Test dropping odds endpoint
        response = api_client.get_dropping_odds()
        
        if response and 'events' in response:
            events = api_client.extract_events_from_dropping_odds(response)
            
            print(f"✓ API connection successful - Found {len(events)} events")
            return True
        else:
            print("✗ API connection failed - No events found")
            return False
            
    except Exception as e:
        print(f"✗ API error: {e}")
        return False

def test_odds_processing():
    """Test odds processing with sample data"""
    print("\n=== Testing Odds Processing ===")
    
    # Sample odds changes data (from your changes.json)
    sample_changes = [
        {
            "timestamp": "1756293835",
            "choice1": {"name": "1", "fractionalValue": "3/5"},
            "choice2": {"name": "X", "fractionalValue": "27/10"},
            "choice3": {"name": "2", "fractionalValue": "7/2"}
        },
        {
            "timestamp": "1756365813",
            "choice1": {"name": "1", "fractionalValue": "13/20"},
            "choice2": {"name": "X", "fractionalValue": "13/5"},
            "choice3": {"name": "2", "fractionalValue": "10/3"}
        }
    ]
    
    try:
        odds_data = process_event_odds(sample_changes)
        
        if odds_data:
            print("✓ Odds processing successful")
            print(f"  Open odds: 1={odds_data.get('one_open')}, X={odds_data.get('x_open')}, 2={odds_data.get('two_open')}")
            print(f"  Current odds: 1={odds_data.get('one_cur')}, X={odds_data.get('x_cur')}, 2={odds_data.get('two_cur')}")
            return True
        else:
            print("✗ Odds processing failed")
            return False
            
    except Exception as e:
        print(f"✗ Odds processing error: {e}")
        return False

def test_repository_operations():
    """Test repository operations"""
    print("\n=== Testing Repository Operations ===")
    
    try:
        # Test event repository
        event_repo = EventRepository()
        
        # Sample event data
        sample_event = {
            'id': 999999,
            'customId': 'TEST123',
            'slug': 'test-event',
            'startTimestamp': int(datetime.now().timestamp()) + 3600,  # 1 hour from now
            'sport': 'Football',
            'competition': 'Test League',
            'country': 'Test Country',
            'homeTeam': 'Test Home Team',
            'awayTeam': 'Test Away Team'
        }
        
        # Test event upsert
        event = event_repo.upsert_event(sample_event)
        if event:
            print("✓ Event repository operations successful")
            
            # Clean up test data
            with db_manager.get_session() as session:
                from models import Event
                test_event = session.query(Event).filter(Event.id == 999999).first()
                if test_event:
                    session.delete(test_event)
                    session.commit()
                    print("✓ Test data cleaned up")
            
            return True
        else:
            print("✗ Event repository operations failed")
            return False
            
    except Exception as e:
        print(f"✗ Repository error: {e}")
        return False

def test_alert_system():
    """Test alert system with sample data"""
    print("\n=== Testing Alert System ===")
    
    try:
        # Create a test event with odds
        with db_manager.get_session() as session:
            from models import Event, EventOdds
            
            # Create test event
            test_event = Event(
                id=888888,
                slug='test-alert-event',
                start_time_utc=datetime.utcnow(),
                sport='Football',
                competition='Test League',
                home_team='Home Team',
                away_team='Away Team'
            )
            session.add(test_event)
            
            # Create test odds with significant drop
            test_odds = EventOdds(
                event_id=888888,
                market='1X2',
                one_open=2.00,  # Opening odds
                x_open=3.50,
                two_open=3.00,
                one_final=1.50,  # Final odds (significant drop)
                x_final=4.00,
                two_final=4.50,
                last_sync_at=datetime.utcnow()
            )
            session.add(test_odds)
            session.commit()
            
            # Test alert evaluation
            alerts = alert_engine.evaluate_event(test_odds)
            
            if alerts:
                print(f"✓ Alert system working - Generated {len(alerts)} alerts")
                for alert in alerts:
                    print(f"  - {alert['rule_key']}: {alert['description']}")
            else:
                print("✓ Alert system working - No alerts triggered (expected)")
            
            # Clean up test data
            session.delete(test_odds)
            session.delete(test_event)
            session.commit()
            print("✓ Test data cleaned up")
            
            return True
            
    except Exception as e:
        print(f"✗ Alert system error: {e}")
        return False

def test_scheduler():
    """Test scheduler functionality"""
    print("\n=== Testing Scheduler ===")
    
    try:
        from scheduler import job_scheduler
        
        # Test job scheduling
        jobs = job_scheduler.get_scheduled_jobs()
        if jobs:
            print(f"✓ Scheduler working - {len(jobs)} jobs scheduled")
            for job in jobs:
                print(f"  - {job['function']}: {job['interval']} {job['unit']}")
            return True
        else:
            print("✗ No jobs scheduled")
            return False
            
    except Exception as e:
        print(f"✗ Scheduler error: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    print("Starting SofaScore Odds System Tests")
    print("=" * 50)
    
    tests = [
        ("Odds Conversion", test_odds_conversion),
        ("Database Connection", test_database_connection),
        ("API Connection", test_api_connection),
        ("Odds Processing", test_odds_processing),
        ("Repository Operations", test_repository_operations),
        ("Alert System", test_alert_system),
        ("Scheduler", test_scheduler),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        icon = "✓" if result else "✗"
        print(f"{icon} {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! System is ready to use.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        return False

if __name__ == '__main__':
    setup_logging()
    success = run_all_tests()
    sys.exit(0 if success else 1)
