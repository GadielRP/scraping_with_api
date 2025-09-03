import logging
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)

class PreStartNotification:
    """Notification system for upcoming games starting within 30 minutes - Telegram only"""
    
    def __init__(self):
        self.notification_enabled = True
        self.telegram_enabled = False
        
        # Load notification settings from config
        self._load_notification_settings()
    
    def _load_notification_settings(self):
        """Load notification settings from environment variables"""
        import os
        from dotenv import load_dotenv
        load_dotenv()
        
        # Telegram settings
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
        self.telegram_enabled = bool(self.telegram_bot_token and self.telegram_chat_id)
        
        logger.info(f"Telegram notification: {'✅ Enabled' if self.telegram_enabled else '❌ Disabled'}")
        if not self.telegram_enabled:
            logger.warning("Telegram not configured. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env file")
    
    def notify_upcoming_games(self, upcoming_events: List[Dict]) -> bool:
        """
        Send notifications about upcoming games starting within 30 minutes
        
        Args:
            upcoming_events: List of events with start time, teams, minutes until start, and odds data
        
        Returns:
            True if notifications were sent successfully
        """
        if not self.notification_enabled or not upcoming_events:
            return False
        
        try:
            # Create notification message
            message = self._create_upcoming_games_message(upcoming_events)
            
            # Send Telegram notification
            if self.telegram_enabled:
                success = self._send_telegram_notification(message)
                if success:
                    logger.info(f"✅ Successfully sent Telegram notification for {len(upcoming_events)} upcoming games")
                else:
                    logger.warning("⚠️ Failed to send Telegram notification")
                return success
            else:
                logger.warning("Telegram notifications not configured")
                return False
            
        except Exception as e:
            logger.error(f"Error sending upcoming games notifications: {e}")
            return False
    
    def _create_upcoming_games_message(self, upcoming_events: List[Dict]) -> str:
        """Create a formatted message for upcoming games with odds information"""
        if not upcoming_events:
            return "No upcoming games found."
        
        message = "🚨 UPCOMING GAMES ALERT 🚨\n\n"
        message += f"Found {len(upcoming_events)} game(s) starting within 30 minutes:\n\n"
        
        for event in upcoming_events:
            minutes = event.get('minutes_until_start', 0)
            home_team = event.get('home_team', 'Unknown')
            away_team = event.get('away_team', 'Unknown')
            start_time = event.get('start_time', 'Unknown')
            competition = event.get('competition', 'Unknown')
            odds = event.get('odds', None)
            
            message += f"⚽ {home_team} vs {away_team}\n"
            message += f"   🏆 {competition}\n"
            message += f"   ⏰ Starts in {minutes} minutes ({start_time})\n"
            
            # Add odds information if available
            if odds:
                # Get both opening and final odds
                one_open = odds.get('one_open')
                x_open = odds.get('x_open')
                two_open = odds.get('two_open')
                one_final = odds.get('one_final')
                x_final = odds.get('x_final')
                two_final = odds.get('two_final')
                
                # Check if we have any valid odds data (opening or final)
                # For opening odds: require both 1 and 2 (X is optional for 2-choice sports)
                has_opening_odds = one_open and two_open
                
                # For final odds: require both 1 and 2 (X is optional for 2-choice sports)
                # This handles both 2-choice (Tennis) and 3-choice (Football) markets
                has_final_odds = one_final and two_final
                
                if has_opening_odds or has_final_odds:
                    message += f"   💰 Odds:\n"
                    
                    # Show opening odds if available
                    if has_opening_odds:
                        message += f"      Opening: 1={one_open:.2f}"
                        if x_open:  # Some sports don't have draw options
                            message += f", X={x_open:.2f}"
                        message += f", 2={two_open:.2f}\n"
                    else:
                        message += f"      Opening: Not available\n"
                    
                    # Show final odds if available
                    if has_final_odds:
                        message += f"      Final:   1={one_final:.2f}"
                        if x_final:  # Some sports don't have draw options
                            message += f", X={x_final:.2f}"
                        message += f", 2={two_final:.2f}\n"
                    else:
                        message += f"      Final:   Not available\n"
                else:
                    message += f"   💰 Odds: Not available\n"
            else:
                message += f"   💰 Odds: Not available\n"
            
            message += "\n"
        
        message += "🎯 Check SofaScore for final odds and place your bets!"
        return message
    
    def _send_telegram_notification(self, message: str) -> bool:
        """Send notification via Telegram bot"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                logger.info("Telegram notification sent successfully")
                return True
            else:
                logger.error(f"Telegram notification failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending Telegram notification: {e}")
            return False
    
    def test_notifications(self) -> bool:
        """Test Telegram notification with a test message"""
        test_message = "🧪 TEST NOTIFICATION\n\nThis is a test message from your SofaScore Odds System.\n\nIf you receive this, your Telegram notifications are working correctly! ✅"
        
        logger.info("Testing Telegram notification...")
        return self._send_telegram_notification(test_message)

# Global notification instance
pre_start_notifier = PreStartNotification()
