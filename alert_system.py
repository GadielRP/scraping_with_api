import logging
from typing import Dict, List
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
            if odds and odds.get('one_open') and odds.get('two_open'):
                one_open = odds.get('one_open')
                x_open = odds.get('x_open')
                two_open = odds.get('two_open')
                one_final = odds.get('one_final')
                x_final = odds.get('x_final')
                two_final = odds.get('two_final')
                
                message += f"   💰 Odds:\n"
                message += f"      Opening: 1={one_open:.2f}"
                if x_open:
                    message += f", X={x_open:.2f}"
                message += f", 2={two_open:.2f}\n"
                
                if one_final and two_final:
                    message += f"      Final:   1={one_final:.2f}"
                    if x_final:
                        message += f", X={x_final:.2f}"
                    message += f", 2={two_final:.2f}\n"
                else:
                    message += f"      Final:   Not available\n"
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
    
    def send_telegram_message(self, message: str) -> bool:
        """Send a custom Telegram message (used by alert system)"""
        if not self.telegram_enabled:
            logger.warning("Telegram notifications not configured - cannot send alert message")
            return False
        
        logger.info("Sending alert message via Telegram...")
        return self._send_telegram_notification(message)
    
    
    def create_candidate_report_message(self, report_data: Dict) -> str:
        """
        Create a unified candidate report message covering all scenarios
        
        Args:
            report_data: Dictionary containing comprehensive candidate information
        
        Returns:
            Formatted message string for Telegram
        """
        # Extract report information
        participants = report_data.get('participants', 'Unknown vs Unknown')
        competition = report_data.get('competition', 'Unknown')
        sport = report_data.get('sport', 'Unknown')
        start_time = report_data.get('start_time', 'Unknown')
        minutes_until_start = report_data.get('minutes_until_start')
        status = report_data.get('status', 'unknown')
        primary_prediction = report_data.get('primary_prediction')
        primary_confidence = report_data.get('primary_confidence')
        odds_display = report_data.get('odds_display', 'Not available')
        vars_display = report_data.get('vars_display', 'Not available')
        has_draw_odds = report_data.get('has_draw_odds', False)
        
        tier1_data = report_data.get('tier1_candidates', {})
        tier2_data = report_data.get('tier2_candidates', {})
        
        tier1_count = tier1_data.get('count', 0)
        tier2_count = tier2_data.get('count', 0)
        
        # Determine message header
        if status == 'success':
            confidence_emoji = "✅" if primary_confidence == 'high' else "📊"
            header = f"{confidence_emoji} **CANDIDATE REPORT - SUCCESS**"
        elif status == 'no_match':
            header = f"❌ **CANDIDATE REPORT - NO MATCH**"
        else:
            header = f"❓ **CANDIDATE REPORT - UNKNOWN STATUS**"
        
        message = f"{header}\n\n"
        
        # Event information
        message += f"🏆 **{participants}**\n"
        message += f"🏟️ {competition} ({sport})\n"
        message += f"⏰ Starts at {start_time}"
        if minutes_until_start is not None:
            message += f" (in {minutes_until_start} minutes)"
        message += "\n\n"
        
        # Current event data
        message += f"📈 **Current Variations:**\n"
        message += f"   {vars_display}\n\n"
        
        message += f"💰 **Current Odds:**\n"
        message += f"   {odds_display}\n\n"
        
        # Candidate summary
        total_candidates = tier1_count + tier2_count
        message += f"🔍 **Candidate Summary:**\n"
        message += f"   • Tier 1 (exact): {tier1_count} candidates\n"
        message += f"   • Tier 2 (similar): {tier2_count} candidates\n"
        message += f"   • Total: {total_candidates} candidates\n\n"
        
        # Tier 1 candidates
        if tier1_count > 0:
            message += f"🎯 **Tier 1 - Exact Variations ({tier1_count}):**\n"
            
            # Check results
            tier1_identical = tier1_data.get('identical_results')
            tier1_similar = tier1_data.get('similar_results')
            
            if tier1_identical:
                prediction_text = tier1_identical.prediction if hasattr(tier1_identical, 'prediction') else tier1_identical.get('prediction', 'Unknown')
                message += f"   ✅ Identical Results: {prediction_text}\n"
            elif tier1_similar:
                prediction_text = tier1_similar.prediction if hasattr(tier1_similar, 'prediction') else tier1_similar.get('prediction', 'Unknown')
                message += f"   📊 Similar Results: {prediction_text}\n"
            else:
                message += f"   ❌ No consistent results found\n"
            
            # List matches with variations
            tier1_matches = tier1_data.get('matches', [])
            for i, match in enumerate(tier1_matches, 1):
                variations = match.get('variations', {})
                var_one = variations.get('var_one', 'N/A')
                var_x = variations.get('var_x')
                var_two = variations.get('var_two', 'N/A')
                
                # Format variations display based on sport type
                var_display = f"Δ1: {var_one}"
                if has_draw_odds:  # 3-way sport (Football, etc.)
                    if var_x is not None:
                        var_display += f", ΔX: {var_x:.2f}"
                    else:
                        var_display += f", ΔX: N/A"
                # For no-draw sports (Tennis, etc.), skip ΔX entirely
                var_display += f", Δ2: {var_two}"
                
                message += f"   {i}. {match['participants']} → {match['result_text']}\n"
                message += f"      Variations: {var_display}\n"
            message += "\n"
        
        # Tier 2 candidates
        if tier2_count > 0:
            message += f"📊 **Tier 2 - Similar Variations ({tier2_count}):**\n"
            
            # Check results
            tier2_identical = tier2_data.get('identical_results')
            tier2_similar = tier2_data.get('similar_results')
            
            if tier2_identical:
                prediction_text = tier2_identical.prediction if hasattr(tier2_identical, 'prediction') else tier2_identical.get('prediction', 'Unknown')
                message += f"   ✅ Identical Results: {prediction_text}\n"
            elif tier2_similar:
                prediction_text = tier2_similar.prediction if hasattr(tier2_similar, 'prediction') else tier2_similar.get('prediction', 'Unknown')
                message += f"   📊 Similar Results: {prediction_text}\n"
            else:
                message += f"   ❌ No consistent results found\n"
            
            # List matches with variations
            tier2_matches = tier2_data.get('matches', [])
            for i, match in enumerate(tier2_matches, 1):
                variations = match.get('variations', {})
                var_one = variations.get('var_one', 'N/A')
                var_x = variations.get('var_x')
                var_two = variations.get('var_two', 'N/A')
                
                # Format variations display based on sport type
                var_display = f"Δ1: {var_one}"
                if has_draw_odds:  # 3-way sport (Football, etc.)
                    if var_x is not None:
                        var_display += f", ΔX: {var_x:.2f}"
                    else:
                        var_display += f", ΔX: N/A"
                # For no-draw sports (Tennis, etc.), skip ΔX entirely
                var_display += f", Δ2: {var_two}"
                
                message += f"   {i}. {match['participants']} → {match['result_text']}\n"
                message += f"      Variations: {var_display}\n"
            message += "\n"
        
        # Primary prediction
        if primary_prediction:
            message += f"🎯 **Primary Prediction:** {primary_prediction}\n\n"
        else:
            message += f"❌ **No Prediction:** No consistent patterns found\n\n"
        
        # Footer
        message += "*Comprehensive candidate analysis completed*"
        
        return message
    

# Global notification instance
pre_start_notifier = PreStartNotification()
