"""
Alert System - Telegram notification system for Process 1

PROCESS 1 BOUNDARIES:
====================
START: This file contains Process 1 notification system
END: Process 1 notification system ends at the end of this file

PROCESS 1 INTEGRATION:
This file handles Telegram notifications for Process 1 candidate reports.
It formats and sends Process 1 results (SUCCESS/NO MATCH/NO CANDIDATES).

PROCESS 2 PREPARATION:
Process 2 notifications will be handled by separate files or extended
functionality in this file with clear Process 2 boundaries.
"""

import logging
from typing import Dict, List, Optional
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
        selected_tier = report_data.get('selected_tier', 'Unknown')
        primary_prediction = report_data.get('primary_prediction')
        primary_confidence = report_data.get('primary_confidence')
        successful_candidates = report_data.get('successful_candidates', 0)
        total_candidates = report_data.get('total_candidates', 0)
        odds_display = report_data.get('odds_display', 'Not available')
        vars_display = report_data.get('vars_display', 'Not available')
        has_draw_odds = report_data.get('has_draw_odds', False)
        
        tier1_data = report_data.get('tier1_candidates', {})
        tier2_data = report_data.get('tier2_candidates', {})
        
        tier1_count = tier1_data.get('count', 0)
        tier2_count = tier2_data.get('count', 0)
        
        # Determine message header
        status_headers = {
            'success': "✅ CANDIDATE REPORT - SUCCESS",
            'partial': "⚠️ CANDIDATE REPORT - PARTIAL",
            'no_match': "❌ CANDIDATE REPORT - NO MATCH",
            'no_candidates': "❓ CANDIDATE REPORT - NO  VALID CANDIDATES"
        }
        header = status_headers.get(status, "❓ CANDIDATE REPORT - UNKNOWN STATUS")
        
        message = f"{header}\n\n"
        
        # Event information
        message += f"🏆 {participants}\n"
        message += f"🏟️ {competition} ({sport})\n"
        
        
        message += f"⏰ Starts at {start_time}"
        if minutes_until_start is not None:
            message += f" (in {minutes_until_start} minutes)"
        message += "\n\n"
        
        # Current event data
        message += f"📈 Current Variations:\n"
        message += f"   {vars_display}\n\n"
        
        message += f"💰 Current Odds:\n"
        message += f"   {odds_display}\n\n"
        
        # Candidate summary with new logic
        total_candidates_found = tier1_count + tier2_count
        non_symmetrical_count = report_data.get('non_symmetrical_candidates', 0)
        
        message += f"🔍 Candidate Summary:\n"
        message += f"   • Tier 1 (exact): {tier1_count} candidates\n"
        message += f"   • Tier 2 (similar): {tier2_count} candidates"
        if non_symmetrical_count > 0:
            message += f" ({non_symmetrical_count} non-symmetrical filtered out)"
        message += f"\n"
        message += f"   • Selected tier: {selected_tier}\n"
        message += f"   • Successful: {successful_candidates}/{total_candidates} candidates\n"
        message += f"   • Confidence: {primary_confidence}\n\n"
        
        # Add rule activations summary
        rule_activations = report_data.get('rule_activations', {})
        if rule_activations:
            message += self._format_rule_activations(rule_activations)
        
        # Show candidates from all available tiers
        if tier1_count > 0:
            message += self._format_tier_candidates("🎯", "Tier 1 - Exact Variations", 
                                                 tier1_count, tier1_data.get('matches', []), has_draw_odds)
        if tier2_count > 0:
            # Always show ALL Tier 2 candidates with symmetrical status indicators
            message += self._format_tier_candidates("📊", "Tier 2 - Similar Variations", 
                                                 tier2_count, tier2_data.get('matches', []), has_draw_odds)
        
        # Primary prediction
        if primary_prediction:
            message += f"🎯 **Primary Prediction:** {primary_prediction}\n\n"
        elif status == 'partial':
            message += f"⚠️ **Partial prediction:** No consistent patterns found, need at least 2 candidates\n\n"
        else:
            message += f"❌ **No Prediction:** No consistent patterns found\n\n"
        
        # Footer
        message += "*Comprehensive candidate analysis completed*"
        
        return message
    
    def _format_tier_candidates(self, icon: str, title: str, count: int, 
                              matches: List[Dict], has_draw_odds: bool) -> str:
        """Format tier candidates for display"""
        message = f"\n{icon} **{title} ({count}):**\n"
            
        for i, match in enumerate(matches, 1):
            var_display = self._format_variations_display(match.get('variations', {}), has_draw_odds)
            symmetry_status = ""
            if 'is_symmetrical' in match:
                if match['is_symmetrical']:
                    symmetry_status = " ✅"
                else:
                    symmetry_status = " ❌ (unsymmetrical)"
            
            message += f"   {i}. {match['participants']} → {match['result_text']}{symmetry_status}\n"
            message += f"      Competition: {match.get('competition', 'Unknown')}\n"
            message += f"      Variations: {var_display}\n\n"
            
            # Add variation differences for Tier 2 candidates (similar matches)
            var_diffs = match.get('var_diffs')
            if var_diffs:
                diff_display = self._format_variation_differences(var_diffs, has_draw_odds)
                message += f"      Differences: {diff_display}\n"
            
            # DEBUG: Log candidate info
            candidate_event_id = match.get('event_id')
            candidate_sport = match.get('sport')
            logger.info(f"🔍 DEBUG: Processing candidate {i} - event_id={candidate_event_id}, sport='{candidate_sport}'")
            
            from sport_observations import sport_observations_manager
            sport_info = sport_observations_manager.format_sport_info_for_candidates(candidate_event_id, candidate_sport)
            logger.info(f"🔍 DEBUG: Sport info result for candidate {i}: '{sport_info}'")
            
            if sport_info:
                message += f"      {sport_info}\n"
                logger.info(f"🔍 DEBUG: Added sport info to message for candidate {i}")
            else:
                logger.info(f"🔍 DEBUG: No sport info to add for candidate {i}")
        
        return message + "\n"
    
    def _format_variations_display(self, variations: Dict, has_draw_odds: bool) -> str:
        """Format variations display based on sport type"""
        var_one = variations.get('var_one', 'N/A')
        var_x = variations.get('var_x')
        var_two = variations.get('var_two', 'N/A')
        
        var_display = f"Δ1: {var_one}"
        
        if has_draw_odds:  # 3-way sport (Football, etc.)
            var_display += f", ΔX: {var_x:.2f}" if var_x is not None else ", ΔX: N/A"
        
        var_display += f", Δ2: {var_two}"
        return var_display
    
    def _format_variation_differences(self, var_diffs: Dict, has_draw_odds: bool) -> str:
        """Format variation differences display based on sport type"""
        d1_diff = var_diffs.get('d1', 0)
        d2_diff = var_diffs.get('d2', 0)
        dx_diff = var_diffs.get('dx')
        
        diff_display = f"Δ1: ±{d1_diff:.3f}"
        
        if has_draw_odds and dx_diff is not None:  # 3-way sport (Football, etc.)
            diff_display += f", ΔX: ±{dx_diff:.3f}"
        
        diff_display += f", Δ2: ±{d2_diff:.3f}"
        return diff_display
    
    def _format_rule_activations(self, rule_activations: Dict) -> str:
        """Format rule activations for display"""
        if not rule_activations:
            return ""
        
        message = "📋 **Rule Activations:**\n"
        
        # Define rule descriptions
        rule_descriptions = {
            'A': 'Identical Results',
            'B': 'Similar Results', 
            'C': 'Same Winning Side'
        }
        
        for tier, activation in rule_activations.items():
            count = activation['count']
            weight = activation['weight']
            description = rule_descriptions.get(tier, f'Tier {tier}')
            
            message += f"   • Tier {tier} ({description}): {count} candidates (weight: {weight})\n"
            
            # Show which candidates activated this rule
            for candidate in activation['candidates']:
                message += f"     - {candidate['participants']} → {candidate['result_text']}\n"
        
        message += "\n"
        return message
    

# Global notification instance
pre_start_notifier = PreStartNotification()

# PROCESS 1 END BOUNDARY
# ======================
# Process 1 notification system ends here.
# Process 2 notifications will be implemented with clear boundaries.
