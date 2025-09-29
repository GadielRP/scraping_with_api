"""
Alert System - Telegram notification system for Process 1 and Dual Process

PROCESS 1 BOUNDARIES:
====================
START: This file contains Process 1 notification system
END: Process 1 notification system ends at the end of this file

PROCESS 1 INTEGRATION:
This file handles Telegram notifications for Process 1 candidate reports.
It formats and sends Process 1 results (SUCCESS/NO MATCH/NO CANDIDATES).

DUAL PROCESS INTEGRATION:
This file now handles Telegram notifications for Dual Process reports.
It formats and sends combined Process 1 + Process 2 results with comparison.

PROCESS 2 INTEGRATION:
Process 2 notifications are integrated within Dual Process notifications.
Process 2 boundaries are handled by separate files with clear boundaries.
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
        self.telegram_test_only = os.getenv('TEST_ONLY_MODE', 'false').lower() == 'true'
        self.personal_chat_id = os.getenv('PERSONAL_CHAT_ID', '')
        
        logger.info(f"Telegram notification: {'âœ… Enabled' if self.telegram_enabled else 'âŒ Disabled'}")
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
            
            if self.telegram_test_only:
                data['chat_id'] = self.personal_chat_id
                
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
            'success': "✅ PROCESS 1 - SUCCESS",
            'partial': "⚠️ PROCESS 1 - PARTIAL",
            'no_match': "❌ PROCESS 1 - NO MATCH",
            'no_candidates': "❓ PROCESS 1 - NO  VALID CANDIDATES"
        }
        header = status_headers.get(status, "❓ PROCESS 1 - UNKNOWN STATUS")
        
        message = f"{header}\n\n"
        
        # Event information
        message += f"🏆 {report_data.get('event_id', 'Unknown')} {participants}\n"
                
        # Current event data
        message += f"📈 Current Variations:\n"
        message += f"{vars_display}\n\n"
        
        message += f"💰 Current Odds:\n"
        message += f"{odds_display}\n\n"
        
        # Candidate summary with new logic
        total_candidates_found = tier1_count + tier2_count
        non_symmetrical_count = report_data.get('non_symmetrical_candidates', 0)
        
        message += f"🔍 Candidate Summary:\n"
        message += f"• Tier 1 (exact): {tier1_count} candidates\n"
        message += f"• Tier 2 (similar): {tier2_count} candidates"
        if non_symmetrical_count > 0:
            message += f" ({non_symmetrical_count} non-symmetrical filtered out)"
        message += f"\n"
        message += f"• Selected tier: {selected_tier}\n"
        message += f"• Successful: {successful_candidates}/{total_candidates} candidates\n"
        message += f"• Confidence: {primary_confidence}\n\n"
        
        # Add rule activations summary
        rule_activations = report_data.get('rule_activations', {})
        if rule_activations:
            message += self._format_rule_activations(rule_activations)
        
        # Show candidates from all available tiers
        if tier1_count > 0:
            message += self._format_tier_candidates("🎯", "Exact Vars", 
                                                 tier1_count, tier1_data.get('matches', []), has_draw_odds)
        if tier2_count > 0:
            # Always show ALL Tier 2 candidates with symmetrical status indicators
            message += self._format_tier_candidates("📊", "Similar Vars", 
                                                 tier2_count, tier2_data.get('matches', []), has_draw_odds)
        
        # Primary prediction
        if primary_prediction:
            message += f"🎯: {primary_prediction}\n"
        elif status == 'partial':
            message += f"⚠️ Need at least 2 candidates\n"
        else:
            message += f"❌ No Prediction\n"
        
        return message
    
    def _format_tier_candidates(self, icon: str, title: str, count: int, 
                              matches: List[Dict], has_draw_odds: bool) -> str:
        """Format tier candidates for display"""
        message = f"\n{icon} {title} ({count}):\n"
            
        for i, match in enumerate(matches, 1):
            var_display = self._format_variations_display(match.get('variations', {}), has_draw_odds)
            symmetry_status = ""
            if 'is_symmetrical' in match:
                if match['is_symmetrical']:
                    symmetry_status = " ✅"
                else:
                    symmetry_status = " ❌ (unsymmetrical)"
            
            message += f"\n{i}. {match['participants']} ({match.get('competition', 'Unknown')}):\n"
            message += f"R: {match['result_text']}{symmetry_status}\n"  
            message += f"Δ: {var_display}\n"
            
            # Add variation differences for Tier 2 candidates (similar matches)
            var_diffs = match.get('var_diffs')
            if var_diffs:
                diff_display = self._format_variation_differences(var_diffs, has_draw_odds)
                message += f"Diff: {diff_display}\n"
                message += f"L1: {match.get('distance_l1', 'N/A')}\n"
            
            # DEBUG: Log candidate info
            candidate_event_id = match.get('event_id')
            candidate_sport = match.get('sport')
            logger.info(f"🔍 DEBUG: Processing candidate {i} - event_id={candidate_event_id}, sport='{candidate_sport}'")
            
            from sport_observations import sport_observations_manager
            sport_info = sport_observations_manager.format_sport_info_for_candidates(candidate_event_id, candidate_sport)
            logger.info(f"🔍 DEBUG: Sport info result for candidate {i}: '{sport_info}'")
            
            if sport_info:
                message += f"{sport_info}\n"
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
        
        # Show the actual sign (positive/negative) instead of ±
        diff_display = f"Δ1: {d1_diff:+.3f}"
        
        if has_draw_odds and dx_diff is not None:  # 3-way sport (Football, etc.)
            diff_display += f", ΔX: {dx_diff:+.3f}"
        
        diff_display += f", Δ2: {d2_diff:+.3f}"
        return diff_display
    
    def _format_rule_activations(self, rule_activations: Dict) -> str:
        """Format rule activations for display"""
        if not rule_activations:
            return ""
        
        message = "📋 Rule Activations:\n"
        
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
    
    def create_dual_process_message(self, dual_report) -> str:
        """
        Create enhanced Telegram message for dual process report with full debug information.
        Integrates Process 1 candidate report with Process 2 formula results.
        
        Args:
            dual_report: DualProcessReport object
            
        Returns:
            Formatted message string for Telegram
        """
        try:
            # Determine header based on verdict
            verdict_headers = {
                'AGREE': "✅ DUAL PROCESS - AGREEMENT",
                'DISAGREE': "⚔️ DUAL PROCESS - DISAGREEMENT", 
                'PARTIAL': "⚠️ DUAL PROCESS - PARTIAL RESULT",
                'ERROR': "❌ DUAL PROCESS - ERROR"
            }
            
            header = verdict_headers.get(dual_report.verdict.value, "❓ DUAL PROCESS - UNKNOWN")
            
            message = f"{header}\n\n"
            
            # Event information
            message += f"🏆 {dual_report.participants}\n"
            
            # Get competition from process1_report if available
            competition = "Unknown"
            if dual_report.process1_report:
                competition = dual_report.process1_report.get('competition', 'Unknown')
            
            # Add sport with emoji and competition
            if dual_report.sport == 'Football':
                message += f"⚽ {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Basketball':
                message += f"🏀 {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Tennis':
                message += f"🎾 {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Hockey':
                message += f"🏒 {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Baseball':
                message += f"⚾ {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Handball':
                message += f"🤼 {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Rugby':
                message += f"🏉 {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Volleyball':
                message += f"🏐 {dual_report.sport} ({competition})"
            else:
                message += f"🏟️ {dual_report.sport} ({competition})"
            
            if dual_report.minutes_until_start is not None:
                message += f"\n🕒 Game starting in {dual_report.minutes_until_start} minutes"
            message += "\n\n"
            
            # Process 1 Results - COMPLETE REPORT (reusing existing format)
            message += f"📊 Process 1 (Historical Patterns):\n\n"
            if dual_report.process1_report:
                # Reuse the complete Process 1 message format
                process1_message = self.create_candidate_report_message(dual_report.process1_report)
                
                # Extract the main content from Process 1 message (skip header)
                lines = process1_message.split('\n')
                start_index = 0
                process1_content = '\n'.join(lines[start_index:])
                
                # Add Process 1 content with proper indentation
                for line in process1_content.split('\n'):
                    if line.strip():  # Skip empty lines
                        message += f"{line}\n"
                    else:
                        message += "\n"
            else:
                message += f"❌ No Process 1 report available ({dual_report.process1_status})\n"
            
            # Process 2 Results - DETAILED DEBUG
            message += f"\n🧪 Process 2 (Sport Formulas):\n"
            if dual_report.process2_prediction:
                p2_winner = dual_report.process2_prediction[0]
                winner_text = {'1': 'Home', 'X': 'Draw', '2': 'Away'}.get(p2_winner, p2_winner)
                if winner_text == 'Draw':
                    message += f"   ✅ Prediction: {winner_text}\n"
                else:
                    message += f"   ✅ Prediction: {winner_text} wins\n"
                
                # Show Process 2 detailed information
                if dual_report.process2_report:
                    # Show variables calculated
                    variables = dual_report.process2_report.get('variables_calculated', {})
                    if variables:
                        message += f"   📊 Variables: β={variables.get('β', 0):.3f}, ζ={variables.get('ζ', 0):.3f}, γ={variables.get('γ', 0):.3f}\n"
                        message += f"   📊 Variables: δ={variables.get('δ', 0):.3f}, ε={variables.get('ε', 0):.3f}\n"
                    
                    # Show activated formulas
                    if 'activated_formulas' in dual_report.process2_report:
                        formulas = dual_report.process2_report['activated_formulas']
                        message += f"   📋 Formulas activated: {len(formulas)}\n"
                        
                        # Show all activated formulas (not just first 3)
                        for formula in formulas:
                            formula_name = formula.get('formula_name', 'Unknown')
                            winner_side = formula.get('winner_side', '?')
                            point_diff = formula.get('point_diff', 0)
                            
                            # Clean up formula name for display
                            clean_name = formula_name.replace('formula_', '').replace('_', ' ').title()
                            winner_text = {'1': 'Home', 'X': 'Draw', '2': 'Away'}.get(winner_side, winner_side)
                            message += f"     • {clean_name}: {winner_text} wins (diff: {point_diff})\n"
                    
                    # Show total formulas checked
                    total_formulas = dual_report.process2_report.get('total_formulas_checked', 0)
                    activated_count = dual_report.process2_report.get('formulas_activated_count', 0)
                    message += f"   🧮 Formulas checked: {activated_count}/{total_formulas}\n"
            else:
                message += f"   ❌ No prediction ({dual_report.process2_status})\n"
            
            # Final Verdict - ENHANCED
            message += f"\n🎯 Final Verdict: {dual_report.verdict.value}\n"
            message += f"📝 {dual_report.agreement_details}\n"
            
            if dual_report.final_prediction:
                final_winner = dual_report.final_prediction[0]
                winner_text = {'1': 'Home', 'X': 'Draw', '2': 'Away'}.get(final_winner, final_winner)
                if winner_text == 'Draw':
                    message += f"🏆 Final Prediction: {winner_text}\n"
                else:
                    message += f"🏆 Final Prediction: {winner_text} wins\n"
            
            # Debug summary
            message += f"\n🔍 Debug Summary:\n"
            message += f"   Process 1 Status: {dual_report.process1_status}\n"
            message += f"   Process 2 Status: {dual_report.process2_status}\n"
            message += f"   Comparison: {dual_report.verdict.value}\n"
            
            message += f"\n⏰ Generated at {dual_report.timestamp}"
            
            return message
            
        except Exception as e:
            logger.error(f"Error creating dual process message: {e}")
            return f"❌ Error creating dual process message for event {dual_report.event_id}: {str(e)}"
    
    def send_dual_process_alerts(self, dual_reports: List) -> bool:
        """
        Send dual process alerts via Telegram.
        
        Args:
            dual_reports: List of DualProcessReport objects
            
        Returns:
            True if at least one alert was sent successfully
        """
        if not dual_reports:
            return True
            
        success_count = 0
        
        for dual_report in dual_reports:
            try:
                # Create enhanced message for dual process report
                message = self.create_dual_process_message(dual_report)
                
                # Send via Telegram
                sent = self.send_telegram_message(message)
                
                if sent:
                    success_count += 1
                    logger.info(f"✅ Dual process alert sent for event {dual_report.event_id}: {dual_report.verdict.value}")
                else:
                    logger.warning(f"❌ Failed to send dual process alert for event {dual_report.event_id}")
                    
            except Exception as e:
                logger.error(f"Error sending dual process alert for event {dual_report.event_id}: {e}")
                continue
        
        logger.info(f"Sent {success_count}/{len(dual_reports)} dual process alerts successfully")
        return success_count > 0

# Global notification instance
pre_start_notifier = PreStartNotification()

