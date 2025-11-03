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
from datetime import datetime
import logging
from typing import Dict, List, Optional
import requests
from repository import EventRepository

logger = logging.getLogger(__name__)

class PreStartNotification:
    """Notification system for upcoming games starting within 30 minutes - Telegram only"""
    
    def __init__(self):
        self.notification_enabled = True
        self.telegram_enabled = False
        
        # Load notification settings from config
        self._load_notification_settings()
    
    def _format_game_date(self, timestamp: int) -> str:
        """Format timestamp to date string (MM/DD format)"""
        if timestamp == 0:
            return ""
        try:
            from datetime import datetime
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime("%m/%d/%Y")
        except Exception:
            return ""
    
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
        
        
        status = report_data.get('status', 'unknown')
        
        primary_prediction = report_data.get('primary_prediction')
        
        
        odds_display = report_data.get('odds_display', 'Not available')
        vars_display = report_data.get('vars_display', 'Not available')
        has_draw_odds = report_data.get('has_draw_odds', False)
        confidence = report_data.get('primary_confidence', 'Not available')
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
        # Current event data
        message += f"📈 Current Variations:\n"
        message += f"{vars_display}\n\n"
        
        message += f"💰 Current Odds:\n"
        message += f"{odds_display}\n\n"
        
        # Candidate summary with new logic
        total_candidates_found = tier1_count + tier2_count
        non_symmetrical_count = report_data.get('non_symmetrical_candidates', 0)
        
        message += f"🔍Summary:\n"
        message += f"T1 (exact): {tier1_count}\n"
        message += f"T2 (similar): {tier2_count}\n"
        message += f"Confidence: {confidence}\n"
        if non_symmetrical_count > 0:
            message += f" ({non_symmetrical_count} non-symmetrical filtered out)"
        message += f"\n"
        
        
        
        
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
                    symmetry_status = " ❌"
            competition_parts = match.get('competition', 'Unknown').split(',')
            competition = competition_parts[-1].strip() if competition_parts else 'Unknown'
            message += f"\n{i}. {match['participants']} ({competition}):\n"
            message += f"R: {match['result_text']}{symmetry_status}\n"  
            message += f"Open: {match['one_open']}, {match['x_open']}, {match['two_open']}\n"
            message += f"Final: {match['one_final']}, {match['x_final']}, {match['two_final']}\n"
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

            if sport_info:
                message += f"{sport_info}\n"
        
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
            
            message += f"Tier {tier} ({description}): {count} candidates (weight: {weight})\n"
            
            # Show which candidates activated this rule
            for candidate in activation['candidates']:
                message += f" - {candidate['participants']} → {candidate['result_text']}\n"
        
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
            message = f"{header}\n"
            
            # Event information
            message += f"🏆 {dual_report.event_id} {dual_report.participants}\n"
            message += f"🔍 {dual_report.discovery_source.title().replace('_', ' ')}\n"
            # Get competition from process1_report if available
            competition = "Unknown"
            if dual_report.process1_report:
                competition = dual_report.process1_report.get('competition', 'Unknown')
            
            # Add sport with emoji and competition
            if dual_report.sport == 'Football':
                message += f"⚽({competition})"
            elif dual_report.sport == 'Basketball':
                message += f"🏀({competition})"
            elif dual_report.sport == 'Tennis':
                message += f"🎾({competition})"
            elif dual_report.sport == 'Hockey':
                message += f"🏒({competition})"
            elif dual_report.sport == 'Baseball':
                message += f"⚾({competition})"
            elif dual_report.sport == 'Handball':
                message += f"🤼 {dual_report.sport} ({competition})"
            elif dual_report.sport == 'Rugby':
                message += f"🏉({competition})"
            elif dual_report.sport == 'American Football':
                message += f"🏈({competition})"
            elif dual_report.sport == 'Volleyball':
                message += f"🏐({competition})"
            else:
                message += f"🏟️ {dual_report.sport} ({competition})"
            
            if dual_report.minutes_until_start is not None:
                message += f"\n🕒 {dual_report.minutes_until_start} min."
            
            # Add court type observation for Tennis/Tennis Doubles events
            if dual_report.sport in ['Tennis', 'Tennis Doubles'] and dual_report.court_type:
                message += f"\n📢Obs: {dual_report.court_type}"
            
            message += "\n"
            
            # Process 1 Results - COMPLETE REPORT (reusing existing format)
            message += f"\n"
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
                    message += f" ✅ Prediction: {winner_text}\n"
                else:
                    message += f" ✅ Prediction: {winner_text} wins\n"
                
                # Show Process 2 detailed information
                if dual_report.process2_report:
                    # Show variables calculated
                    variables = dual_report.process2_report.get('variables_calculated', {})
                    if variables:
                        message += f" 📊 Variables: β={variables.get('β', 0):.3f}, ζ={variables.get('ζ', 0):.3f}, γ={variables.get('γ', 0):.3f}\n"
                        message += f" 📊 Variables: δ={variables.get('δ', 0):.3f}, ε={variables.get('ε', 0):.3f}\n"
                    
                    # Show activated formulas
                    if 'activated_formulas' in dual_report.process2_report:
                        formulas = dual_report.process2_report['activated_formulas']
                        message += f" 📋 Formulas activated: {len(formulas)}\n"
                        
                        # Show all activated formulas (not just first 3)
                        for formula in formulas:
                            formula_name = formula.get('formula_name', 'Unknown')
                            winner_side = formula.get('winner_side', '?')
                            point_diff = formula.get('point_diff', 0)
                            
                            # Clean up formula name for display
                            clean_name = formula_name.replace('formula_', '').replace('_', ' ').title()
                            winner_text = {'1': 'Home', 'X': 'Draw', '2': 'Away'}.get(winner_side, winner_side)
                            message += f"{clean_name}: {winner_text} wins (diff: {point_diff})\n"
                    
                    # Show total formulas checked
                    total_formulas = dual_report.process2_report.get('total_formulas_checked', 0)
                    activated_count = dual_report.process2_report.get('formulas_activated_count', 0)
                    message += f"🧮 Formulas checked: {activated_count}/{total_formulas}\n"
            else:
                message += f"❌ No prediction ({dual_report.process2_status})\n"
            
            if dual_report.final_prediction:
                final_winner = dual_report.final_prediction[0]
                winner_text = {'1': 'Home', 'X': 'Draw', '2': 'Away'}.get(final_winner, final_winner)
                if winner_text == 'Draw':
                    message += f"🏆 Final Prediction: {winner_text}\n"
                else:
                    message += f"🏆 Final Prediction: {winner_text} wins\n"
            
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
    

    def create_h2h_streak_message(self, streak) -> str:
        """
        Create H2H streak alert message for Telegram.
        
        Args:
            streak: H2HStreak object with analysis results
            
        Returns:
            Formatted message string for Telegram
        """
        try:
            message = f"📊 <b>{streak.discovery_source.title().replace('_', ' ')} Streak Alert</b>\n"
            message += f"🏆 <b>{streak.event_id} {streak.participants}</b>\n"
            if streak.sport == 'Football':
                message += f"⚽ "
            elif streak.sport == 'Basketball':
                message += f"🏀 "
            elif streak.sport == 'Tennis':
                message += f"⚜️ H~{streak.home_team_ranking} vs A~{streak.away_team_ranking}\n"
                message += f"🎾 "
            elif streak.sport == 'Hockey':
                message += f"🏒 "
            elif streak.sport == 'Baseball':
                message += f"⚾ "
            elif streak.sport == 'Handball':
                message += f"🤼 {streak.sport})"
            elif streak.sport == 'Rugby':
                message += f"🏉 "
            elif streak.sport == 'American Football':
                message += f"🏈 "
            elif streak.sport == 'Volleyball':
                message += f"🏐 "
            else:
                message += f"🏟️ {streak.sport}"
            
            message += f"({streak.competition_name})\n"
            message += f"⏰ {streak.minutes_until_start} minutes\n\n"
            
            message += f"📈 H2H (Last 2 Years):\n"
            
            # Total matches only
            message += f"Total Matches: {streak.matches_analyzed}\n"
            
            # Group matches by winner to show results organized by team
            if hasattr(streak, 'all_matches') and streak.all_matches:
                # Extract results to count wins per team
                all_results = [match.get('winner', '?') for match in streak.all_matches]
                
                # Show home team wins section
                if streak.home_wins > 0:
                    # Compute per-team net points by role for upcoming home team
                    home_team_home_net = 0
                    home_team_away_net = 0
                    for m in streak.all_matches:
                        if m.get('winner') == '1':
                            hist_home = m.get('hist_home')
                            hs = m.get('hist_home_score', 0)
                            as_ = m.get('hist_away_score', 0)
                            if hist_home == streak.home_team_name:
                                home_team_home_net += (hs - as_)
                            else:
                                home_team_away_net += (as_ - hs)
                    home_net_str = f"+{home_team_home_net}" if home_team_home_net >= 0 else str(home_team_home_net)
                    away_net_str = f"+{home_team_away_net}" if home_team_away_net >= 0 else str(home_team_away_net)
                    message += f"\n{streak.home_team_name}: {streak.home_wins} wins ({streak.home_win_rate}%) [H:{home_net_str}, A:{away_net_str}]\n"
                    match_num = 1
                    for match in streak.all_matches:
                        if match.get('winner') == '1':
                            hist_home = match.get('hist_home', 'Unknown')
                            hist_away = match.get('hist_away', 'Unknown')
                            hist_home_score = match.get('hist_home_score', 0)
                            hist_away_score = match.get('hist_away_score', 0)
                            match_timestamp = match.get('startTimestamp', 0)
                            match_date = self._format_game_date(match_timestamp)
                            date_prefix = f"{match_date} " if match_date else ""
                            message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away}\n"
                            match_num += 1
                
                # Show away team wins section
                if streak.away_wins > 0:
                    # Compute per-team net points by role for upcoming away team
                    away_team_home_net = 0
                    away_team_away_net = 0
                    for m in streak.all_matches:
                        if m.get('winner') == '2':
                            hist_home = m.get('hist_home')
                            hs = m.get('hist_home_score', 0)
                            as_ = m.get('hist_away_score', 0)
                            if hist_home == streak.away_team_name:
                                away_team_home_net += (hs - as_)
                            else:
                                away_team_away_net += (as_ - hs)
                    home_net_str = f"+{away_team_home_net}" if away_team_home_net >= 0 else str(away_team_home_net)
                    away_net_str = f"+{away_team_away_net}" if away_team_away_net >= 0 else str(away_team_away_net)
                    message += f"\n{streak.away_team_name}: {streak.away_wins} wins ({streak.away_win_rate}%) [H:{home_net_str}, A:{away_net_str}]\n"
                    for match in streak.all_matches:
                        if match.get('winner') == '2':
                            hist_home = match.get('hist_home', 'Unknown')
                            hist_away = match.get('hist_away', 'Unknown')
                            hist_home_score = match.get('hist_home_score', 0)
                            hist_away_score = match.get('hist_away_score', 0)
                            match_timestamp = match.get('startTimestamp', 0)
                            match_date = self._format_game_date(match_timestamp)
                            date_prefix = f"{match_date} " if match_date else ""
                            message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away}\n"
                
                # Show draws section (if any)
                if streak.draws > 0:
                    message += f"\nDraws: {streak.draws} ({streak.draw_rate}%)\n"
                    for match in streak.all_matches:
                        if match.get('winner') == 'X':
                            hist_home = match.get('hist_home', 'Unknown')
                            hist_away = match.get('hist_away', 'Unknown')
                            hist_home_score = match.get('hist_home_score', 0)
                            hist_away_score = match.get('hist_away_score', 0)
                            match_timestamp = match.get('startTimestamp', 0)
                            match_date = self._format_game_date(match_timestamp)
                            date_prefix = f"{match_date} " if match_date else ""
                            message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away}\n"
            else:
                # No matches available, show summary only
                message += f"{streak.home_team_name}: {streak.home_wins} wins ({streak.home_win_rate}%)\n"
                message += f"{streak.away_team_name}: {streak.away_wins} wins ({streak.away_win_rate}%)\n"
                if streak.draws > 0:
                    message += f"Draws: {streak.draws} ({streak.draw_rate}%)\n"
            
            message += "\n"
            
            
            # NEW: Team Results Section - Overall Form + Batched Display
            if hasattr(streak, 'home_team_wins') and hasattr(streak, 'away_team_wins'):
                message += f"📊 Last 10 Games:\n"
                message += f"{streak.home_team_name}: {streak.home_team_wins}W-{streak.home_team_losses}L-{streak.home_team_draws}D\n"
                message += f"{streak.away_team_name}: {streak.away_team_wins}W-{streak.away_team_losses}L-{streak.away_team_draws}D\n\n"
                
                # Display batched form for historical analysis
                if hasattr(streak, 'home_team_batches') and hasattr(streak, 'away_team_batches'):
                    message += f"📈 Historical Form:\n"
                    
                    if streak.sport == 'Tennis' or streak.sport == 'Tennis Doubles':
                        if streak.home_team_batches:
                            message += f"<b>{streak.home_team_name}</b>:\n"
                            for i, batch in enumerate(streak.home_team_batches):
                                # Calculate game count for this batch (5, 10, 15, etc.)
                                game_count = (i + 1) * 5
                                batch_summary = f"{game_count}: {batch['batch_wins']}W-{batch['batch_losses']}L-{batch['batch_draws']}D"
                                if batch['batch_net_points'] > 0:
                                    batch_summary += f"(+{batch['batch_net_points']})"
                                elif batch['batch_net_points'] < 0:
                                    batch_summary += f"({batch['batch_net_points']})"
                                else:
                                    batch_summary += " (0)"
                                
                                # Add net points by role
                                home_net = batch.get('batch_home_net_points', 0)
                                away_net = batch.get('batch_away_net_points', 0)
                                # Format with proper sign
                                home_net_str = f"+{home_net}" if home_net >= 0 else str(home_net)
                                away_net_str = f"+{away_net}" if away_net >= 0 else str(away_net)
                                batch_summary += f" [H:{home_net_str}, A:{away_net_str}]"
                                
                                # Add net ranking differential
                                net_ranking_diff = batch.get('batch_net_ranking_differential', 0)
                                if net_ranking_diff != 0:
                                    ranking_diff_str = f"+{net_ranking_diff}" if net_ranking_diff >= 0 else str(net_ranking_diff)
                                    batch_summary += f" [~{ranking_diff_str}]"
                                
                                message += f"{batch_summary}\n"
                                
                                # Show individual games in this batch
                                for game in batch['games']:
                                    game_date = self._format_game_date(game.get('startTimestamp', 0))
                                    date_prefix = f"{game_date} " if game_date else ""
                                    message += f"{date_prefix} ~{game['own_ranking']} {game['result']} vs ~{game['opponent_ranking']} {game['opponent']} ({game['score_for']}-{game['score_against']})\n"
                                
                                # Add break line between batches (except for the last batch)
                                if i < len(streak.home_team_batches) - 1:
                                    message += "\n"
                        else:
                            message += f"<b>{streak.home_team_name}</b>: No recent form data\n"
                    else:
                        # Display home team batches
                        if streak.home_team_batches:
                            message += f"<b>{streak.home_team_name}</b>:\n"
                            for i, batch in enumerate(streak.home_team_batches):
                                # Calculate game count for this batch (5, 10, 15, etc.)
                                game_count = (i + 1) * 5
                                batch_summary = f"{game_count}: {batch['batch_wins']}W-{batch['batch_losses']}L-{batch['batch_draws']}D"
                                if batch['batch_net_points'] > 0:
                                    batch_summary += f"(+{batch['batch_net_points']})"
                                elif batch['batch_net_points'] < 0:
                                    batch_summary += f"({batch['batch_net_points']})"
                                else:
                                    batch_summary += " (0)"
                                
                                # Add net points by role
                                home_net = batch.get('batch_home_net_points', 0)
                                away_net = batch.get('batch_away_net_points', 0)
                                # Format with proper sign
                                home_net_str = f"+{home_net}" if home_net >= 0 else str(home_net)
                                away_net_str = f"+{away_net}" if away_net >= 0 else str(away_net)
                                batch_summary += f" [H:{home_net_str}, A:{away_net_str}]"
                                
                                message += f"{batch_summary}\n"
                                
                                # Show individual games in this batch
                                for game in batch['games']:
                                    game_date = self._format_game_date(game.get('startTimestamp', 0))
                                    date_prefix = f"{game_date} " if game_date else ""
                                    role_indicator = "🏠" if game.get('role') == 'home' else "✈️"
                                    message += f"{role_indicator}{date_prefix}{game['result']} vs {game['opponent']} ({game['score_for']}-{game['score_against']})\n"
                                
                                # Add break line between batches (except for the last batch)
                                if i < len(streak.home_team_batches) - 1:
                                    message += "\n"
                        else:
                            message += f"<b>{streak.home_team_name}</b>: No recent form data\n"
                    
                    message += "\n"
                    
                    # Display away team batches
                    if streak.away_team_batches:
                        message += f"<b>{streak.away_team_name}</b>:\n"
                        for i, batch in enumerate(streak.away_team_batches):
                            # Calculate game count for this batch (5, 10, 15, etc.)
                            game_count = (i + 1) * 5
                            batch_summary = f"{game_count}: {batch['batch_wins']}W-{batch['batch_losses']}L-{batch['batch_draws']}D"
                            if batch['batch_net_points'] > 0:
                                batch_summary += f"(+{batch['batch_net_points']})"
                            elif batch['batch_net_points'] < 0:
                                batch_summary += f"({batch['batch_net_points']})"
                            else:
                                batch_summary += " (0)"
                            
                            # Add net points by role
                            home_net = batch.get('batch_home_net_points', 0)
                            away_net = batch.get('batch_away_net_points', 0)
                            # Format with proper sign
                            home_net_str = f"+{home_net}" if home_net >= 0 else str(home_net)
                            away_net_str = f"+{away_net}" if away_net >= 0 else str(away_net)
                            batch_summary += f" [H:{home_net_str}, A:{away_net_str}]"
                            
                            # Add net ranking differential (only for Tennis/Tennis Doubles)
                            if streak.sport == 'Tennis' or streak.sport == 'Tennis Doubles':
                                net_ranking_diff = batch.get('batch_net_ranking_differential', 0)
                                if net_ranking_diff != 0:
                                    ranking_diff_str = f"+{net_ranking_diff}" if net_ranking_diff >= 0 else str(net_ranking_diff)
                                    batch_summary += f" [~{ranking_diff_str}]"
                            
                            message += f"{batch_summary}\n"
                            
                            # Show individual games in this batch
                            for game in batch['games']:
                                game_date = self._format_game_date(game.get('startTimestamp', 0))
                                date_prefix = f"{game_date} " if game_date else ""
                                if streak.sport == 'Tennis' or streak.sport == 'Tennis Doubles':
                                    message += f"{date_prefix} ~{game['own_ranking']} {game['result']} vs ~{game['opponent_ranking']} {game['opponent']} ({game['score_for']}-{game['score_against']})\n"
                                else:
                                    role_indicator = "🏠" if game.get('role') == 'home' else "✈️"
                                    message += f"{role_indicator}{date_prefix}{game['result']} vs {game['opponent']} ({game['score_for']}-{game['score_against']})\n"
                            
                            # Add break line between batches (except for the last batch)
                            if i < len(streak.away_team_batches) - 1:
                                message += "\n"
                    else:
                        message += f"<b>{streak.away_team_name}</b>: No recent form data\n"
                    
                    message += "\n"
            
            # NEW: Winning Odds Section
            if hasattr(streak, 'winning_odds_data') and streak.winning_odds_data:
                # Check if we have any valid odds data
                has_home_odds = 'home' in streak.winning_odds_data and streak.winning_odds_data['home'] is not None
                has_away_odds = 'away' in streak.winning_odds_data and streak.winning_odds_data['away'] is not None
                
                if has_home_odds or has_away_odds:
                    message += f"🎯 Winning Odds:\n"
                    
                    # Home team odds
                    if has_home_odds:
                        home_odds = streak.winning_odds_data['home']
                        home_decimal = home_odds.get('decimalValue', 0)
                        home_expected = home_odds.get('expected', 0)
                        home_actual = home_odds.get('actual', 0)
                        
                        message += f"<b>{streak.home_team_name}</b>\n"
                        message += f"📊 Odds: {home_decimal} (Expected: {home_expected}%, Actual: {home_actual}%)\n"
                        if home_actual > home_expected:
                            message += f"✅⬆️ {home_actual - home_expected}%\n"
                        elif home_actual < home_expected:
                            message += f"⚠️⬇️ {home_expected - home_actual}%\n"
                        else:
                            message += f"⚖️ Meeting expectations\n"
                    else:
                        message += f"{streak.home_team_name}: No odds data available\n"
                    
                    # Away team odds
                    if has_away_odds:
                        away_odds = streak.winning_odds_data['away']
                        away_decimal = away_odds.get('decimalValue', 0)
                        away_expected = away_odds.get('expected', 0)
                        away_actual = away_odds.get('actual', 0)
                        
                        message += f"<b>{streak.away_team_name}</b>\n"
                        message += f"📊 Odds: {away_decimal} (Expected: {away_expected}%, Actual: {away_actual}%)\n"
                        if away_actual > away_expected:
                            message += f"✅⬆️ {away_actual - away_expected}%\n"
                        elif away_actual < away_expected:
                            message += f"⚠️⬇️ {away_expected - away_actual}%\n"
                        else:
                            message += f"⚖️ Meeting expectations\n"
                    else:
                        message += f"<b>{streak.away_team_name}</b>: <i>No odds data available</i>\n"
                    
                    message += "\n"
            
            
            return message
            
        except Exception as e:
            logger.error(f"Error creating H2H streak message: {e}")
            return f"❌ Error creating H2H streak message for event {streak.event_id}: {str(e)}"
    
    def send_h2h_streak_alerts(self, streak_reports: List) -> bool:
        """
        Send H2H streak alerts via Telegram.
        
        Args:
            streak_reports: List of H2HStreak objects
            
        Returns:
            True if at least one alert was sent successfully
        """
        if not streak_reports:
            return True
            
        success_count = 0
        
        for streak in streak_reports:
            try:
                # Create message for streak report
                message = self.create_h2h_streak_message(streak)
                
                # Send via Telegram
                sent = self.send_telegram_message(message)
                
                if sent:
                    success_count += 1
                    logger.info(f"✅ H2H streak alert sent for event {streak.event_id}")
                else:
                    logger.warning(f"❌ Failed to send H2H streak alert for event {streak.event_id}")
                    
            except Exception as e:
                logger.error(f"Error sending H2H streak alert for event {streak.event_id}: {e}")
                continue
        
        logger.info(f"Sent {success_count}/{len(streak_reports)} H2H streak alerts successfully")
        return success_count > 0
    
    def send_time_correction_message(self, event_id: int, current_starting_time: datetime, new_starting_time: datetime) -> bool:
        """
        Send a time correction message via Telegram.
        
        Args:
            event_id: Event ID
            current_starting_time: Original starting time
            new_starting_time: Updated starting time

        Returns:
            True if the message was sent successfully
        """
        try:
            # Fetch event details to get participants
            
            event = EventRepository.get_event_by_id(event_id)
            
            if not event:
                logger.warning(f"Could not find event {event_id} for time correction message")
                participants = "Unknown vs Unknown"
            else:
                participants = f"{event.home_team} vs {event.away_team}"
            
            # Format times for display
            current_time_str = current_starting_time.strftime("%H:%M")
            new_time_str = new_starting_time.strftime("%H:%M")
            
            # Calculate time difference
            time_diff = new_starting_time - current_starting_time
            if time_diff.total_seconds() > 0:
                diff_str = f"+{int(time_diff.total_seconds() / 60)} min"
            else:
                diff_str = f"{int(time_diff.total_seconds() / 60)} min"
            
            # Create message
            message = f"🕐 <b>Time Correction Alert</b>\n\n"
            message += f"🏆 <b>{participants}</b>\n"
            message += f"📅 Event ID: {event_id}\n\n"
            message += f"⏰ <b>Time Change:</b>\n"
            message += f"Original: {current_time_str}\n"
            message += f"Updated: {new_time_str}\n"
            message += f"Difference: {diff_str}\n\n"
            message += f"🔄 <i>Starting time corrected during late timestamp check</i>"
            
            if not self.telegram_enabled:
                logger.warning("Telegram notifications not configured - cannot send time correction message")
                return False
            
            logger.info(f"Sending time correction message for event {event_id}: {participants}")
            return self.send_telegram_message(message)
            
        except Exception as e:
            logger.error(f"Error creating time correction message for event {event_id}: {e}")
            # Fallback message without participants
            message = f"🕐 Time correction message for event {event_id}\n\n"
            message += f"Current starting time: {current_starting_time}\n"
            message += f"New starting time: {new_starting_time}\n"
            return self.send_telegram_message(message)

# Global notification instance
pre_start_notifier = PreStartNotification()

