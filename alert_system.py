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
    
    def _calculate_h2h_tennis_total_points(self, match: Dict, is_home: bool) -> int:
        """
        Calculate total tennis points from all periods for an H2H match.
        
        Args:
            match: Match dictionary with hist_home_periodX and hist_away_periodX data
            is_home: True to calculate for hist_home team, False for hist_away team
            
        Returns:
            Total points across all periods
        """
        prefix = 'hist_home' if is_home else 'hist_away'
        
        period1 = match.get(f'{prefix}_period1', 0) or 0
        period2 = match.get(f'{prefix}_period2', 0) or 0
        period3 = match.get(f'{prefix}_period3', 0) or 0  # May be 0 for 2-set matches
        
        return period1 + period2 + period3
    
    def _calculate_ranking_prediction(self, streak, home_total_games, away_total_games) -> Optional[Dict]:
        """
        Calculate ranking prediction based on final real rankings and historical form points.
        
        Args:
            streak: H2HStreak object with batches and final real rankings
            home_total_games: Total games for home team
            away_total_games: Total games for away team
        Returns:
            Dictionary with prediction data or None if insufficient data
        """
        # Check if we have both final real rankings
        home_ranking = streak.home_team_final_real_ranking
        away_ranking = streak.away_team_final_real_ranking
        

        if streak.sport not in ['Tennis', 'Tennis Doubles']:
            return None

        if home_ranking == 0 or away_ranking == 0:
            return None
        
        # Determine best (lowest) and worst (highest) rankings
        if home_ranking < away_ranking:
            best_ranking = home_ranking
            worst_ranking = away_ranking
            best_team_name = streak.home_team_name
            worst_team_name = streak.away_team_name
            best_batches = streak.home_team_batches
            worst_batches = streak.away_team_batches
            best_total_games = home_total_games
            worst_total_games = away_total_games
        else:
            best_ranking = away_ranking
            worst_ranking = home_ranking
            best_team_name = streak.away_team_name
            worst_team_name = streak.home_team_name
            best_batches = streak.away_team_batches
            worst_batches = streak.home_team_batches
            best_total_games = away_total_games
            worst_total_games = home_total_games
        # Calculate ranking advantage
        ranking_advantage = abs(best_ranking - worst_ranking)
        
        # Sum all [H:x, A:y] points from all batches for best team
        best_total_points = 0
        for batch in best_batches:
            home_net = batch.get('batch_home_net_points', 0)
            away_net = batch.get('batch_away_net_points', 0)
            best_total_points += (home_net + away_net)
        
        # Sum all [H:x, A:y] points from all batches for worst team
        worst_total_points = 0
        for batch in worst_batches:
            home_net = batch.get('batch_home_net_points', 0)
            away_net = batch.get('batch_away_net_points', 0)
            worst_total_points += (home_net + away_net)
        best_total_points_per_game = best_total_points / best_total_games if best_total_games > 0 else 0
        worst_total_points_per_game = worst_total_points / worst_total_games if worst_total_games > 0 else 0
        
        # Calculate prediction (simple difference in total points)
        prediction_diff = best_total_points - worst_total_points
        
        return {
            'ranking_advantage': ranking_advantage,
            'best_ranking': best_ranking,
            'worst_ranking': worst_ranking,
            'best_total_points_per_game': best_total_points_per_game,
            'worst_total_points_per_game': worst_total_points_per_game,
            'best_team_name': best_team_name,
            'worst_team_name': worst_team_name,
            'best_total_points': best_total_points,
            'worst_total_points': worst_total_points,
            'prediction_diff': prediction_diff,
            'best_total_games': best_total_games,
            'worst_total_games': worst_total_games
        }
    
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
    
    
    def _split_message(self, message: str, limit: int = 4000) -> List[str]:
        """
        Split a message into chunks of approximately `limit` characters.
        Respects line breaks and avoids splitting inside HTML tags.
        """
        if len(message) <= limit:
            return [message]

        chunks = []
        while message:
            if len(message) <= limit:
                chunks.append(message)
                break

            # Find the best place to split
            split_at = message.rfind('\n', 0, limit)
            if split_at == -1:
                split_at = limit

            # Ensure we don't split inside an HTML tag
            # Find the last '<' and '>' before split_at
            last_open = message.rfind('<', 0, split_at)
            last_close = message.rfind('>', 0, split_at)

            if last_open > last_close:
                # We are inside a tag, split before the tag starts
                split_at = last_open
            
            if split_at == 0:
                # Fallback: if we can't find a good split point, just take the limit
                split_at = limit

            chunks.append(message[:split_at].strip())
            message = message[split_at:].strip()

        return chunks

    def _send_telegram_notification(self, message: str) -> bool:
        """Send notification via Telegram bot, splitting if necessary"""
        if not message:
            return False

        try:
            chunks = self._split_message(message)
            all_success = True

            for chunk in chunks:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
                data = {
                    'chat_id': self.telegram_chat_id,
                    'text': chunk,
                    'parse_mode': 'HTML'
                }
                
                if self.telegram_test_only:
                    data['chat_id'] = self.personal_chat_id
                    
                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Telegram notification chunk sent successfully ({len(chunk)} chars)")
                else:
                    logger.error(f"Telegram notification failed: {response.status_code} - {response.text}")
                    all_success = False
            
            return all_success
                
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
        
        # Candidate summary (exact candidates only)
        message += f"🔍Summary:\n"
        message += f"Candidates (exact): {tier1_count}\n"
        message += f"Confidence: {confidence}\n"
        message += f"\n"
        
        
        
        
        # Add rule activations summary
        rule_activations = report_data.get('rule_activations', {})
        if rule_activations:
            message += self._format_rule_activations(rule_activations)
        
        # Show candidates (exact matches only)
        if tier1_count > 0:
            message += self._format_tier_candidates("🎯", "Exact Matches", 
                                                 tier1_count, tier1_data.get('matches', []), has_draw_odds)
        
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
            # All exact matches are symmetrical, no need to display symmetry status
            competition_parts = match.get('competition', 'Unknown').split(',')
            competition = competition_parts[-1].strip() if competition_parts else 'Unknown'
            message += f"\n{i}. {match['participants']} ({competition}):\n"
            message += f"R: {match['result_text']}\n"  
            message += f"Open: {match['one_open']}, {match['x_open']}, {match['two_open']}\n"
            message += f"Final: {match['one_final']}, {match['x_final']}, {match['two_final']}\n"
            message += f"Δ: {var_display}\n"
            
            # Variation differences only exist for similar matches (Tier 2), 
            # which are no longer used - this code path will never execute for exact matches
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
            
            if dual_report.minutes_until_start is not None and dual_report.minutes_until_start == 0:
                message += f"\n🕒 Event is startig now!"
            elif dual_report.minutes_until_start is not None:
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
            # Skip sending if Process 1 status is not success
            if dual_report.process1_status != 'success':
                logger.info(f"Skipping dual process alert for event {dual_report.event_id} because Process 1 status is not success ({dual_report.process1_status})")
                continue
                
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
            # Calculate total games for each team (from results count, not just W+L+D in case of data issues)
            away_total_games = len(streak.away_team_results) if hasattr(streak, 'away_team_results') else (streak.away_team_wins + streak.away_team_losses + streak.away_team_draws)
            home_total_games = len(streak.home_team_results) if hasattr(streak, 'home_team_results') else (streak.home_team_wins + streak.home_team_losses + streak.home_team_draws)
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
            if streak.minutes_until_start == 0:
                message += f"\n🕒 Event is startig now!"
            else:
                message += f"\n🕒 {streak.minutes_until_start} minutes\n"
            
            # Display current event odds if available (similar to Process 1 format)
            if streak.one_open is not None and streak.one_final is not None:
                odds_display = f"1: {streak.one_open}→{streak.one_final}"
                if streak.x_open is not None and streak.x_final is not None:
                    odds_display += f", X: {streak.x_open}→{streak.x_final}"
                odds_display += f", 2: {streak.two_open}→{streak.two_final}"
                message += f"💰 {odds_display}\n"
            elif streak.one_final is not None:
                # Fallback: show final odds only if open odds not available
                odds_display = f"1: {streak.one_final}"
                if streak.x_final is not None:
                    odds_display += f", X: {streak.x_final}"
                odds_display += f", 2: {streak.two_final}"
                message += f"💰 {odds_display}\n"
 
            # Display overall team win streaks (unfiltered) if available
            overall_streak_lines = []
            if getattr(streak, 'home_current_win_streak', 0):
                overall_streak_lines.append(f"{streak.home_team_name}: {streak.home_current_win_streak} consecutive wins")
            if getattr(streak, 'away_current_win_streak', 0):
                overall_streak_lines.append(f"{streak.away_team_name}: {streak.away_current_win_streak} consecutive wins")
            if overall_streak_lines:
                message += "\n🎯 General Win Streaks:\n"
                for line in overall_streak_lines:
                    message += f"{line}\n"

            # Standings snapshot if available
            home_standing = getattr(streak, 'home_team_standing', None)
            away_standing = getattr(streak, 'away_team_standing', None)
            if home_standing or away_standing:
                message += "\n🏆 Standings Snapshot:\n"

                def _format_standing_line(team_name: str, standing: Dict) -> str:
                    position = standing.get('position')
                    matches = standing.get('matches')
                    wins = standing.get('wins')
                    draws = standing.get('draws')
                    losses = standing.get('losses')
                    points = standing.get('points')
                    goal_diff_formatted = standing.get('goal_diff_formatted')
                    goal_diff = standing.get('goal_diff')

                    if goal_diff_formatted:
                        goal_diff_display = goal_diff_formatted
                    elif goal_diff is not None:
                        try:
                            goal_diff_display = f"{goal_diff:+d}"
                        except (TypeError, ValueError):
                            goal_diff_display = str(goal_diff)
                    else:
                        goal_diff_display = "N/A"

                    parts = []
                    parts.append(f"{team_name}: ")
                    parts.append(f"#{position}" if position is not None else "#N/A")
                    if points is not None:
                        parts.append(f", {points} pts")
                    record_parts = []
                    if wins is not None:
                        record_parts.append(f"{wins}W")
                    if draws is not None:
                        record_parts.append(f"{draws}D")
                    if losses is not None:
                        record_parts.append(f"{losses}L")
                    if record_parts:
                        parts.append(f" ({'-'.join(record_parts)})")
                    if matches is not None:
                        parts.append(f", {matches} played")
                    parts.append(f", GD {goal_diff_display}")
                    return "".join(parts)

                if home_standing:
                    message += f"{_format_standing_line(streak.home_team_name, home_standing)}\n"
                else:
                    message += f"{streak.home_team_name}: No standings data\n"

                if away_standing:
                    message += f"{_format_standing_line(streak.away_team_name, away_standing)}\n"
                else:
                    message += f"{streak.away_team_name}: No standings data\n"

            message += f"\n📈 H2H (Last 2 Years):\n"
            
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
                            
                            # For tennis, use total points from periods instead of sets
                            if streak.sport in ['Tennis', 'Tennis Doubles'] and 'hist_home_period1' in m:
                                hs = self._calculate_h2h_tennis_total_points(m, is_home=True)
                                as_ = self._calculate_h2h_tennis_total_points(m, is_home=False)
                            else:
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
                            hist_home_penalties = match.get('hist_home_penalties', 0)
                            hist_away_penalties = match.get('hist_away_penalties', 0)
                            match_timestamp = match.get('startTimestamp', 0)
                            match_date = self._format_game_date(match_timestamp)
                            date_prefix = f"{match_date} " if match_date else ""
                            if hist_home_penalties or hist_away_penalties:
                                message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away} (P:{hist_home_penalties}-{hist_away_penalties})\n"
                            else:
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
                            
                            # For tennis, use total points from periods instead of sets
                            if streak.sport in ['Tennis', 'Tennis Doubles'] and 'hist_home_period1' in m:
                                hs = self._calculate_h2h_tennis_total_points(m, is_home=True)
                                as_ = self._calculate_h2h_tennis_total_points(m, is_home=False)
                            else:
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
                
                            
                # Use dynamic labels based on actual game count
                home_label = f"Last {home_total_games} Games" if home_total_games != 10 else "Last 10 Games"
                away_label = f"Last {away_total_games} Games" if away_total_games != 10 else "Last 10 Games"
                
                message += f"📊 Season Form:\n"
                message += f"{streak.home_team_name}: {streak.home_team_wins}W-{streak.home_team_losses}L-{streak.home_team_draws}D ({home_total_games} games)\n"
                message += f"{streak.away_team_name}: {streak.away_team_wins}W-{streak.away_team_losses}L-{streak.away_team_draws}D ({away_total_games} games)\n\n"
                
                # Display batched form for historical analysis
                if hasattr(streak, 'home_team_batches') and hasattr(streak, 'away_team_batches'):
                    message += f"📈 Historical Form:\n"
                    
                    if streak.sport == 'Tennis' or streak.sport == 'Tennis Doubles':
                        if streak.home_team_batches:
                            final_ranking_str = f" (~{streak.home_team_final_real_ranking})" if streak.home_team_final_real_ranking > 0 else ""
                            message += f"<b>{streak.home_team_name}{final_ranking_str}</b>:\n"
                            # Initialize cumulative counters across all batches
                            cumulative_home_net = 0
                            cumulative_away_net = 0
                            
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
                                
                                # Add real ranking
                                real_ranking = batch.get('batch_real_ranking', 0)
                                if real_ranking > 0:
                                    batch_summary += f" [~{real_ranking}]"
                                
                                message += f"{batch_summary}\n"
                                
                                # Show individual games in this batch with cumulative differentials
                                for game in batch['games']:
                                    game_date = self._format_game_date(game.get('startTimestamp', 0))
                                    date_prefix = f"{game_date} " if game_date else ""
                                    
                                    # Calculate game differential and update cumulative counters
                                    game_net_score = game.get('net_score', 0)
                                    game_role = game.get('role', 'home')
                                    
                                    if game_role == 'home':
                                        cumulative_home_net += game_net_score
                                    else:
                                        cumulative_away_net += game_net_score
                                    
                                    # Format cumulative differentials
                                    cum_home_str = f"+{cumulative_home_net}" if cumulative_home_net >= 0 else str(cumulative_home_net)
                                    cum_away_str = f"+{cumulative_away_net}" if cumulative_away_net >= 0 else str(cumulative_away_net)
                                    
                                    message += f"{date_prefix} ~{game['own_ranking']} {game['result']} vs ~{game['opponent_ranking']} {game['opponent']} ({game['home_score']}-{game['away_score']})\n"
                                
                                # Add break line between batches (except for the last batch)
                                if i < len(streak.home_team_batches) - 1:
                                    message += "\n"
                        else:
                            message += f"<b>{streak.home_team_name}</b>: No recent form data\n"
                    else:
                        # Display home team batches
                        if streak.home_team_batches:
                            final_ranking_str = f" (~{streak.home_team_final_real_ranking})" if streak.home_team_final_real_ranking > 0 else ""
                            message += f"<b>{streak.home_team_name}{final_ranking_str}</b>:\n"
                            # Initialize cumulative counters across all batches
                            cumulative_home_net = 0
                            cumulative_away_net = 0
                            
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
                                
                                # Show individual games in this batch with cumulative differentials
                                for game in batch['games']:
                                    game_date = self._format_game_date(game.get('startTimestamp', 0))
                                    date_prefix = f"{game_date} " if game_date else ""
                                    role_indicator = "🏠" if game.get('role') == 'home' else "✈️"
                                    
                                    # Calculate game differential and update cumulative counters
                                    game_net_score = game.get('net_score', 0)
                                    game_role = game.get('role', 'home')
                                    
                                    if game_role == 'home':
                                        cumulative_home_net += game_net_score
                                    else:
                                        cumulative_away_net += game_net_score
                                    
                                    # Format cumulative differentials
                                    cum_home_str = f"+{cumulative_home_net}" if cumulative_home_net >= 0 else str(cumulative_home_net)
                                    cum_away_str = f"+{cumulative_away_net}" if cumulative_away_net >= 0 else str(cumulative_away_net)
                                    
                                    # Format standings prefix (team's position) and suffix (opponent's position)
                                    team_standings_str = ""
                                    opponent_standings_str = ""
                                    if game.get('standings_position') is not None:
                                        team_standings_str = f"[#{game['standings_position']}] "
                                    if game.get('opponent_standings_position') is not None:
                                        opponent_standings_str = f" [#{game['opponent_standings_position']}]"
                                    
                                    message += f"{date_prefix}{role_indicator}{team_standings_str}{game['result']} vs {game['opponent']} ({game['score_for']}-{game['score_against']}){opponent_standings_str}\n"
                                
                                # Add break line between batches (except for the last batch)
                                if i < len(streak.home_team_batches) - 1:
                                    message += "\n"
                        else:
                            message += f"<b>{streak.home_team_name}</b>: No recent form data\n"
                    
                    message += "\n"
                    
                    # Display away team batches
                    if streak.away_team_batches:
                        final_ranking_str = f" (~{streak.away_team_final_real_ranking})" if streak.away_team_final_real_ranking > 0 else ""
                        message += f"<b>{streak.away_team_name}{final_ranking_str}</b>:\n"
                        # Initialize cumulative counters across all batches
                        cumulative_home_net = 0
                        cumulative_away_net = 0
                        
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
                            
                            # Add real ranking (only for Tennis/Tennis Doubles)
                            if streak.sport == 'Tennis' or streak.sport == 'Tennis Doubles':
                                real_ranking = batch.get('batch_real_ranking', 0)
                                if real_ranking > 0:
                                    batch_summary += f" [~{real_ranking}]"
                            
                            message += f"{batch_summary}\n"
                            
                            # Show individual games in this batch with cumulative differentials
                            for game in batch['games']:
                                game_date = self._format_game_date(game.get('startTimestamp', 0))
                                date_prefix = f"{game_date} " if game_date else ""
                                
                                # Calculate game differential and update cumulative counters
                                game_net_score = game.get('net_score', 0)
                                game_role = game.get('role', 'home')
                                
                                if game_role == 'home':
                                    cumulative_home_net += game_net_score
                                else:
                                    cumulative_away_net += game_net_score
                                
                                # Format cumulative differentials
                                cum_home_str = f"+{cumulative_home_net}" if cumulative_home_net >= 0 else str(cumulative_home_net)
                                cum_away_str = f"+{cumulative_away_net}" if cumulative_away_net >= 0 else str(cumulative_away_net)
                                
                                if streak.sport == 'Tennis' or streak.sport == 'Tennis Doubles':
                                    message += f"{date_prefix} ~{game['own_ranking']} {game['result']} vs ~{game['opponent_ranking']} {game['opponent']} ({game['home_score']}-{game['away_score']})\n"
                                else:
                                    role_indicator = "🏠" if game_role == 'home' else "✈️"
                                    # Format standings prefix (team's position) and suffix (opponent's position)
                                    team_standings_str = ""
                                    opponent_standings_str = ""
                                    if game.get('standings_position') is not None:
                                        team_standings_str = f"[#{game['standings_position']}] "
                                    if game.get('opponent_standings_position') is not None:
                                        opponent_standings_str = f" [#{game['opponent_standings_position']}]"
                                    
                                    message += f"{date_prefix}{role_indicator}{team_standings_str}{game['result']} vs {game['opponent']} ({game['score_for']}-{game['score_against']}){opponent_standings_str}\n"
                            
                            # Add break line between batches (except for the last batch)
                            if i < len(streak.away_team_batches) - 1:
                                message += "\n"
                    else:
                        message += f"<b>{streak.away_team_name}</b>: No recent form data\n"
                    
                    message += "\n"
            
            # NEW: Ranking Prediction Section
            ranking_prediction = self._calculate_ranking_prediction(streak, home_total_games, away_total_games)
            if ranking_prediction:
                message += f"🎯 Ranking Prediction:\n"
                message += f"Ranking Advantage: {ranking_prediction['ranking_advantage']}\n"
                message += f"<b>{ranking_prediction['best_team_name']}</b> (~{ranking_prediction['best_ranking']}):\n"
                message += f"Total Points: {ranking_prediction['best_total_points']}\n\n"
                
                message += f"<b>{ranking_prediction['worst_team_name']}</b> (~{ranking_prediction['worst_ranking']}):\n"
                message += f"Total Points: {ranking_prediction['worst_total_points']}\n\n"
                
                # Final prediction based on total points difference
                prediction_diff = ranking_prediction['prediction_diff']
                if prediction_diff > 0:
                    message += f"🏆 Prediction: {ranking_prediction['best_team_name']} wins by {prediction_diff} points\n"
                elif prediction_diff < 0:
                    message += f"🏆 Prediction: {ranking_prediction['worst_team_name']} wins by {abs(prediction_diff)} points\n"
                else:
                    message += f"🏆 Prediction: Tie (0 point difference)\n"
                
                message += "\n"
            
            # NEW: Winning Odds Section. Turned off for now
            # if hasattr(streak, 'winning_odds_data') and streak.winning_odds_data:
            #     # Check if we have any valid odds data
            #     has_home_odds = 'home' in streak.winning_odds_data and streak.winning_odds_data['home'] is not None
            #     has_away_odds = 'away' in streak.winning_odds_data and streak.winning_odds_data['away'] is not None
                
            #     if has_home_odds or has_away_odds:
            #         message += f"🎯 Winning Odds:\n"
                    
            #         # Home team odds
            #         if has_home_odds:
            #             home_odds = streak.winning_odds_data['home']
            #             home_decimal = home_odds.get('decimalValue', 0)
            #             home_expected = home_odds.get('expected', 0)
            #             home_actual = home_odds.get('actual', 0)
                        
            #             message += f"<b>{streak.home_team_name}</b>\n"
            #             message += f"📊 Odds: {home_decimal} (Expected: {home_expected}%, Actual: {home_actual}%)\n"
            #             if home_actual > home_expected:
            #                 message += f"✅⬆️ {home_actual - home_expected}%\n"
            #             elif home_actual < home_expected:
            #                 message += f"⚠️⬇️ {home_expected - home_actual}%\n"
            #             else:
            #                 message += f"⚖️ Meeting expectations\n"
            #         else:
            #             message += f"{streak.home_team_name}: No odds data available\n"
                    
            #         # Away team odds
            #         if has_away_odds:
            #             away_odds = streak.winning_odds_data['away']
            #             away_decimal = away_odds.get('decimalValue', 0)
            #             away_expected = away_odds.get('expected', 0)
            #             away_actual = away_odds.get('actual', 0)
                        
            #             message += f"<b>{streak.away_team_name}</b>\n"
            #             message += f"📊 Odds: {away_decimal} (Expected: {away_expected}%, Actual: {away_actual}%)\n"
            #             if away_actual > away_expected:
            #                 message += f"✅⬆️ {away_actual - away_expected}%\n"
            #             elif away_actual < away_expected:
            #                 message += f"⚠️⬇️ {away_expected - away_actual}%\n"
            #             else:
            #                 message += f"⚖️ Meeting expectations\n"
            #         else:
            #             message += f"<b>{streak.away_team_name}</b>: <i>No odds data available</i>\n"
                    
            #         message += "\n"
            
            
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

