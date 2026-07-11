"""Transport and compatibility facade for SofaScore."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from curl_cffi import requests

from infrastructure.network import ProxyIdentityManager
from infrastructure.settings import Config
from shared.shutdown import is_shutdown_requested

from .discovery_feeds import (
    extract_events_and_odds_from_dropping_response,
    extract_events_from_high_value_streaks,
    get_dropping_odds_with_odds_and_events_response,
    get_h2h_events,
    get_high_value_streaks_events,
    get_team_streaks_events,
    get_winning_odds_events,
)
from .event_details import get_event_details, get_event_results, update_event_information_from_response
from .event_normalizer import clean_competition, get_gender, normalize_event_payload
from .challenge import (
    build_challenge_evidence,
    body_preview,
    get_challenge_reason,
    is_sofascore_challenge_response,
    write_challenge_evidence,
)
from .exceptions import SofaScoreChallengeException, SofaScoreNotFoundException, SofaScoreRateLimitException
from .h2h import get_h2h_events_for_event
from .results_parser import extract_results_from_response
from .schedule_feeds import (
    get_live_events_response_per_sport,
    get_today_sport_events_odds_response,
    get_today_sport_events_response,
    get_unique_tournament_scheduled_events,
)
from .standings import get_standings_response, process_standings_response
from .team_history import get_nearest_event_for_team, get_team_last_results_response
from .winning_odds import get_winning_odds_response

logger = logging.getLogger(__name__)


def _safe_token_fingerprint(token: str | None) -> str:
    if not token:
        return "none"
    digest = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    return f"sha256:{digest[:10]}"


def _safe_token_suffix(token: str | None) -> str:
    if not token:
        return "none"
    token = str(token)
    return token[-2:] if len(token) >= 2 else "**"


def _safe_token_context(token: str | None, header_sent: bool) -> dict:
    return {
        "x_requested_with_header_sent": bool(header_sent),
        "x_requested_with_value_non_empty": bool(token),
        "x_requested_with_fingerprint": _safe_token_fingerprint(token),
        "x_requested_with_suffix": _safe_token_suffix(token),
    }


class SofaScoreAPI:
    def __init__(self):
        self.base_url = Config.SOFASCORE_BASE_URL
        self.session = None
        self.x_requested_with = getattr(Config, "SOFASCORE_X_REQUESTED_WITH", "XMLHttpRequest")
        self.last_request_time = 0
        self._rate_limit_lock = threading.Lock()
        self.proxy_manager = ProxyIdentityManager(Config, client_name="sofascore")
        self.proxy_identity = None
        self._proxy_error_streak = 0
        self.challenge_evidence_enabled = bool(getattr(Config, "global_debug_mode", False))
        self._x_requested_with_missing_warned = False
        self._setup_session(reason="initial_setup")

    def set_challenge_evidence_enabled(self, enabled: bool) -> None:
        self.challenge_evidence_enabled = bool(enabled)

    def _should_capture_challenge_evidence(self) -> bool:
        return bool(self.challenge_evidence_enabled)

    def _build_headers(self) -> Dict[str, str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Origin": "https://www.sofascore.com",
            "Referer": "https://www.sofascore.com/",
            "Sec-Ch-Ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        if self.x_requested_with is not None:
            headers["X-Requested-With"] = self.x_requested_with
        else:
            if not self._x_requested_with_missing_warned:
                logger.warning(
                    "SOFASCORE_X_REQUESTED_WITH is disabled; SofaScore requests may receive challenge responses"
                )
                self._x_requested_with_missing_warned = True

        return headers

    def _setup_session(self, rotate_proxy_identity: bool = False, reason: str = "runtime"):
        self.session = requests.Session(impersonate="chrome136")

        if not self.proxy_manager.proxy_enabled:
            logger.info("Proxy disabled - using direct connection")
            self.proxy_identity = None
            return

        self.proxy_identity = self.proxy_manager.get_identity(
            rotate_session=rotate_proxy_identity,
            reason=reason,
        )

        proxies = self.proxy_manager.build_requests_proxies(self.proxy_identity)
        if not proxies:
            logger.warning("Proxy is enabled but proxy identity is not valid; using direct connection")
            self.proxy_identity = None
            return

        self.session.proxies = proxies
        logger.info(
            "SofaScore proxy ready (gen=%s, %s)",
            self.proxy_identity.generation,
            self.proxy_manager.describe_identity(self.proxy_identity),
        )

    def _rotate_proxy_identity(self, reason: str):
        if not self.proxy_manager.proxy_enabled:
            return
        old_gen = self.proxy_identity.generation if self.proxy_identity else 0
        self._setup_session(rotate_proxy_identity=True, reason=reason)
        new_gen = self.proxy_identity.generation if self.proxy_identity else 0
        logger.info(
            "Proxy session rotated: reason=%s, gen %s -> %s (new curl_cffi session created)",
            reason, old_gen, new_gen,
        )
        self._proxy_error_streak = 0

    def _rate_limit(self):
        if is_shutdown_requested():
            raise KeyboardInterrupt()

        with self._rate_limit_lock:
            if is_shutdown_requested():
                raise KeyboardInterrupt()

            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = Config.REQUEST_DELAY_SECONDS

            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                logger.debug("Rate limiting: sleeping for %.2f seconds", sleep_time)
                time.sleep(sleep_time)

            if is_shutdown_requested():
                raise KeyboardInterrupt()

            self.last_request_time = time.time()

    def _extract_endpoint_event_id(self, endpoint: str) -> int:
        parts = endpoint.split("/")
        if len(parts) >= 3 and parts[1] == "event" and parts[2].isdigit():
            return int(parts[2])
        return 0

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        no_retry_on_404: bool = False,
        delete_event_on_404: bool = False,
    ) -> Optional[Dict]:
        url = f"{self.base_url}{endpoint}"
        headers = self._build_headers()
        x_requested_with_token = headers.get("X-Requested-With")

        for attempt in range(Config.MAX_RETRIES):
            try:
                if is_shutdown_requested():
                    raise KeyboardInterrupt()

                self._rate_limit()
                if is_shutdown_requested():
                    raise KeyboardInterrupt()

                logger.debug("Making request to: %s", url)
                response = self.session.get(url, headers=headers, params=params, timeout=30)

                if response.status_code == 200:
                    self._proxy_error_streak = 0
                    return response.json()

                if is_sofascore_challenge_response(response):
                    reason = get_challenge_reason(response)
                    token_context = _safe_token_context(
                        x_requested_with_token,
                        "X-Requested-With" in headers,
                    )
                    evidence = {"request_token_context": token_context}

                    logger.info(
                        "SofaScore challenge token context: token_fingerprint=%s token_suffix=%s endpoint=%s",
                        token_context["x_requested_with_fingerprint"],
                        token_context["x_requested_with_suffix"],
                        endpoint,
                    )

                    if self._should_capture_challenge_evidence():
                        challenge_evidence = build_challenge_evidence(
                            response=response,
                            endpoint=endpoint,
                            base_url=self.base_url,
                            attempt=attempt + 1,
                            max_retries=Config.MAX_RETRIES,
                            params=params,
                            proxy_identity=self.proxy_identity,
                            request_url=url,
                        )
                        challenge_evidence["request_token_context"] = token_context
                        evidence = challenge_evidence
                        write_challenge_evidence(evidence)
                    else:
                        logger.debug(
                            "SofaScore challenge evidence capture disabled for %s (debug_mode=%s)",
                            endpoint,
                            self.challenge_evidence_enabled,
                        )

                    logger.error(
                        "SofaScore challenge detected for %s, reason=%s, attempt %s/%s",
                        endpoint,
                        reason,
                        attempt + 1,
                        Config.MAX_RETRIES,
                    )

                    if (
                        attempt == 0
                        and self.proxy_manager.should_rotate_on_sofascore_error()
                        and Config.MAX_RETRIES > 1
                    ):
                        self._rotate_proxy_identity(
                            reason=f"http_403_challenge_attempt_{attempt + 1}_{endpoint}"
                        )
                        continue

                    raise SofaScoreChallengeException(
                        self._extract_endpoint_event_id(endpoint),
                        endpoint=endpoint,
                        reason=reason,
                        evidence=evidence,
                    )

                if response.status_code == 407:
                    wait_time = min(30 * (2**attempt), 300)
                    logger.warning(
                        "Proxy authentication error (407) for %s, waiting %ss, attempt %s/%s",
                        endpoint,
                        wait_time,
                        attempt + 1,
                        Config.MAX_RETRIES,
                    )
                    if self.proxy_manager.should_rotate_on_sofascore_error():
                        self._rotate_proxy_identity(reason=f"http_407_attempt_{attempt + 1}_{endpoint}")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    break

                if response.status_code == 429:
                    wait_time = min(60 * (2**attempt), 600)
                    logger.warning(
                        "Rate limited (429) for %s, waiting %ss, attempt %s/%s",
                        endpoint,
                        wait_time,
                        attempt + 1,
                        Config.MAX_RETRIES,
                    )
                    if self.proxy_manager.should_rotate_on_sofascore_error():
                        self._rotate_proxy_identity(reason=f"http_429_attempt_{attempt + 1}_{endpoint}")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    raise SofaScoreRateLimitException(self._extract_endpoint_event_id(endpoint), endpoint=endpoint)

                if response.status_code == 404:
                    if no_retry_on_404:
                        logger.debug("HTTP 404 for %s - skipping retry as requested", endpoint)
                    else:
                        logger.warning("HTTP 404 for %s - skipping retries", endpoint)
                    raise SofaScoreNotFoundException(self._extract_endpoint_event_id(endpoint), endpoint=endpoint)

                if response.status_code == 403:
                    wait_time = min(30 * (2**attempt), 300)
                    logger.warning(
                        "HTTP 403 for %s, waiting %ss, attempt %s/%s, body=%s",
                        endpoint,
                        wait_time,
                        attempt + 1,
                        Config.MAX_RETRIES,
                        body_preview(getattr(response, "text", "") or ""),
                    )
                    if self.proxy_manager.should_rotate_on_sofascore_error():
                        self._rotate_proxy_identity(reason=f"http_403_attempt_{attempt + 1}_{endpoint}")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    raise SofaScoreRateLimitException(self._extract_endpoint_event_id(endpoint), endpoint=endpoint)

                if response.status_code in [500, 502, 503, 504, 522, 525]:
                    wait_time = min(5 * (2**attempt), 60)
                    logger.warning(
                        "HTTP %s for %s, waiting %ss, attempt %s/%s",
                        response.status_code,
                        endpoint,
                        wait_time,
                        attempt + 1,
                        Config.MAX_RETRIES,
                    )
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    break

                logger.error("HTTP %s for %s: %s", response.status_code, endpoint, response.text)
                break
            except (
                SofaScoreChallengeException,
                SofaScoreNotFoundException,
                SofaScoreRateLimitException,
            ):
                raise
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if is_shutdown_requested():
                    logger.info("Shutdown requested while requesting %s", endpoint)
                    raise KeyboardInterrupt() from exc
                logger.error("Unexpected error for %s: %s", endpoint, exc)
                break

        return None

    def _request_json(self, endpoint: str, params: Optional[Dict] = None, no_retry_on_404: bool = False) -> Optional[Dict]:
        try:
            return self._make_request(endpoint, params=params, no_retry_on_404=no_retry_on_404)
        except SofaScoreChallengeException as exc:
            logger.error("%s", exc)
            return None
        except (SofaScoreNotFoundException, SofaScoreRateLimitException) as exc:
            logger.warning("%s", exc)
            return None

    def clean_competition(self, competition: str) -> str:
        return clean_competition(competition)

    def get_gender(self, home_team: Dict, away_team: Dict) -> str:
        return get_gender(home_team, away_team)

    def normalize_event_payload(self, event: Dict, discovery_source: str = "dropping_odds") -> Dict:
        return normalize_event_payload(event, discovery_source)

    def get_dropping_odds_with_odds_and_events_response(self, sport: str = None) -> Optional[Dict]:
        return get_dropping_odds_with_odds_and_events_response(self, sport=sport)

    def get_high_value_streaks_events(self):
        return get_high_value_streaks_events(self)

    def get_team_streaks_events(self):
        return get_team_streaks_events(self)

    def get_h2h_events(self):
        return get_h2h_events(self)

    def get_winning_odds_events(self):
        return get_winning_odds_events(self)

    def get_nearest_event_for_team(self, team_id: int) -> Optional[Dict]:
        return get_nearest_event_for_team(self, team_id)

    def get_event_details(self, event_id: int) -> Optional[Dict]:
        return get_event_details(self, event_id)

    def get_live_events_response_per_sport(self, sport: str) -> Optional[Dict]:
        return get_live_events_response_per_sport(self, sport)

    def get_team_last_results_response(
        self,
        team_id: int,
        is_tennis_singles: bool = False,
        is_tennis_doubles: bool = False,
        fetch_index: int = 0,
    ) -> Optional[Dict]:
        return get_team_last_results_response(
            self,
            team_id,
            is_tennis_singles=is_tennis_singles,
            is_tennis_doubles=is_tennis_doubles,
            fetch_index=fetch_index,
        )

    def get_winning_odds_response(self, event_id: int) -> Optional[Dict]:
        return get_winning_odds_response(self, event_id)

    def get_standings_response(
        self,
        season_id: int,
        unique_tournament_id: int,
        competition_context=None,
        standings_endpoint_missing_competition_ids=None,
    ) -> Optional[Dict]:
        return get_standings_response(
            self,
            season_id,
            unique_tournament_id,
            competition_context=competition_context,
            standings_endpoint_missing_competition_ids=standings_endpoint_missing_competition_ids,
        )

    def process_standings_response(
        self,
        standings: Optional[List[Dict]],
        home_team_id: Optional[int],
        away_team_id: Optional[int],
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        return process_standings_response(standings, home_team_id, away_team_id)

    def extract_events_from_high_value_streaks(self, response: Dict) -> Tuple[List[Dict], List[Dict]]:
        return extract_events_from_high_value_streaks(response)

    def get_h2h_events_for_event(self, custom_id: str) -> Optional[Dict]:
        return get_h2h_events_for_event(self, custom_id)

    def get_today_sport_events_response(self, date: str, sport: str, page: int = 1):
        return get_today_sport_events_response(self, date, sport, page)

    def get_unique_tournament_scheduled_events(self, unique_tournament_id: int | str, date: str):
        return get_unique_tournament_scheduled_events(self, unique_tournament_id, date)

    def get_today_sport_events_odds_response(self, date: str, sport: str):
        return get_today_sport_events_odds_response(self, date, sport)

    def get_event_final_odds(self, id: int, slug: str = None, no_retry_on_404: bool = False) -> Optional[Dict]:
        if slug:
            logger.info("✈️ Fetching final odds for event %s - %s using dedicated endpoint", id, slug)
        return self._request_json(f"/event/{id}/odds/1/all", no_retry_on_404=no_retry_on_404)

    def update_event_information_from_response(self, response: Dict) -> bool:
        return update_event_information_from_response(response)

    def _extract_observations_from_response(self, response: Dict):
        from .event_details import _extract_observations_from_response as _extract

        return _extract(response)

    def _extract_metadata_snapshot(self, response: Dict):
        from .event_details import _extract_metadata_snapshot

        return _extract_metadata_snapshot(response)

    def get_event_results(
        self,
        event_id: int,
        update_time: bool = False,
        update_court_type: bool = False,
        minutes_until_start: int = 0,
        update_event_info: bool = True,
        return_snapshot: bool = False,
        current_start_time: Optional[datetime] = None,
        canonical_event_id: int | None = None,
    ) -> Optional[Dict]:
        return get_event_results(
            self,
            event_id,
            canonical_event_id=canonical_event_id,
            update_time=update_time,
            update_court_type=update_court_type,
            minutes_until_start=minutes_until_start,
            update_event_info=update_event_info,
            return_snapshot=return_snapshot,
            current_start_time=current_start_time,
        )

    def extract_results_from_response(
        self,
        response: Dict,
        extract_tennis_points: bool = False,
        for_streaks: bool = False,
    ) -> Optional[Dict]:
        return extract_results_from_response(
            response,
            extract_tennis_points=extract_tennis_points,
            for_streaks=for_streaks,
        )

    def extract_events_and_odds_from_dropping_response(
        self,
        response: Dict,
        odds_extraction: bool = True,
        discovery_source: str = "dropping_odds",
    ) -> Tuple[List[Dict], Dict]:
        return extract_events_and_odds_from_dropping_response(
            response,
            odds_extraction=odds_extraction,
            discovery_source=discovery_source,
        )

    def check_and_update_starting_time(
        self,
        event_id: int,
        startTimeStamp: int,
        send_alert: bool = False,
        current_starting_time: Optional[datetime] = None,
    ) -> bool:
        from modules.jobs.pre_start_check_job.timestamp_corrections import check_and_update_starting_time as _check

        return _check(
            event_id,
            startTimeStamp,
            send_alert=send_alert,
            current_starting_time=current_starting_time,
        )

    def convert_timestamp_to_datetime(self, timestamp: int) -> datetime:
        from modules.jobs.pre_start_check_job.timestamp_corrections import convert_timestamp_to_datetime as _convert

        return _convert(timestamp)

    def is_event_starting_soon(self, start_timestamp: int, window_minutes: int = 30) -> bool:
        from modules.jobs.pre_start_check_job.timestamp_corrections import is_event_starting_soon as _is_soon

        return _is_soon(start_timestamp, window_minutes=window_minutes)


api_client = SofaScoreAPI()
