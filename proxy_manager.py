import re
import secrets
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import quote


DECODO_DEFAULT_ENDPOINT = "gate.decodo.com:7000"
DEFAULT_PROXY_PROTOCOL = "http"

_SESSION_TOKEN_PATTERN = re.compile(r"(session-)([^-]+)")
_SAFE_CHARS_PATTERN = re.compile(r"[^a-z0-9\-]")
_TRUE_VALUES = {"1", "true", "yes", "on"}
_DECODO_RUNTIME_PARAM_PATTERN = re.compile(
    r"-(?:country|city|state|zip|asn|continent|session|sessionduration|session-duration)-[^-]+",
    re.IGNORECASE,
)


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in _TRUE_VALUES


def _normalize_mode(mode: Optional[str], default_mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if not normalized:
        return default_mode
    if normalized in {"rotating", "sticky", "short-sticky"}:
        return normalized
    return default_mode


def _safe_geo_value(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower().replace(" ", "-")
    normalized = _SAFE_CHARS_PATTERN.sub("", normalized)
    return normalized


def _normalize_provider(provider: Optional[str], endpoint: str) -> str:
    normalized = (provider or "").strip().lower()
    if not normalized or normalized == "auto":
        return "decodo" if "decodo" in endpoint.lower() else "legacy"
    return normalized


def _strip_decodo_runtime_params(username_value: Optional[str]) -> str:
    base = (username_value or "").strip()
    if not base:
        return ""

    previous = None
    normalized = base
    while previous != normalized:
        previous = normalized
        normalized = _DECODO_RUNTIME_PARAM_PATTERN.sub("", normalized)

    return normalized.strip("-")


def _ensure_decodo_user_prefix(username_base: str) -> str:
    base = (username_base or "").strip()
    if not base:
        return ""
    if base.lower().startswith("user-"):
        return base
    return f"user-{base}"


def mint_session_token(length: int = 12) -> str:
    target_length = max(8, int(length))
    token = secrets.token_hex(target_length)
    return token[:target_length]


def mask_secret(value: Optional[str], keep_prefix: int = 3, keep_suffix: int = 2) -> str:
    raw = value or ""
    if not raw:
        return ""
    if len(raw) <= keep_prefix + keep_suffix:
        return "*" * len(raw)
    return f"{raw[:keep_prefix]}***{raw[-keep_suffix:]}"


def mask_decodo_username(username: str) -> str:
    return _SESSION_TOKEN_PATTERN.sub(
        lambda match: f"{match.group(1)}{mask_secret(match.group(2), keep_prefix=4, keep_suffix=2)}",
        username or "",
    )


def mask_proxy_url(proxy_url: str) -> str:
    if not proxy_url:
        return ""
    # protocol://username:password@endpoint
    pattern = r"^(?P<scheme>[a-zA-Z]+://)?(?:(?P<username>[^:@]+):(?P<password>[^@]+)@)?(?P<endpoint>.+)$"
    match = re.match(pattern, proxy_url)
    if not match:
        return proxy_url
    scheme = match.group("scheme") or ""
    username = match.group("username")
    password = match.group("password")
    endpoint = match.group("endpoint") or ""

    if username is None and password is None:
        return f"{scheme}{endpoint}"

    masked_username = mask_secret(username or "", keep_prefix=3, keep_suffix=1)
    return f"{scheme}{masked_username}:***@{endpoint}"


def build_decodo_username(
    username_base: str,
    *,
    country: Optional[str] = None,
    city: Optional[str] = None,
    session_token: Optional[str] = None,
    session_duration_minutes: Optional[int] = None,
    include_session: bool = True,
) -> str:
    base = (username_base or "").strip()
    if not base:
        return ""

    normalized_base = base

    def _has_segment(segment_prefix: str) -> bool:
        return bool(re.search(rf"(?:^|-){re.escape(segment_prefix)}", normalized_base))

    suffixes = []
    safe_country = _safe_geo_value(country)
    safe_city = _safe_geo_value(city)

    if safe_country and not _has_segment("country-"):
        suffixes.append(f"country-{safe_country}")
    if safe_city and not _has_segment("city-"):
        suffixes.append(f"city-{safe_city}")

    if include_session and session_token:
        if _has_segment("session-"):
            normalized_base = re.sub(
                r"(?:^|-)session-[^-]+",
                lambda match: match.group(0).split("session-")[0] + f"session-{session_token}",
                normalized_base,
                count=1,
            )
        else:
            suffixes.append(f"session-{session_token}")
        if session_duration_minutes and int(session_duration_minutes) > 0:
            session_duration_value = f"sessionduration-{int(session_duration_minutes)}"
            if _has_segment("sessionduration-"):
                normalized_base = re.sub(
                    r"(?:^|-)sessionduration-[^-]+",
                    lambda match: match.group(0).split("sessionduration-")[0] + session_duration_value,
                    normalized_base,
                    count=1,
                )
            else:
                suffixes.append(session_duration_value)

    return "-".join([normalized_base] + suffixes) if suffixes else normalized_base


@dataclass(frozen=True)
class ProxyIdentity:
    enabled: bool
    provider: str
    protocol: str
    endpoint: str
    username: str
    password: str
    mode: str
    session_token: Optional[str] = None
    generation: int = 0
    client_name: str = ""
    session_duration_minutes: Optional[int] = None
    country: Optional[str] = None
    city: Optional[str] = None


class ProxyIdentityManager:
    def __init__(self, config: Any, *, client_name: str):
        self.config = config
        self.client_name = (client_name or "default").strip().lower()
        self.proxy_enabled = _to_bool(getattr(config, "PROXY_ENABLED", False), False)
        self.safe_logging = _to_bool(getattr(config, "PROXY_LOG_SAFE", True), True)

        configured_endpoint = (getattr(config, "PROXY_ENDPOINT", "") or "").strip()
        self.provider = _normalize_provider(getattr(config, "PROXY_PROVIDER", ""), configured_endpoint)
        self.protocol = ((getattr(config, "PROXY_PROTOCOL", DEFAULT_PROXY_PROTOCOL) or DEFAULT_PROXY_PROTOCOL).strip().lower())
        self.endpoint = configured_endpoint
        if self.provider == "decodo" and not self.endpoint:
            self.endpoint = DECODO_DEFAULT_ENDPOINT
        self._is_decodo_gate = self.endpoint.lower().startswith("gate.decodo.com")

        self.username = (getattr(config, "PROXY_USERNAME", "") or "").strip()
        self.password = (getattr(config, "PROXY_PASSWORD", "") or "").strip()
        raw_username_base = (getattr(config, "PROXY_USERNAME_BASE", self.username) or self.username).strip()
        if self.provider == "decodo":
            sanitized_base = _strip_decodo_runtime_params(raw_username_base)
            if self._is_decodo_gate:
                sanitized_base = _ensure_decodo_user_prefix(sanitized_base)
            self.username_base = sanitized_base
        else:
            self.username_base = raw_username_base
        self.country = _safe_geo_value(getattr(config, "PROXY_COUNTRY", ""))
        self.city = _safe_geo_value(getattr(config, "PROXY_CITY", ""))
        self.session_duration_minutes = int(getattr(config, "PROXY_SESSION_DURATION_MINUTES", 10) or 10)

        default_mode = "sticky" if self.client_name == "oddsportal" else "rotating"
        configured_mode = getattr(
            config,
            "PROXY_MODE_ODDSPORTAL" if self.client_name == "oddsportal" else "PROXY_MODE_SOFASCORE",
            default_mode,
        )
        self.mode = _normalize_mode(configured_mode, default_mode)

        # OddsPortal must stay sticky in Decodo mode.
        if self.provider == "decodo" and self.client_name == "oddsportal" and self.mode == "rotating":
            self.mode = "sticky"

        self.rotate_on_browser_restart = _to_bool(
            getattr(config, "PROXY_ROTATE_ON_ODDSPORTAL_BROWSER_RESTART", True),
            True,
        )
        self.rotate_on_sofascore_proxy_error = _to_bool(
            getattr(config, "PROXY_ROTATE_ON_SOFASCORE_PROXY_ERROR", True),
            True,
        )

        self._identity: Optional[ProxyIdentity] = None
        self._generation = 0

    def _can_use_proxy(self) -> bool:
        if not self.proxy_enabled:
            return False
        if not self.endpoint:
            return False
        if self.provider == "decodo":
            return bool(self.password and self.username_base)
        # Legacy flow keeps old behavior: require username + password for auth proxy.
        return bool(self.username and self.password)

    def _build_identity(self) -> ProxyIdentity:
        self._generation += 1
        if not self._can_use_proxy():
            return ProxyIdentity(
                enabled=False,
                provider=self.provider,
                protocol=self.protocol or DEFAULT_PROXY_PROTOCOL,
                endpoint=self.endpoint,
                username="",
                password="",
                mode=self.mode,
                generation=self._generation,
                client_name=self.client_name,
            )

        session_token: Optional[str] = None
        if self.provider == "decodo":
            include_session = self.mode in {"sticky", "short-sticky"}
            if include_session:
                session_token = mint_session_token()
            username = build_decodo_username(
                self.username_base,
                country=self.country,
                city=self.city,
                session_token=session_token,
                session_duration_minutes=self.session_duration_minutes,
                include_session=include_session,
            )
        else:
            username = self.username

        return ProxyIdentity(
            enabled=True,
            provider=self.provider,
            protocol=self.protocol or DEFAULT_PROXY_PROTOCOL,
            endpoint=self.endpoint,
            username=username,
            password=self.password,
            mode=self.mode,
            session_token=session_token,
            generation=self._generation,
            client_name=self.client_name,
            session_duration_minutes=self.session_duration_minutes if session_token else None,
            country=self.country or None,
            city=self.city or None,
        )

    def get_identity(self, *, rotate_session: bool = False, reason: str = "runtime") -> ProxyIdentity:
        del reason  # currently informational only
        if self._identity is None or rotate_session:
            self._identity = self._build_identity()
        return self._identity

    def rotate_session(self, reason: str = "runtime") -> ProxyIdentity:
        return self.get_identity(rotate_session=True, reason=reason)

    def build_requests_proxy_url(self, identity: Optional[ProxyIdentity] = None) -> Optional[str]:
        active_identity = identity or self.get_identity()
        if not active_identity.enabled:
            return None

        endpoint = active_identity.endpoint
        protocol = active_identity.protocol or DEFAULT_PROXY_PROTOCOL
        username = active_identity.username
        password = active_identity.password
        if not endpoint:
            return None
        return f"{protocol}://{quote(username, safe='')}:{quote(password, safe='')}@{endpoint}"

    def build_requests_proxies(self, identity: Optional[ProxyIdentity] = None) -> Dict[str, str]:
        proxy_url = self.build_requests_proxy_url(identity)
        if not proxy_url:
            return {}
        return {"http": proxy_url, "https": proxy_url}

    def build_playwright_proxy(self, identity: Optional[ProxyIdentity] = None) -> Optional[Dict[str, str]]:
        active_identity = identity or self.get_identity()
        if not active_identity.enabled:
            return None
        server = f"{active_identity.protocol}://{active_identity.endpoint}"
        return {
            "server": server,
            "username": active_identity.username,
            "password": active_identity.password,
        }

    def session_label(self, identity: Optional[ProxyIdentity] = None) -> str:
        active_identity = identity or self.get_identity()
        if not active_identity.enabled:
            return "no-proxy"
        if active_identity.session_token:
            return mask_secret(active_identity.session_token, keep_prefix=4, keep_suffix=2)
        return f"{active_identity.mode}-g{active_identity.generation}"

    def describe_identity(self, identity: Optional[ProxyIdentity] = None) -> str:
        active_identity = identity or self.get_identity()
        if not active_identity.enabled:
            return "proxy=disabled"

        username_for_log = active_identity.username
        if self.safe_logging:
            if active_identity.provider == "decodo":
                username_for_log = mask_decodo_username(username_for_log)
            else:
                username_for_log = mask_secret(username_for_log, keep_prefix=4, keep_suffix=1)

        pieces = [
            f"provider={active_identity.provider}",
            f"mode={active_identity.mode}",
            f"endpoint={active_identity.endpoint}",
            f"username={username_for_log}",
        ]

        if active_identity.session_token:
            pieces.append(f"session={self.session_label(active_identity)}")
        return ", ".join(pieces)

    def should_rotate_on_sofascore_error(self) -> bool:
        return self.provider == "decodo" and self.rotate_on_sofascore_proxy_error

    def should_rotate_on_browser_restart(self) -> bool:
        return self.provider == "decodo" and self.rotate_on_browser_restart

    @staticmethod
    def looks_like_proxy_or_network_error(error: Exception) -> bool:
        message = str(error or "").lower()
        markers = (
            "proxy",
            "407",
            "tunnel",
            "connection reset",
            "connection aborted",
            "timed out",
            "timeout",
            "temporarily unavailable",
            "dns",
            "network",
        )
        return any(marker in message for marker in markers)
