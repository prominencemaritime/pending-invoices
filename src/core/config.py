#src/core/config.py
"""
Centralized configuration management for alert system.

Loads configuration from environment variables and provides
validated access to all settings needed by alerts.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
from pathlib import Path
from decouple import config
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """
    Configuration container for alert system.

    All alerts share this common configuration, with the ability
    to override specific settings per alert type if needed.
    """

    # Project structure
    project_root: Path
    queries_dir: Path
    logs_dir: Path
    data_dir: Path
    media_dir: Path

    # Database connection handled by db_utils
    # (no config needed here)

    # Email settings
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str

    # Company-specific email routing
    email_routing: Dict[str, Dict[str, List[str]]]  # domain -> {to: [...], cc: [...]}
    internal_recipients: List[str]

    # Feature flags
    enable_email_alerts: bool
    enable_teams_alerts: bool
    enable_special_teams_email: bool
    special_teams_email: str

    # Logos
    company_logos: Dict[str, Path]  # company_name -> logo_path

    # Scheduling
    # If schedule_frequency_hours is set, interval mode is used.
    # Otherwise time+day (cron) mode is used.
    schedule_frequency_hours: Optional[float]   # None means time-based mode
    schedule_times: Optional[List[str]]         # e.g. ['09:00', '12:00']
    schedule_days: Optional[List[int]]          # ISO weekdays: 1=Mon..7=Sun
    schedule_times_timezone: str                # Timezone for schedule_times
    timezone: str

    # Alert-specific configurations
    lookback_days: Optional[int]
    include_grey_metadata_section: bool

    # Tracking
    reminder_frequency_days: Union[float, None]
    sent_events_file: Path
    resend_events_on: Bool

    # Logging
    log_file: Path
    log_max_bytes: int
    log_backup_count: int

    # URLs
    enable_links: bool
    base_url: str
    url_path: str

    # Runtime objects (injected after initialization)
    tracker: Optional['EventTracker'] = None
    email_sender: Optional['EmailSender'] = None
    html_formatter: Optional['HTMLFormatter'] = None
    text_formatter: Optional['TextFormatter'] = None
    dry_run: bool = False
    dry_run_email: str = ''  # Redirect all emails here in dry-run mode

    @classmethod
    def from_env(cls, project_root: Optional[Path] = None) -> 'AlertConfig':
        """
        Load configuration from environment variables.

        Args:
            project_root: Override project root path (default: auto-detect)

        Returns:
            AlertConfig instance with all settings loaded
        """
        # Determine project root
        if project_root is None:
            # Assume this file is in src/core/, so project root is 2 levels up
            project_root = Path(__file__).resolve().parent.parent.parent

        # Directory structure
        queries_dir = project_root / 'queries'
        logs_dir = project_root / 'logs'
        data_dir = project_root / 'data'
        media_dir = project_root / 'media'

        # Ensure directories exist
        logs_dir.mkdir(exist_ok=True)
        data_dir.mkdir(exist_ok=True)

        # Load email routing configuration
        email_routing = cls._load_email_routing()

        # Load company logos
        company_logos = {
            'prominence': media_dir / config('PROMINENCE_LOGO', default='trans_logo_prominence_procreate_small.png'),
            'seatraders': media_dir / config('SEATRADERS_LOGO', default='trans_logo_seatraders_procreate_small.png'),
        }

        return cls(
            project_root=project_root,
            queries_dir=queries_dir,
            logs_dir=logs_dir,
            data_dir=data_dir,
            media_dir=media_dir,

            # Email settings
            smtp_host=config('SMTP_HOST'),
            smtp_port=int(config('SMTP_PORT', default=465)),
            smtp_user=config('SMTP_USER'),
            smtp_pass=config('SMTP_PASS'),

            email_routing=email_routing,
            internal_recipients=cls._parse_email_list('INTERNAL_RECIPIENTS'),

            # Feature flags
            enable_email_alerts=config('ENABLE_EMAIL_ALERTS', default=True, cast=bool),
            enable_teams_alerts=config('ENABLE_TEAMS_ALERTS', default=False, cast=bool),
            enable_special_teams_email=config('ENABLE_SPECIAL_TEAMS_EMAIL_ALERT', default=False, cast=bool),
            special_teams_email=config('SPECIAL_TEAMS_EMAIL', default='').strip(),

            company_logos=company_logos,

            # Scheduling
            schedule_frequency_hours=cls._parse_optional_float('SCHEDULE_FREQUENCY_HOURS'),
            timezone=config('TIMEZONE', default='Europe/Athens'),
            schedule_times=cls._parse_csv_list('SCHEDULE_TIMES'),
            schedule_days=cls._parse_int_csv_list('SCHEDULE_DAYS'),
            schedule_times_timezone=config('SCHEDULE_TIMES_TIMEZONE', default='Europe/Athens'),

            # Tracking - if None or empty, never resend (track "forever")
            reminder_frequency_days=config('REMINDER_FREQUENCY_DAYS', default=None, cast=lambda x: float(x) if x and x.strip() else None),
            sent_events_file=data_dir / config('SENT_EVENTS_FILE', default='sent_alerts.json'),
            resend_events_on=config('RESEND_EVENTS_ON', default=False, cast=bool),

            # Logging
            log_file=logs_dir / config('LOG_FILE', default='alerts.log'),
            log_max_bytes=int(config('LOG_MAX_BYTES', default=10_485_760)),
            log_backup_count=int(config('LOG_BACKUP_COUNT', default=5)),

            # URLs
            base_url=config('BASE_URL', default='https://prominence.orca.tools/'),
            enable_links=config('ENABLE_LINKS', default='False', cast=bool),
            url_path=config('URL_PATH', default='/events'),

            # Alert-specific configurations
            lookback_days=int(config('LOOKBACK_DAYS', default=1)) if config('LOOKBACK_DAYS', default='').strip() else None,
            include_grey_metadata_section=config('INCLUDE_GREY_METADATA_SECTION', default=False, cast=bool),

            # Dry-run settings (don't set dry_run here, it's set by CLI flag in main.py)
            dry_run_email=config('DRY_RUN_EMAIL', default='').strip(),
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_optional_float(env_var: str) -> Optional[float]:
        """Return float if env var is set and non-empty, else None."""
        raw = config(env_var, default='').strip()
        return float(raw) if raw else None

    @staticmethod
    def _parse_csv_list(env_var: str) -> Optional[List[str]]:
        """Parse comma-separated string list. Returns None if env var is empty."""
        raw = config(env_var, default='').strip()
        if not raw:
            return None
        return [s.strip() for s in raw.split(',') if s.strip()]

    @staticmethod
    def _parse_int_csv_list(env_var: str) -> Optional[List[int]]:
        """Parse comma-separated int list. Returns None if env var is empty."""
        raw = config(env_var, default='').strip()
        if not raw:
            return None
        return [int(s.strip()) for s in raw.split(',') if s.strip()]

    @staticmethod
    def _parse_email_list(env_var: str) -> List[str]:
        """Parse comma-separated email list from environment variable."""
        raw = config(env_var, default='')
        return [s.strip() for s in raw.split(',') if s.strip()]

    @staticmethod
    def _load_email_routing() -> Dict[str, Dict[str, List[str]]]:
        """
        Load company-specific email routing configuration.

        Returns dict mapping domain suffix to recipient configuration:
        {
            'prominencemaritime.com': {
                'to': ['to_user1@prominencemaritime.com', ...],
                'cc': ['user1@prominencemaritime.com', ...]
            },
            'seatraders.com': {
                'to': ['to_user1@seatraders.com', ...],
                'cc': ['user1@seatraders.com', ...]
            }
        }
        """
        return {
            'prominencemaritime.com': {
                'to': AlertConfig._parse_email_list('PROMINENCE_EMAIL_TO_RECIPIENTS'),
                'cc': AlertConfig._parse_email_list('PROMINENCE_EMAIL_CC_RECIPIENTS')
            },
            'seatraders.com': {
                'to': AlertConfig._parse_email_list('SEATRADERS_EMAIL_TO_RECIPIENTS'),
                'cc': AlertConfig._parse_email_list('SEATRADERS_EMAIL_CC_RECIPIENTS')
            }
        }

    def validate(self) -> None:
        """
        Validate that all required configuration is present and consistent.

        Raises:
            ValueError: If required configuration is missing or inconsistent.
        """
        required = {
            'SMTP_HOST': self.smtp_host,
            'SMTP_USER': self.smtp_user,
            'SMTP_PASS': self.smtp_pass,
        }

        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(
                f"Required configuration missing from .env: {', '.join(missing)}"
            )

        # Validate scheduling configuration
        if not self.schedule_frequency_hours and not self.schedule_times:
            raise ValueError(
                "Either SCHEDULE_FREQUENCY_HOURS or SCHEDULE_TIMES must be set."
            )

        logger.info("[OK] Configuration validation passed")
