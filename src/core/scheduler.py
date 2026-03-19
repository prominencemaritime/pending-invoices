#src/core/scheduler.py
"""
Scheduling system for running alerts at regular intervals or at specific times.

Uses APScheduler for robust scheduling with timezone support, missed job
recovery, and graceful shutdown.
"""
import logging
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Optional
from src.formatters.date_formatter import duration_hours

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class AlertScheduler:
    """
    Scheduler for running alerts at regular intervals or at specific times/days.

    Supports:
      - Interval-based scheduling (every N hours)
      - Time+day-based scheduling (cron-style, specific weekdays and times)
      - Graceful shutdown via SIGTERM/SIGINT
      - Missed job recovery (fires once on restart if job was missed)
      - Concurrent alert execution via thread pool
    """

    def __init__(
        self,
        timezone: str,
        frequency_hours: Optional[float] = None,
        schedule_times: Optional[List[str]] = None,
        schedule_days: Optional[List[int]] = None,
        schedule_times_timezone: Optional[str] = None,
        max_workers: int = 4,
    ):
        """
        Initialise the scheduler.

        Args:
            timezone:                General timezone for logging and display.
            frequency_hours:         If set, run every N hours (interval mode).
            schedule_times:          List of 'HH:MM' strings for time-based mode.
            schedule_days:           ISO weekday ints (1=Mon..7=Sun). Defaults to
                                     every day if not set.
            schedule_times_timezone: Timezone for interpreting schedule_times.
                                     Defaults to `timezone` if not provided.
            max_workers:             Thread pool size for concurrent alert execution.
        """
        self.timezone = ZoneInfo(timezone)
        self.frequency_hours = frequency_hours
        self.schedule_times = schedule_times
        self.schedule_days = schedule_days if schedule_days else list(range(1, 8))
        self.schedule_times_timezone = ZoneInfo(
            schedule_times_timezone if schedule_times_timezone else timezone
        )
        self._alerts: List[Callable] = []

        executors = {
            'default': APSThreadPoolExecutor(max_workers=max_workers),
        }
        job_defaults = {
            # If a job is still running when the next fire time arrives,
            # allow it to run concurrently rather than skipping.
            'coalesce': True,           # Collapse multiple missed firings into one
            'max_instances': max_workers,
            'misfire_grace_time': None, # Always run even if late
        }
        self._scheduler = BlockingScheduler(
            executors=executors,
            job_defaults=job_defaults,
            timezone=self.schedule_times_timezone,
        )

        # Register signal handlers so SIGTERM/SIGINT shut down APScheduler cleanly
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_alert(self, alert_runner: Callable) -> None:
        """
        Register an alert callable to be run on schedule.

        Args:
            alert_runner: Callable that executes the alert (typically alert.run()).
        """
        self._alerts.append(alert_runner)
        name = getattr(alert_runner, '__name__', 'anonymous')
        logger.info(f"Registered alert: {name}")

    def run_once(self) -> None:
        """
        Run all alerts once and exit.

        Useful for manual execution or testing.
        """
        logger.info("=" * 60)
        logger.info("▶ RUN-ONCE MODE: Executing alerts once without scheduling")
        logger.info("=" * 60)

        self._run_all_alerts()

        logger.info("=" * 60)
        logger.info("◼ RUN-ONCE COMPLETE")
        logger.info("=" * 60)

    def run_continuous(self) -> None:
        """
        Run alerts continuously using interval-based or time-based scheduling.

        Mode is determined by whether frequency_hours is set:
          - frequency_hours set  → IntervalTrigger (every N hours)
          - frequency_hours None → CronTrigger     (specific times and weekdays)

        In both modes, a missed firing (e.g. process was down) will be executed
        once immediately on restart due to coalesce=True + misfire_grace_time=None.
        """
        if self.frequency_hours:
            self._run_interval_mode()
        else:
            self._run_time_based_mode()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_interval_mode(self) -> None:
        """Configure and start the scheduler in interval mode."""
        if not self.frequency_hours:
            raise ValueError("frequency_hours must be set for interval mode.")

        logger.info("=" * 60)
        logger.info("▶ INTERVAL SCHEDULER STARTED")
        logger.info(f"Frequency:  Every {duration_hours(self.frequency_hours)}")
        logger.info(f"Timezone:   {self.timezone}")
        logger.info(f"Registered alerts: {len(self._alerts)}")
        logger.info("=" * 60)

        self._scheduler.add_job(
            func=self._run_all_alerts,
            trigger=IntervalTrigger(
                hours=self.frequency_hours,
                timezone=self.timezone,
            ),
            id='interval_job',
            name='AlertScheduler interval job',
            replace_existing=True,
        )

        self._start()

    def _run_time_based_mode(self) -> None:
        """Configure and start the scheduler in time+day (cron) mode."""
        if not self.schedule_times:
            raise ValueError("schedule_times must be set for time-based scheduling.")

        # Convert ISO weekday list (1=Mon..7=Sun) to APScheduler dow (0=Mon..6=Sun)
        aps_days = ','.join(str(d - 1) for d in self.schedule_days)

        # Parse HH:MM strings
        parsed_times = []
        for time_str in self.schedule_times:
            hour, minute = map(int, time_str.split(':'))
            parsed_times.append((hour, minute))

        days_display = (
            ', '.join(str(d) for d in self.schedule_days)
            if self.schedule_days != list(range(1, 8))
            else 'every day'
        )
        logger.info("=" * 60)
        logger.info("▶ TIME-BASED SCHEDULER STARTED")
        logger.info(f"Run times:  {', '.join(self.schedule_times)}")
        logger.info(f"Days:       {days_display}")
        logger.info(f"Timezone:   {self.schedule_times_timezone}")
        logger.info(f"Registered alerts: {len(self._alerts)}")
        logger.info("=" * 60)

        # Register one cron job per scheduled time
        for idx, (hour, minute) in enumerate(parsed_times):
            self._scheduler.add_job(
                func=self._run_all_alerts,
                trigger=CronTrigger(
                    day_of_week=aps_days,
                    hour=hour,
                    minute=minute,
                    timezone=self.schedule_times_timezone,
                ),
                id=f'cron_job_{idx}',
                name=f'AlertScheduler cron job {hour:02d}:{minute:02d}',
                replace_existing=True,
            )

        self._start()

    def _start(self) -> None:
        """Start the APScheduler blocking scheduler with graceful shutdown."""
        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutdown signal received.")
        finally:
            self._stop()

    def _stop(self) -> None:
        """Shut down the APScheduler instance cleanly."""
        if self._scheduler.running:
            logger.info("Shutting down scheduler (waiting for running jobs)...")
            self._scheduler.shutdown(wait=True)
            logger.info("=" * 60)
            logger.info("⏹ SCHEDULER STOPPED")
            logger.info("=" * 60)

    def _signal_handler(self, signum, frame) -> None:
        """Handle SIGTERM/SIGINT by shutting down the scheduler."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self._stop()

    def _run_all_alerts(self) -> None:
        """Execute all registered alerts concurrently via a thread pool."""
        if not self._alerts:
            logger.warning("No alerts registered. Nothing to run.")
            return

        logger.info(f"Running {len(self._alerts)} alert(s)...")

        with ThreadPoolExecutor(max_workers=len(self._alerts)) as executor:
            futures = {
                executor.submit(self._run_single_alert, idx, alert_runner): idx
                for idx, alert_runner in enumerate(self._alerts, 1)
            }
            for future in futures:
                # Retrieve result to surface any unhandled exceptions in logging
                try:
                    future.result()
                except Exception as e:
                    logger.exception(f"Unhandled exception in alert future: {e}")

    def _run_single_alert(self, idx: int, alert_runner: Callable) -> None:
        """
        Execute a single alert with error isolation.

        Args:
            idx:          1-based index for logging.
            alert_runner: The alert callable.
        """
        try:
            logger.info(f"Executing alert {idx}/{len(self._alerts)}...")
            alert_runner()
        except Exception as e:
            logger.exception(f"Error executing alert {idx}: {e}")
