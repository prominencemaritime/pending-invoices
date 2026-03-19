# tests/test_scheduler.py
"""
Tests for AlertScheduler.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.core.scheduler import AlertScheduler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def scheduler():
    """Basic scheduler with no frequency (time-based mode)."""
    return AlertScheduler(
        timezone='Europe/Athens',
        schedule_times=['09:00', '17:00'],
        schedule_days=[1, 2, 3, 4, 5],
        schedule_times_timezone='Europe/Athens',
    )


@pytest.fixture
def interval_scheduler():
    """Scheduler in interval mode."""
    return AlertScheduler(
        timezone='Europe/Athens',
        frequency_hours=6.0,
    )


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInitialisation:

    def test_timezone_is_set(self, scheduler):
        assert str(scheduler.timezone) == 'Europe/Athens'

    def test_schedule_times_stored(self, scheduler):
        assert scheduler.schedule_times == ['09:00', '17:00']

    def test_schedule_days_stored(self, scheduler):
        assert scheduler.schedule_days == [1, 2, 3, 4, 5]

    def test_schedule_days_defaults_to_every_day_when_not_provided(self):
        s = AlertScheduler(
            timezone='Europe/Athens',
            schedule_times=['09:00'],
        )
        assert s.schedule_days == list(range(1, 8))

    def test_frequency_hours_stored(self, interval_scheduler):
        assert interval_scheduler.frequency_hours == 6.0

    def test_no_alerts_on_init(self, scheduler):
        assert len(scheduler._alerts) == 0

    def test_schedule_times_timezone_defaults_to_timezone_when_not_provided(self):
        s = AlertScheduler(
            timezone='Europe/London',
            schedule_times=['09:00'],
        )
        assert str(s.schedule_times_timezone) == 'Europe/London'


# ---------------------------------------------------------------------------
# Alert registration
# ---------------------------------------------------------------------------

class TestAlertRegistration:

    def test_register_single_alert(self, scheduler):
        mock_alert = Mock()
        scheduler.register_alert(mock_alert)
        assert len(scheduler._alerts) == 1

    def test_register_multiple_alerts(self, scheduler):
        scheduler.register_alert(Mock())
        scheduler.register_alert(Mock())
        assert len(scheduler._alerts) == 2


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------

class TestRunOnce:

    def test_run_once_executes_all_alerts(self, scheduler):
        mock1, mock2 = Mock(), Mock()
        scheduler.register_alert(mock1)
        scheduler.register_alert(mock2)
        scheduler.run_once()
        mock1.assert_called_once()
        mock2.assert_called_once()

    def test_run_once_continues_after_alert_failure(self, scheduler):
        failing = Mock(side_effect=Exception("boom"))
        successful = Mock()
        scheduler.register_alert(failing)
        scheduler.register_alert(successful)
        scheduler.run_once()
        failing.assert_called_once()
        successful.assert_called_once()

    def test_run_once_with_no_alerts_does_not_raise(self, scheduler):
        scheduler.run_once()  # should not raise


# ---------------------------------------------------------------------------
# _run_all_alerts (concurrent execution)
# ---------------------------------------------------------------------------

class TestRunAllAlerts:

    def test_all_alerts_are_called(self, scheduler):
        mocks = [Mock() for _ in range(3)]
        for m in mocks:
            scheduler.register_alert(m)
        scheduler._run_all_alerts()
        for m in mocks:
            m.assert_called_once()

    def test_exception_in_one_alert_does_not_prevent_others(self, scheduler):
        failing = Mock(side_effect=RuntimeError("fail"))
        good = Mock()
        scheduler.register_alert(failing)
        scheduler.register_alert(good)
        scheduler._run_all_alerts()  # should not raise
        good.assert_called_once()

    def test_no_alerts_logs_warning_and_does_not_raise(self, scheduler):
        scheduler._run_all_alerts()  # should not raise


# ---------------------------------------------------------------------------
# run_continuous — mode selection
# ---------------------------------------------------------------------------

class TestRunContinuousModSelection:

    def test_run_continuous_calls_interval_mode_when_frequency_set(self, interval_scheduler):
        with patch.object(interval_scheduler, '_run_interval_mode') as mock_interval:
            interval_scheduler.run_continuous()
            mock_interval.assert_called_once()

    def test_run_continuous_calls_time_based_mode_when_no_frequency(self, scheduler):
        with patch.object(scheduler, '_run_time_based_mode') as mock_time:
            scheduler.run_continuous()
            mock_time.assert_called_once()


# ---------------------------------------------------------------------------
# _run_time_based_mode — validation
# ---------------------------------------------------------------------------

class TestTimedBasedModeValidation:

    def test_raises_if_schedule_times_not_set(self):
        s = AlertScheduler(timezone='Europe/Athens')
        # schedule_times is None, schedule_days defaults to all days
        with pytest.raises(ValueError, match="schedule_times must be set"):
            s._run_time_based_mode()


# ---------------------------------------------------------------------------
# _run_interval_mode — validation
# ---------------------------------------------------------------------------

class TestIntervalModeValidation:

    def test_raises_if_frequency_hours_not_set(self):
        s = AlertScheduler(
            timezone='Europe/Athens',
            schedule_times=['09:00'],
        )
        with pytest.raises(ValueError, match="frequency_hours must be set"):
            s._run_interval_mode()


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

class TestGracefulShutdown:

    def test_signal_handler_calls_stop(self, scheduler):
        with patch.object(scheduler, '_stop') as mock_stop:
            scheduler._signal_handler(15, None)
            mock_stop.assert_called_once()

    def test_stop_calls_scheduler_shutdown_when_running(self, scheduler):
        scheduler._scheduler = MagicMock()
        scheduler._scheduler.running = True
        scheduler._stop()
        scheduler._scheduler.shutdown.assert_called_once_with(wait=True)

    def test_stop_does_not_call_shutdown_when_not_running(self, scheduler):
        scheduler._scheduler = MagicMock()
        scheduler._scheduler.running = False
        scheduler._stop()
        scheduler._scheduler.shutdown.assert_not_called()
