# scratch_test_scheduler.py  (place in project root)
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

from src.core.scheduler import AlertScheduler

def mock_alert_1():
    print("Alert 1 fired")

def mock_alert_2():
    print("Alert 2 fired")

scheduler = AlertScheduler(
    timezone='Europe/Athens',
    schedule_times=['09:00', '17:00'],
    schedule_days=[1, 2, 3, 4, 5],
    schedule_times_timezone='Europe/Athens',
)

scheduler.register_alert(mock_alert_1)
scheduler.register_alert(mock_alert_2)

# Test run_once — safe, no APScheduler involvement
scheduler.run_once()


# Test run_continuous in time-based mode
# Set a time ~1-2 minutes ahead, then Ctrl+C after it fires
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo('Europe/Athens')
now = datetime.now(tz=tz)
fire_at = now + timedelta(minutes=2)
fire_time_str = fire_at.strftime('%H:%M')

print(f"Scheduling cron job to fire at {fire_time_str}")

scheduler2 = AlertScheduler(
    timezone='Europe/Athens',
    schedule_times=[fire_time_str],
    schedule_times_timezone='Europe/Athens',
)
scheduler2.register_alert(mock_alert_1)
scheduler2.register_alert(mock_alert_2)
scheduler2.run_continuous()  # Ctrl+C after it fires to verify shutdown
