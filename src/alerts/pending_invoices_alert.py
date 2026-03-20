#src/alerts/pending_invoices_alert.py
"""Pending Invoices Alert Implementation.""" 
from typing import Dict, List, Optional
import pandas as pd 
from datetime import datetime, timedelta 
from zoneinfo import ZoneInfo
from sqlalchemy import text
import logging
 
from src.core.base_alert import BaseAlert 
from src.core.config import AlertConfig 
from src.db_utils import get_db_connection, validate_query_file, query_to_df


logger = logging.getLogger(__name__)


class PendingInvoicesAlert(BaseAlert):
    """Alert for pending invoices"""

    def __init__(self, config: AlertConfig):
        """
        Initialise pending invoices alert
        
        Args:
            config: AlertConfig instance
        """
        super().__init__(config)

        # Load query + lookback
        self.sql_main_query_file = 'PendingInvoices.sql'
        self.sql_department_email_query_file = 'DepartmentEmails.sql'

        # Log instantiation
        self.logger.info("[OK] PendingInvoicesAlert instance created")

        
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch pending invoices from database

        Returns:
            DataFrame with columns: 
                vessel,
                department,
                vendor,
                invoice_no,
                invoice_date,
                invoice_due_date,
                amount_usd,
                day_count
        """
        # Fetch SQL queries
        main_query_path = self.config.queries_dir / self.sql_main_query_file
        emails_query_path = self.config.queries_dir / self.sql_department_email_query_file
        main_query_sql = validate_query_file(main_query_path)
        emails_query_sql = validate_query_file(emails_query_path)

        # Convert query to sqlalchemy format
        main_query = text(main_query_sql)
        email_query = text(emails_query_sql)

        # Connect to db and execute queries
        with get_db_connection() as conn:
            df = pd.read_sql_query(main_query, conn)#, params=params)
            emails_df = pd.read_sql_query(email_query, conn)

        # Merge emails (extracted from departments) into df
        df['_dept_key'] = df['department'].str.lower()
        emails_df['_dept_key'] = emails_df['department'].str.lower()
        df = df.merge(
            emails_df[['_dept_key', 'primary_email', 'secondary_email']],
            on='_dept_key',
            how='left'
        ).drop(columns='_dept_key')
        
        self.logger.info(f"PendingInvoicesAlert.fetch_data() is returning a df with {len(df)} rows")
        return df


    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for entries synced in the last lookback_days
    
        Args:
            df: Raw pd.DataFrame from database
                cols:
                    vessel,
                    department,
                    vendor,
                    invoice_no,
                    invoice_date,
                    invoice_due_date,
                    amount_usd,
                    day_count

        Returns:
            Filtered pd.DataFrame with only recently udpated entries

        Note: this filter preserves the number of columns - which columns are going to be displayed is specified in formatter
        """
        if df.empty:
            return df

        # Timezone awareness
        df['invoice_date'] = pd.to_datetime(df['invoice_date'])
        df['invoice_due_date'] = pd.to_datetime(df['invoice_due_date'])

        # If the datetime is timezone-naive, localise it to UTC first, then convert to timezone specified in .env. I am assuming all times appearing are UTC, and then converting to TIMEZONE='Europe/Athens' will automatically be correct during Winter (UTC+2) and Summer (UTC+3).

        if df['invoice_date'].dt.tz is None:
            df['invoice_date'] = df['invoice_date'].dt.tz_localize('UTC').dt.tz_convert(self.config.timezone)
        else:
            # If already timezone-aware, convert to timezone specified in .env
            df['invoice_date'] = df['invoice_date'].dt.tz_convert(self.config.timezone)

        # repeat for due date
        if df['invoice_due_date'].dt.tz is None:
            df['invoice_due_date'] = df['invoice_due_date'].dt.tz_localize('UTC').dt.tz_convert(self.config.timezone)
        else:
            # If already timezone-aware, convert to timezone specified in .env
            df['invoice_due_date'] = df['invoice_due_date'].dt.tz_convert(self.config.timezone)

        # Filter for invoices due in less than 31 days
        df_filtered = df[df['day_count'] <= 30].copy()

        # Include a priority column: RED & ORANGE definition
        df_filtered['priority'] = None
        df_filtered.loc[df_filtered['day_count'] <= 0, 'priority'] = 'red'
        df_filtered.loc[(df_filtered['day_count'] > 0) & (df_filtered['day_count'] <= 30), 'priority'] = 'orange'

        self.logger.info(f"Filtered to {len(df_filtered)} entr{'y' if len(df_filtered)==1 else 'ies'}")
        return df_filtered


    def _get_url_links(self, invoice_no: int) -> Optional[str]:
        """
        Generate URL if links are enabled.

        Constructs URL by combining:
            - BASE_URL from config (e.g. https://prominence.orca.tools)
            - URL_PATH from config (e.g. /invoices)
            - invoice_no from database (e.g. 123)
        Result: https://prominence.orca.tools/invoices/123

        Args:
            invoice_no: in PendingInvoices project, given by
                public_reporting.fct_invoicing__per_ref_code.invoice_no = invoice_no

        Returns:
            Complete URL, or None if links are disabled
        """
        if not self.config.enable_links:
            return None

        # Build URL: BASE_URL + URL_PATH + link_id
        base_url = self.config.base_url.rstrip('/')
        url_path = self.config.url_path.rstrip('/')
        full_url = f"{base_url}{url_path}/{invoice_no}"

        return full_url


    def route_notifications(self, df:pd.DataFrame) -> List[Dict]:
        """
        Route data to appropriate recipients.

        Returns list of notification jobs, where each job is a dict with:
        - 'recipients': List[str] - primary email addresses
        - 'cc_recipients': List[str] - CC email addresses
        - 'data': pd.DataFrame - data for this specific notification
        - 'metadata': Dict - any additional info (vessel name, etc.)

        Args:
            df: Filtered DataFrame

        Returns:
            List of notification job dictionaries
        """
        jobs = []

        # Group by vessel
        grouped = df.groupby(['department', 'vsl_email'])

        for (vessel_name, vessel_email), vessel_df in grouped:
            # Determine cc recipients
            cc_recipients = self._get_cc_recipients(vessel_email)

            # Add URLs to dataframe if ENABLE_LINKS
            if self.config.enable_links:
                vessel_df = vessel_df.copy()
                vessel_df['url'] = vessel_df['event_id'].apply(
                        self._get_url_links
                )

            # Keep full data with tracking columns for the job
            # The formatter will handle which columns to display
            full_data = vessel_df.copy()

            # Specify WHICH cols to display in email and in what order here
            display_columns = [
                    'vessel',
                    'department',
                    'vendor',
                    'invoice_no',
                    'invoice_date',
                    'invoice_due_date',
                    'amount_usd'
            ]


            # Create notification job
            job = {
                    'recipients': [vessel_email],
                    'cc_recipients': cc_recipients,
                    'data': full_data,
                    'metadata': {
                        'vessel_name': vessel_name,
                        'alert_title': 'Passage Plan',
                        'company_name': self._get_company_name(vessel_email),
                        'display_columns': display_columns
                    }
            }

            jobs.append(job)

            self.logger.info(
                    f"Created notification for vessel '{vessel_name}' "
                    f"({len(full_data)} document{'' if len(full_data)==1 else 's'}) -> {vessel_email} "
                    f"(CC: {len(cc_recipients)})"
            )

        return jobs


    def _get_cc_recipients(self, vessel_email: str) -> List[str]:
        """
        Determine CC recipients based on vessel email domain.
        Always includes internal recipients.

        Args:
            vessel_email: Vessel's email address

        Returns:
            List of CC email addresses (domain-specific + internal)
        """
        vessel_email_lower = vessel_email.lower()

        # Start with empty list
        cc_list = []

        # Check each configured domain
        entry = 0
        total_entries = len(self.config.email_routing.items())
        for domain, recipients_config in self.config.email_routing.items():
            entry += 1
            if domain.lower() in vessel_email_lower:
                cc_list = recipients_config.get('cc', [])
                break
            else:
                self.logger.info(f"Entry {entry}/{total_entries}: No domain match for vessel_email={vessel_email} (only including internal CC recipients)")

        # Always add internal recipients to CC list
        all_cc_recipients = list(set(cc_list + self.config.internal_recipients))

        return all_cc_recipients


    def _get_company_name(self, vessel_email: str) -> str:
        """
        Determine company name based on vessel email domain.
        
        Args:
            vessel_email: Vessel's email address
            
        Returns:
            Company name string
        """
        vessel_email_lower = vessel_email.lower()
        
        if 'prominence' in vessel_email_lower:
            return 'Prominence Maritime S.A.'
        elif 'seatraders' in vessel_email_lower:
            return 'Sea Traders S.A.'
        else:
            return 'Prominence Maritime S.A.'   # Default company name


    def get_tracking_key(self, row:pd.Series) -> str:
        """
        Generate unique tracking key for a data row.

        This key is used to prevent duplicate notifications.

        Args:
            row: Single row from DataFrame

        Returns:
            Unique string key (e.g., "vessel_123_doc_456")
        """
        try:
            vessel_id = row['vessel_id']
            event_type_id = row['event_type_id']
            event_id = row['event_id']

            return f"vessel_id_{vessel_id}__event_type_{event_type_id}__event_id_{event_id}"

        except KeyError as e:
            self.logger.error(f"Missing column in row for tracking key: {e}")
            self.logger.error(f"Available columns: {list(row.index)}")
            raise


    def get_subject_line(self, data: pd.DataFrame, metadata: Dict) -> str:
        """
        Generate email subject line for a notification.

        Args:
            data: DataFrame for this notification
            metadata: Additional context (vessel name, etc.)

        Returns:
            Email subject string
        """
        vessel_name = metadata.get('vessel_name', 'Vessel')
        return f"AlertDev | {data['department']}  | {len(data)} Pending Invoices"


    def get_required_columns(self) -> List[str]:
        """
        Return list of column names required in the DataFrame

        Returns:
            List of required column names
        """
        return [
            'vessel',
            'department',
            'vendor',
            'invoice_no',
            'invoice_date',
            'invoice_due_date',
            'amount_usd',
            'day_count'
        ]


    def get_required_columns(self) -> List[str]:
        """
        Return list of column names required in the DataFrame.

        Returns:
            List of required column names
        """
        return [
            'vsl_email',
            'vessel_id',
            'event_type_id',
            'event_id',
            'event_name',
            'created_at',
            'synced_at',
            'status'
        ]


"""
df_filtered.columns:
    vsl_email, vessel_id,   <- groupby
    event_type_id, event_type_name, vessel_name, status_id,     <- extra stuff
    event_id, event_name, created_at, synced_at, status     <- display stuff
"""

