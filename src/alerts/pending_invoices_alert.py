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
        Fetch pending invoices and enrich with department-level email data.

        This method executes two SQL queries:
            1. Main query: retrieves pending invoice records
            2. Email query: retrieves department → (primary, secondary) email mappings

        The two datasets are merged via a case-insensitive join on department.

        Returns:
            pd.DataFrame with columns:

            Core invoice data:
                - ref: str
                - vessel: str
                - department: str
                - vendor: str
                - invoice_no: int
                - invoice_date: datetime (timezone-naive or aware, depending on DB)
                - invoice_due_date: datetime
                - amount_usd: float
                - day_count: int
                - primary_email: Optional[str]
                - secondary_email: Optional[str]

        Notes:
            - The join is a LEFT JOIN on department (case-insensitive), so all invoice
              rows are preserved even if no email mapping exists.
            - Email fields are not yet used for routing at this stage; they are passed
              downstream for use in `route_notifications()`.
            - No filtering is applied here; all filtering is handled in `filter_data()`.

        Logging:
            Logs the number of rows returned after merging.
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
                    primary_email: Optional[str]
                    secondary_email: Optional[str]

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
        df_filtered.loc[df_filtered['day_count'] <= 0, 'priority'] = 'OVERDUE'
        df_filtered.loc[(df_filtered['day_count'] > 0) & (df_filtered['day_count'] <= 30), 'priority'] = 'SOON DUE'

        self.logger.info(f"Filtered to {len(df_filtered)} entr{'y' if len(df_filtered)==1 else 'ies'}")

        df_filtered['invoice_date'] = df_filtered['invoice_date'].dt.strftime('%Y-%m-%d')
        df_filtered['invoice_due_date'] = df_filtered['invoice_due_date'].dt.strftime('%Y-%m-%d')
        return df_filtered


    def _get_url_links(self, ref: str) -> Optional[str]:
        """
        Generate URL if links are enabled.

        Constructs URL by combining:
            - BASE_URL from config (e.g. https://prominence.orca.tools)
            - URL_PATH from config (e.g. /invoices)
            - ref=invoice_ref_code from database (e.g. 123)
        Result: https://prominence.orca.tools/invoices/123

        Args:
            ref: in PendingInvoices project, given by
                public_reporting.fct_invoicing__per_ref_code.invoice_ref_code = ref

        Returns:
            Complete URL, or None if links are disabled
        """
        if not self.config.enable_links:
            return None

        # extract the url ref, e.g. 1504-2026 -> 1504
        ref = ref.split('-')[0]

        # Build URL: BASE_URL + URL_PATH + link_id
        base_url = self.config.base_url.rstrip('/')
        url_path = self.config.url_path.rstrip('/')
        full_url = f"{base_url}{url_path}/{ref}"

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
                Expected column names:
                    ref,
                    vessel,
                    department,
                    vendor,
                    invoice_no,
                    invoice_date,
                    invoice_due_date,
                    amount_usd,
                    day_count
                    primary_email: Optional[str]
                    secondary_email: Optional[str]

        Returns:
            List of notification job dictionaries
        """
        jobs = []

        # Group by vessel
        grouped = df.groupby('department', dropna=False)

        for department_name, dept_df in grouped:
            # Determine cc recipients
            primary_email = dept_df['primary_email'].iloc[0]
            secondary_email = dept_df['secondary_email'].iloc[0] if 'secondary_email' in dept_df.columns else None

            # Skip departments with no email configured
            if pd.isna(primary_email) or not primary_email:
                self.logger.warning(f"No primary email for department '{department_name}', skipping")
                continue

            # Build to recipients: primary + secondary (if present)
            to_recipients = [primary_email]
            if secondary_email and not pd.isna(secondary_email):
                to_recipients.append(secondary_email)

            # CC recipients: fixed internal list from config
            cc_recipients = self.config.internal_recipients.copy()

            # URL
            dept_df = dept_df.copy()
            dept_df['url'] = dept_df['ref'].apply(self._get_url_links)

            # Keep full data with tracking columns for the job
            # The formatter will handle which columns to display
            full_data = dept_df.copy()

            # Specify WHICH cols to display in email and in what order here
            display_columns = [
                    'priority',
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
                    'recipients': to_recipients,
                    'cc_recipients': cc_recipients,
                    'data': full_data,
                    'metadata': {
                        'alert_title': 'Pending Invoices',
                        'department_name': department_name,
                        'company_name': 'Prominence Maritime S.A.',
                        'display_columns': display_columns
                    }
            }

            jobs.append(job)

            self.logger.info(
                    f"Created notification for department '{department_name}' "
                    f"({len(full_data)} invoice{'' if len(full_data)==1 else 's'}) -> {to_recipients} "
                    f"(CC: {len(cc_recipients)})"
            )

        return jobs


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
            department = row['department']
            invoice_no = row['invoice_no']
            return f"department__{department}__invoice_no__{invoice_no}"
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
        department_name = metadata.get('department_name', 'Department')
        return f"AlertDev | {department_name} | {len(data)} Pending Invoice{'s' if len(data) != 1 else ''}"


    def get_required_columns(self) -> List[str]:
        """
        Return list of column names required in the DataFrame

        Returns:
            List of required column names
        """
        return [
            'ref',
            'vessel',
            'department',
            'vendor',
            'invoice_no',
            'invoice_date',
            'invoice_due_date',
            'amount_usd',
            'day_count'
        ]
