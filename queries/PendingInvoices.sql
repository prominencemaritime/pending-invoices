SELECT
    invoice_ref_code as ref,
	invoice_vessel_company as vessel,
	invoice_department_name as department,
	RTRIM(invoice_vendor_name, ' *') as vendor,
	invoice_no,
    invoice_created_at_ts::date,
	invoice_date::date,
	invoice_due_date::date,
	invoice_amount_base_currency_equivalent as amount_usd,
	(invoice_due_date::date - NOW()::date) as day_count
FROM
	public_reporting.fct_invoicing__per_ref_code
WHERE
    invoice_good_for_payment = 'No'
ORDER BY invoice_due_date ASC;
