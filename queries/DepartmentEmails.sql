SELECT
	d.name,
	d.email as primary_email,
	dse.email as secondary_email
FROM
	departments d
LEFT JOIN
	department_secondary_emails dse
ON dse.department_id = d.id
WHERE 
    d.email IS NOT NULL
    AND LOWER(d.name) = LOWER(:department_name)
