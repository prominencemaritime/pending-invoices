SELECT
	d.name AS department,
	d.email AS primary_email,
	dse.email AS secondary_email
FROM
	departments d
LEFT JOIN
	department_secondary_emails dse
    ON dse.department_id = d.id
WHERE 
    d.email IS NOT NULL
    --AND LOWER(d.name) = LOWER(:department_name)
