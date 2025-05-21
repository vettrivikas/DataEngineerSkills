WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id, pipeline_src ORDER BY id) AS rn
    FROM your_table
),
old_rows AS (
    SELECT id, rn, s4_currency AS old_currency, s4_pfty_cntr AS old_cntr,
           ftp_component AS old_component, ftp_bal_amount AS old_amount
    FROM ranked
    WHERE pipeline_src = 'OLD'
),
new_rows AS (
    SELECT id, rn, s4_currency AS new_currency, s4_pfty_cntr AS new_cntr,
           ftp_component AS new_component, ftp_bal_amount AS new_amount
    FROM ranked
    WHERE pipeline_src = 'NEW'
)
SELECT
    o.id, o.rn,
    o.old_currency, n.new_currency,
    o.old_cntr, n.new_cntr,
    o.old_component, n.new_component,
    o.old_amount, n.new_amount
FROM old_rows o
JOIN new_rows n ON o.id = n.id AND o.rn = n.rn
WHERE
    o.old_currency IS DISTINCT FROM n.new_currency OR
    o.old_cntr IS DISTINCT FROM n.new_cntr OR
    o.old_component IS DISTINCT FROM n.new_component OR
    o.old_amount IS DISTINCT FROM n.new_amount;
