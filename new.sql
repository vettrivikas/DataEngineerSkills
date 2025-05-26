SELECT
    o.id,
    o.s4_currency AS old_currency, n.s4_currency AS new_currency,
    o.s4_pfty_cntr AS old_cntr, n.s4_pfty_cntr AS new_cntr,
    o.ftp_component AS old_component, n.ftp_component AS new_component,
    o.ftp_bal_amount AS old_amount, n.ftp_bal_amount AS new_amount
FROM old o
JOIN new n ON o.id = n.id
WHERE 
    o.s4_currency IS DISTINCT FROM n.s4_currency OR
    o.s4_pfty_cntr IS DISTINCT FROM n.s4_pfty_cntr OR
    o.ftp_component IS DISTINCT FROM n.ftp_component OR
    o.ftp_bal_amount IS DISTINCT FROM n.ftp_bal_amount;


CREATE OR REPLACE FUNCTION get_user_age(user_id INT)
RETURNS INT
IMMUTABLE
AS $$
  SELECT age
  FROM users
  WHERE id = user_id;
$$ LANGUAGE SQL;

