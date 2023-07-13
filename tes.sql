CREATE OR REPLACE FUNCTION tes(anyelement)
returns text language plpgsql as $$
BEGIN
  return ('halo');
end
$$;

CREATE OR REPLACE FUNCTION tes(text)
returns text language plpgsql as $$
BEGIN
  return ('mbek');
end
$$;

SELECT * FROM bitemporal_internal.ll_create_period_bitemporal_table(
  'tes',
	$$
    staff_id INT, 
	  staff_name TEXT NOT NULL,
    staff_location TEXT NOT NULL
	$$,
  'staff_id'
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tes',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  1 ||
  $$,
  'mystaff',
  'mylocation'
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_table_type(
  'tes'
);