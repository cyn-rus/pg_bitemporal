DROP SCHEMA coalesce_tes CASCADE;

CREATE SCHEMA IF NOT EXISTS coalesce_tes;

SELECT * FROM create_interval_bitemporal_table(
  'coalesce_tes.staff',
  $$
    staff_id INT NOT NULL,
    staff_name TEXT NOT NULL,
    staff_location TEXT NOT NULL
  $$,
  'staff_id'
);

CREATE SEQUENCE IF NOT EXISTS coalesce_tes.staff_id_seq;

SELECT * FROM bitemporal_insert(
  'coalesce_tes.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('coalesce_tes.staff_id_seq')) ||
  $$,
    'Adi',
    'Jl. Ganesha No.10'
  $$,
  timeperiod('2023-07-13', 'infinity'),
  timeperiod('2023-07-13', 'infinity')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('coalesce_tes.staff_id_seq')) ||
  $$,
    'Budi',
    'Jl. Let. Jend. Purn. Dr. (HC) Mashudi No.1'
  $$,
  timeperiod('2023-07-13', 'infinity'),
  timeperiod('2023-07-13', 'infinity')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('coalesce_tes.staff_id_seq')) ||
  $$,
    'Caca',
    'Jl. H. R. Rasuna Said No.Kav. 1'
  $$,
  timeperiod('2023-07-13', 'infinity'),
  timeperiod('2023-07-13', 'infinity')
);

SELECT * FROM bitemporal_update(
  'coalesce_tes.staff',
  'staff_location',
  $$ 'Jl. Tamansari No.64' $$,
  'staff_name',
  $$ 'Adi' $$,
  timeperiod('2023-07-14', 'infinity'),
  timeperiod('2023-07-14', 'infinity')
);

SELECT * FROM bitemporal_correction(
  'coalesce_tes.staff',
  'staff_location',
  $$ 'Jl. Ganesa No.7' $$,
  'staff_name',
  $$ 'Budi' $$,
  timestamptz('2023-07-14')
);

SELECT * FROM bitemporal_inactivate(
  'coalesce_tes.staff',
  'staff_name',
  $$ 'Caca' $$,
  timeperiod('2023-07-14', 'infinity'),
  timeperiod('2023-07-14', 'infinity')
);

SELECT * FROM bitemporal_delete(
  'coalesce_tes.staff',
  'staff_name',
  $$ 'Adi' $$,
  timestamptz('2023-07-15')
);

SELECT * FROM bitemporal_correction_valid(
  'coalesce_tes.staff',
  'staff_name',
  $$ 'Budi' $$,
  timeperiod('2023-07-15', 'infinity'),
  timestamptz('2023-07-15')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  $$
    1,
    'Adi',
    'Jl. Tamansari No.64'
  $$,
  timeperiod('2023-07-14', 'infinity'),
  timeperiod('2023-07-15', 'infinity')
);

SELECT * FROM bitemporal_delete(
  'coalesce_tes.staff',
  'staff_name',
  $$ 'Adi' $$,
  timestamptz('2023-07-30')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  $$
    1,
    'Adi',
    'Jl. Tamansari No.64'
  $$,
  timeperiod('2023-07-14', 'infinity'),
  timeperiod('2023-07-30', 'infinity')
);

