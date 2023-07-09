DROP SCHEMA tugas_akhir_4 CASCADE;

CREATE SCHEMA IF NOT EXISTS tugas_akhir_4;

SELECT * FROM create_interval_bitemporal_table(
  'tugas_akhir_4.staff',
  $$
    staff_id INT NOT NULL,
    staff_name TEXT NOT NULL,
    staff_location TEXT NOT NULL
  $$,
  'staff_id'
);

CREATE SEQUENCE IF NOT EXISTS tugas_akhir_4.staff_id_seq;

SELECT * FROM bitemporal_insert(
  'tugas_akhir_4.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('tugas_akhir_4.staff_id_seq')) ||
  $$,
    'Adi',
    'Jl. Ganesha No.10'
  $$
);

SELECT * FROM bitemporal_insert(
  'tugas_akhir_4.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('tugas_akhir_4.staff_id_seq')) ||
  $$,
    'Budi',
    'Jl. Let. Jend. Purn. Dr. (HC) Mashudi No.1'
  $$
);

SELECT * FROM bitemporal_insert(
  'tugas_akhir_4.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('tugas_akhir_4.staff_id_seq')) ||
  $$,
    'Caca',
    'Jl. H. R. Rasuna Said No.Kav. 1'
  $$
);

SELECT * FROM bitemporal_update(
  'tugas_akhir_4.staff',
  'staff_location',
  $$ 'Jl. Tamansari No.64' $$,
  'staff_name',
  $$ 'Adi' $$
);

SELECT * FROM bitemporal_correction(
  'tugas_akhir_4.staff',
  'staff_location',
  $$ 'Jl. Ganesa No.7' $$,
  'staff_name',
  $$ 'Budi' $$
);

SELECT * FROM bitemporal_inactivate(
  'tugas_akhir_4.staff',
  'staff_name',
  $$ 'Caca' $$
);

SELECT * FROM bitemporal_delete(
  'tugas_akhir_4.staff',
  'staff_name',
  $$ 'Adi' $$
);

SELECT * FROM bitemporal_correction_valid(
  'tugas_akhir_4.staff',
  'staff_name',
  $$ 'Budi' $$
);

SELECT * FROM create_event_bitemporal_table(
  'tugas_akhir_4.tes',
  $$
    id INT NOT NULL,
    tes TEXT NOT NULL
  $$,
  'id'
);

CALL bitemporal_fk_constraint(
  'tugas_akhir_4.staff',
  'staff_id',
  'tugas_akhir_4.tes',
  'id'
);

SELECT * FROM bitemporal_insert(
  'tugas_akhir_4.tes',
  $$
    id,
    tes
  $$,
  $$
    2,
    'tes'
  $$
);

-- SELECT * FROM bitemporal_delete(
--   'tugas_akhir_4.tes',
--   'staff_id',
--   $$ 2 $$,
--   timestamptz('2023-09-01')
-- );

-- SELECT * FROM bitemporal_delete(
--   'tugas_akhir_4.staff',
--   'staff_id',
--   $$ 2 $$
-- );
