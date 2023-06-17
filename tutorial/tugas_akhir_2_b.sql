CREATE SCHEMA IF NOT EXISTS tugas_akhir_2_b;

SELECT * FROM bitemporal_internal.ll_create_interval_bitemporal_table(
  'tugas_akhir_2_b.staff',
  $$
    staff_id INT NOT NULL,
    staff_name TEXT NOT NULL,
    staff_location TEXT NOT NULL
  $$,
  'staff_id'
);

CREATE SEQUENCE IF NOT EXISTS tugas_akhir_2_b.staff_id_seq;

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_b.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('tugas_akhir_2_b.staff_id_seq')) ||
  $$,
    'Adi',
    'Jl. Ganesha No.10'
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_b.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('tugas_akhir_2_b.staff_id_seq')) ||
  $$,
    'Budi',
    'Jl. Let. Jend. Purn. Dr. (HC) Mashudi No.1'
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_b.staff',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('tugas_akhir_2_b.staff_id_seq')) ||
  $$,
    'Caca',
    'Jl. H. R. Rasuna Said No.Kav. 1'
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
  'tugas_akhir_2_b.staff',
  'staff_location',
  $$ 'Jl. Tamansari No.64' $$,
  'staff_name',
  $$ 'Adi' $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_correction(
  'tugas_akhir_2_b.staff',
  'staff_location',
  $$ 'Jl. Ganesa No.7' $$,
  'staff_name',
  $$ 'Budi' $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_inactivate(
  'tugas_akhir_2_b.staff',
  'staff_name',
  $$ 'Caca' $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_delete(
  'tugas_akhir_2_b.staff',
  'staff_name',
  $$ 'Adi' $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_correction_effective(
  'tugas_akhir_2_b.staff',
  'staff_name',
  $$ 'Budi' $$
)