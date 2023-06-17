DROP SCHEMA tugas_akhir_2_a CASCADE;

CREATE SCHEMA IF NOT EXISTS tugas_akhir_2_a;

SELECT * FROM bitemporal_internal.ll_create_event_bitemporal_table(
  'tugas_akhir_2_a.payment',
  $$
    payment_id INT NOT NULL,
    book_id TEXT NOT NULL,
    buyer_username TEXT NOT NULL,
    price INT NOT NULL
  $$,
  'payment_id'
);

CREATE SEQUENCE IF NOT EXISTS tugas_akhir_2_a.payment_id_seq;

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_a.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_2_a.payment_id_seq')) ||
  $$,
    'ABC123',
    'budi_123',
    100000
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_a.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_2_a.payment_id_seq')) ||
  $$,
    'DEF456',
    'mawar_4',
    50000
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_a.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_2_a.payment_id_seq')) ||
  $$,
    'TAA000',
    'tugas_akhir',
    500000
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_a.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_2_a.payment_id_seq')) ||
  $$,
    'BTP999',
    'bitemporal_skripsi',
    390000
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'tugas_akhir_2_a.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_2_a.payment_id_seq')) ||
  $$,
    'PGB023',
    'pg_bitemporal',
    770000
  $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_update(
  'tugas_akhir_2_a.payment',
  'buyer_username',
  $$ 'bukan_budi' $$,
  'buyer_username',
  $$ 'budi_123' $$,
  timestamptz('2023-06-13 23:30:13.002769+07')
);

SELECT * FROM bitemporal_internal.ll_bitemporal_correction(
  'tugas_akhir_2_a.payment',
  'price',
  $$ 410000 $$,
  'payment_id',
  $$ '4' $$
);

SELECT * FROM bitemporal_internal.ll_bitemporal_delete(
  'tugas_akhir_2_a.payment',
  'payment_id',
  '3'
);

SELECT * FROM bitemporal_internal.ll_bitemporal_correction_effective(
  'tugas_akhir_2_a.payment',
  'payment_id',
  '5'
);