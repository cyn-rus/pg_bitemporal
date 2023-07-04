DROP SCHEMA tugas_akhir_1 CASCADE;

CREATE SCHEMA IF NOT EXISTS tugas_akhir_1;

SELECT * FROM ll_create_event_bitemporal_table(
  'tugas_akhir_1.payment',
  $$
    payment_id INT NOT NULL,
    book_id TEXT NOT NULL,
    buyer_username TEXT NOT NULL,
    price INT NOT NULL
  $$,
  'payment_id'
);

CREATE SEQUENCE IF NOT EXISTS tugas_akhir_1.payment_id_seq;

SELECT * FROM ll_bitemporal_insert(
  'tugas_akhir_1.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_1.payment_id_seq')) ||
  $$,
    'ABC123',
    'budi_123',
    100000
  $$,
  timestamptz('2023-06-13 09:00:00.000000+07'),
  temporal_relationships.timeperiod('2023-06-13 09:00:00.000000+07', 'infinity')
);

SELECT * FROM ll_bitemporal_insert(
  'tugas_akhir_1.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_1.payment_id_seq')) ||
  $$,
    'DEF456',
    'mawar_4',
    50000
  $$,
  timestamptz('2023-06-13 11:04:00.000000+07'),
  temporal_relationships.timeperiod('2023-06-13 11:04:00.000000+07', 'infinity')
);

SELECT * FROM ll_bitemporal_insert(
  'tugas_akhir_1.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_1.payment_id_seq')) ||
  $$,
    'TAA000',
    'tugas_akhir',
    500000
  $$,
  timestamptz('2023-06-13 14:01:00.000000+07'),
  temporal_relationships.timeperiod('2023-06-13 14:01:00.000000+07', 'infinity')
);

SELECT * FROM ll_bitemporal_insert(
  'tugas_akhir_1.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_1.payment_id_seq')) ||
  $$,
    'BTP999',
    'bitemporal_skripsi',
    390000
  $$,
  timestamptz('2023-06-13 14:55:00.000000+07'),
  temporal_relationships.timeperiod('2023-06-13 14:55:00.000000+07', 'infinity')
);

SELECT * FROM ll_bitemporal_insert(
  'tugas_akhir_1.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('tugas_akhir_1.payment_id_seq')) ||
  $$,
    'PGB023',
    'pg_bitemporal',
    770000
  $$,
  timestamptz('2023-06-13 17:37:00.000000+07'),
  temporal_relationships.timeperiod('2023-06-13 17:37:00.000000+07', 'infinity')
);

SELECT * FROM ll_bitemporal_update(
  'tugas_akhir_1.payment',
  'buyer_username',
  $$ 'bukan_budi' $$,
  'buyer_username',
  $$ 'budi_123' $$,
  timestamptz('2023-06-13 09:00'),
  temporal_relationships.timeperiod('2023-06-13 20:02:00.000000+07', 'infinity')
);

SELECT * FROM ll_bitemporal_correction(
  'tugas_akhir_1.payment',
  'price',
  $$ 410000 $$,
  'payment_id',
  $$ '4' $$,
  timestamptz('2023-06-13 20:39:00.000000+07')
);

SELECT * FROM ll_bitemporal_delete(
  'tugas_akhir_1.payment',
  'payment_id',
  '3',
  timestamptz('2023-06-13 21:33:00.000000+07')
);

SELECT * FROM ll_bitemporal_correction_effective(
  'tugas_akhir_1.payment',
  'payment_id',
  '5',
  timestamptz('2023-06-13 21:48:00.000000+07'),
  timestamptz('2023-06-13 21:48:00.000000+07')
);