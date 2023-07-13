DROP SCHEMA coalesce_tes2 CASCADE;

CREATE SCHEMA IF NOT EXISTS coalesce_tes2;

SELECT * FROM create_event_bitemporal_table(
  'coalesce_tes2.payment',
  $$
    payment_id INT NOT NULL,
    book_id TEXT NOT NULL,
    buyer_username TEXT NOT NULL,
    price INT NOT NULL
  $$,
  'payment_id'
);

CREATE SEQUENCE IF NOT EXISTS coalesce_tes2.payment_id_seq;

SELECT * FROM bitemporal_insert(
  'coalesce_tes2.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('coalesce_tes2.payment_id_seq')) ||
  $$,
    'ABC123',
    'budi_123',
    100000
  $$,
  timestamptz('2023-06-13 09:00:00.000000+07'),
  timeperiod('2023-06-13', 'infinity')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes2.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('coalesce_tes2.payment_id_seq')) ||
  $$,
    'DEF456',
    'mawar_4',
    50000
  $$,
  timestamptz('2023-06-13 11:04:00.000000+07'),
  timeperiod('2023-06-13 11:04:00.000000+07', 'infinity')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes2.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('coalesce_tes2.payment_id_seq')) ||
  $$,
    'TAA000',
    'tugas_akhir',
    500000
  $$,
  timestamptz('2023-06-13 14:01:00.000000+07'),
  timeperiod('2023-06-13 14:01:00.000000+07', 'infinity')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes2.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('coalesce_tes2.payment_id_seq')) ||
  $$,
    'BTP999',
    'bitemporal_skripsi',
    390000
  $$,
  timestamptz('2023-06-13 14:55:00.000000+07'),
  timeperiod('2023-06-13 14:55:00.000000+07', 'infinity')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes2.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  quote_literal(nextval('coalesce_tes2.payment_id_seq')) ||
  $$,
    'PGB023',
    'pg_bitemporal',
    770000
  $$,
  timestamptz('2023-06-13 17:37:00.000000+07'),
  timeperiod('2023-06-13 17:37:00.000000+07', 'infinity')
);

SELECT * FROM bitemporal_update(
  'coalesce_tes2.payment',
  'buyer_username',
  $$ 'bukan_budi' $$,
  'buyer_username',
  $$ 'budi_123' $$,
  timestamptz('2023-06-13 09:00'),
  timeperiod('2023-06-13 20:02:00.000000+07', 'infinity')
);

SELECT * FROM bitemporal_correction(
  'coalesce_tes2.payment',
  'price',
  $$ 410000 $$,
  'payment_id',
  $$ '4' $$,
  timestamptz('2023-06-13 20:39:00.000000+07')
);

SELECT * FROM bitemporal_delete(
  'coalesce_tes2.payment',
  'payment_id',
  '3',
  timestamptz('2023-06-13 21:33:00.000000+07')
);

SELECT * FROM bitemporal_correction_valid(
  'coalesce_tes2.payment',
  'payment_id',
  '5',
  timestamptz('2023-06-13 21:48:00.000000+07'),
  timestamptz('2023-06-13 21:48:00.000000+07')
);

SELECT * FROM bitemporal_delete(
  'coalesce_tes2.payment',
  'payment_id',
  '5',
  timestamptz('2023-07-13')
);

SELECT * FROM bitemporal_insert(
  'coalesce_tes2.payment',
  $$
    payment_id,
    book_id,
    buyer_username,
    price
  $$,
  $$
    5,
    'PGB023',
    'pg_bitemporal',
    770000
  $$,
  timestamptz('2023-06-13 21:48:00.000000+07'),
  timeperiod('2023-07-13', 'infinity')
);

