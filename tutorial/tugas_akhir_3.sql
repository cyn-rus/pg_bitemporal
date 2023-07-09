DROP SCHEMA tugas_akhir_3 CASCADE;

CREATE SCHEMA IF NOT EXISTS tugas_akhir_3;

-- Create Table
CREATE TABLE tugas_akhir_3.departments(
  id TEXT PRIMARY KEY,
  dept_name TEXT
);

CREATE TABLE tugas_akhir_3.employee(
  name TEXT PRIMARY KEY,
  department TEXT NOT NULL,
  FOREIGN KEY (department) REFERENCES tugas_akhir_3.departments(id)
);

SELECT * FROM create_interval_bitemporal_table(
  'tugas_akhir_3.contract',
  $$
    id INT NOT NULL,
    name TEXT NOT NULL,
    salary INT NOT NULL,
    FOREIGN KEY (name) REFERENCES tugas_akhir_3.employee(name)
  $$,
  'id'
);

SELECT * FROM create_event_bitemporal_table(
  'tugas_akhir_3.events',
  $$
    id INT NOT NULL,
    type TEXT NOT NULL,
    held_by TEXT NOT NULL,
    location TEXT NOT NULL,
    FOREIGN KEY (held_by) REFERENCES tugas_akhir_3.departments(id)
  $$,
  'id'
);

CREATE SEQUENCE IF NOT EXISTS tugas_akhir_3.contract_id_seq;
CREATE SEQUENCE IF NOT EXISTS tugas_akhir_3.events_id_seq; 

-- Insert
INSERT INTO tugas_akhir_3.departments
VALUES
  ('SLS', 'Sales'),
  ('MKT', 'Marketing'),
  ('HRS', 'Human Resources'),
  ('FNN', 'Finance'),
  ('ITT', 'IT');

INSERT INTO tugas_akhir_3.employee
VALUES
  ('John Doe', (SELECT id FROM tugas_akhir_3.departments WHERE dept_name='Sales') ),
  ('Jane Smith', (SELECT id FROM tugas_akhir_3.departments WHERE dept_name='Marketing') ),
  ('David Lee', (SELECT id FROM tugas_akhir_3.departments WHERE dept_name='Human Resources') ),
  ('Sarah Johnson', (SELECT id FROM tugas_akhir_3.departments WHERE dept_name='Finance') ),
  ('Michael Brown', (SELECT id FROM tugas_akhir_3.departments WHERE dept_name='IT') );

-- 1
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.contract',
  $$
    id,
    name,
    salary
  $$,
  quote_literal(nextval('tugas_akhir_3.contract_id_seq')) ||
  $$,
    'John Doe',
    5000000
  $$,
  timeperiod('2023-01-01', 'infinity'),
  timeperiod('2023-01-01', 'infinity')
);

-- 2
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.contract',
  $$
    id,
    name,
    salary
  $$,
  quote_literal(nextval('tugas_akhir_3.contract_id_seq')) ||
  $$,
    'Jane Smith',
    5400000
  $$,
  timeperiod('2023-02-01', 'infinity'),
  timeperiod('2023-02-01', 'infinity')
);

-- 3
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.contract',
  $$
    id,
    name,
    salary
  $$,
  quote_literal(nextval('tugas_akhir_3.contract_id_seq')) ||
  $$,
    'David Lee',
    5200000
  $$,
  timeperiod('2023-03-15', 'infinity'),
  timeperiod('2023-03-15', 'infinity')
);

-- 4
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.contract',
  $$
    id,
    name,
    salary
  $$,
  quote_literal(nextval('tugas_akhir_3.contract_id_seq')) ||
  $$,
    'Sarah Johnson',
    6000000
  $$,
  timeperiod('2023-04-01', 'infinity'),
  timeperiod('2023-04-01', 'infinity')
);

-- 5
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.contract',
  $$
    id,
    name,
    salary
  $$,
  quote_literal(nextval('tugas_akhir_3.contract_id_seq')) ||
  $$,
    'Michael Brown',
    6100000
  $$,
  timeperiod('2023-06-01', 'infinity'),
  timeperiod('2023-06-01', 'infinity')
);

-- 1
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Meeting 1',
    'Room A',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Sales')),
  timestamptz('2023-06-08 10:30'),
  timeperiod('2023-06-08 10:30', 'infinity')
);

-- 2
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Meeting 2',
    'Room A',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Marketing')),
  timestamptz('2023-06-10 10:00'),
  timeperiod('2023-06-10 10:00', 'infinity')
);

-- 3
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Presentation',
    'Room C',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Finance')),
  timestamptz('2023-06-02 15:33'),
  timeperiod('2023-06-02 15:33', 'infinity')
);

-- 4
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Training',
    'Room B',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Human Resources')),
  timestamptz('2023-06-14 17:12'),
  timeperiod('2023-06-14 17:12', 'infinity')
);

-- 5
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Conference',
    'Sasana Budaya Ganesha',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Marketing')),
  timestamptz('2023-06-14 17:39'),
  timeperiod('2023-06-14 17:39', 'infinity')
);

-- 6
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Seminar',
    'Sasana Budaya Ganesha',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Marketing')),
  timestamptz('2023-06-21 16:27'),
  timeperiod('2023-06-21 16:27', 'infinity')
);

-- 7
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Workshop',
    'Room 7606',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'IT')),
  timestamptz('2023-06-25 13:53'),
  timeperiod('2023-06-25 13:53', 'infinity')
);

-- 8
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Networking',
    'Zoom',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Human Resources')),
  timestamptz('2023-06-30 10:22'),
  timeperiod('2023-06-30 10:22', 'infinity')
);

-- 9
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Hackathon',
    'Aula Barat',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'IT')),
  timestamptz('2023-06-29 14:41'),
  timeperiod('2023-06-29 14:41', 'infinity')
);

-- 10
SELECT * FROM bitemporal_insert(
  'tugas_akhir_3.events',
  $$
    id,
    type,
    location,
    held_by
  $$,
  quote_literal(nextval('tugas_akhir_3.events_id_seq')) ||
  $$,
    'Team Building',
    'Room A',
  $$ ||
  quote_literal((SELECT id FROM tugas_akhir_3.departments WHERE dept_name = 'Human Resources')),
  timestamptz('2023-06-02 10:38'),
  timeperiod('2023-06-02 10:38', 'infinity')
);

-- Update
SELECT * FROM bitemporal_update(
  'tugas_akhir_3.events',
  'location',
  $$ 'Aula Timur' $$,
  'type',
  $$ 'Hackathon' $$,
  timestamptz('2023-07-01 09:11'),
  timeperiod('2023-07-02 09:00', 'infinity')
);

SELECT * FROM bitemporal_update(
  'tugas_akhir_3.contract',
  'salary',
  $$ 6000000 $$,
  'name',
  $$ 'John Doe' $$,
  timeperiod('2023-07-03 17:49', 'infinity'),
  timeperiod('2023-07-03 17:49', 'infinity')
);

SELECT * FROM bitemporal_update(
  'tugas_akhir_3.events',
  $$
    type,
    location
  $$,
  $$
    'Company Lunch',
    'Warunk Upnormal'
  $$,
  'type',
  $$ 'Team Building' $$
);

-- Correction
SELECT * FROM bitemporal_correction(
  'tugas_akhir_3.events',
  'location',
  $$ 'Google Meet' $$,
  'type',
  $$ 'Networking' $$,
  timestamptz('2023-07-01 16:39')
);

SELECT * FROM bitemporal_correction(
  'tugas_akhir_3.contract',
  'salary',
  $$ 5500000 $$,
  'name',
  $$ 'Jane Smith' $$
);

-- Inactivate
SELECT * FROM bitemporal_inactivate(
  'tugas_akhir_3.contract',
  'name',
  $$ 'David Lee' $$,
  timeperiod('2023-09-01', 'infinity'),
  timeperiod('2023-08-01', 'infinity')
);

-- Delete
SELECT * FROM bitemporal_delete(
  'tugas_akhir_3.events',
  'type',
  $$ 'Meeting 2' $$,
  timestamptz('2023-09-01')
);

SELECT * FROM bitemporal_delete(
  'tugas_akhir_3.contract',
  'name',
  $$ 'Jane Smith' $$,
  timestamptz('2023-08-28')
);

-- Valid Correction
SELECT * FROM bitemporal_correction_effective(
  'tugas_akhir_3.events',
  'type',
  $$ 'Meeting 1' $$,
  timestamptz('2023-06-08 10:00'),
  time_endpoint('2023-06-22 15:27')
);

SELECT * FROM bitemporal_correction_effective(
  'tugas_akhir_3.contract',
  'name',
  $$ 'Sarah Johnson' $$,
  timeperiod('2023-05-01', 'infinity')
);