CREATE SCHEMA IF NOT EXISTS bt_tutorial;
grant usage on schema bitemporal_internal to public;

-- Create Table
SELECT * FROM bitemporal_internal.ll_create_period_bitemporal_table(
  'bt_tutorial.staff_bt',
	$$
    staff_id INT, 
	  staff_name TEXT NOT NULL,
    staff_location TEXT NOT NULL
	$$,
  'staff_id'
);
   
SELECT * FROM bitemporal_internal.ll_create_period_bitemporal_table(
  'bt_tutorial.cust_bt',
	$$
    cust_id INT NOT NULL, 
	  cust_name TEXT NOT NULL,
    phone TEXT
	$$,
  'cust_id'
);
   
SELECT * FROM bitemporal_internal.ll_create_period_bitemporal_table(
  'bt_tutorial.product_bt',
	$$
    product_id INT,
	  product_name TEXT NOT NULL,
    weight INT NOT NULL default(0),
    price INT NOT NULL default(0)
	$$,
  'product_id'
);
   
SELECT * FROM bitemporal_internal.ll_create_period_bitemporal_table(
  'bt_tutorial.order_bt',
	$$
    order_id INT NOT NULL,
	  staff_id INT NOT NULL,
    cust_id INT NOT NULL,
	  order_created_at timestamptz
	$$,
  'order_id'
);
     
SELECT * FROM bitemporal_internal.ll_create_period_bitemporal_table(
  'bt_tutorial.order_line_bt',
	$$
    order_line_id INT NOT NULL,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    qty INT NOT NULL,
    order_line_created_at timestamptz
	$$,
  'order_id,order_line_id'
);
  
-- Create Sequence
CREATE SEQUENCE IF NOT EXISTS bt_tutorial.staff_id_seq;
CREATE SEQUENCE IF NOT EXISTS bt_tutorial.cust_id_seq;
CREATE SEQUENCE IF NOT EXISTS bt_tutorial.product_id_seq;
CREATE SEQUENCE IF NOT EXISTS bt_tutorial.order_id_seq;
CREATE SEQUENCE IF NOT EXISTS bt_tutorial.order_line_id_seq;

-- Insert
SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'bt_tutorial.staff_bt',
  $$
    staff_id,
    staff_name,
    staff_location
  $$,
  quote_literal(nextval('bt_tutorial.staff_id_seq')) ||
  $$,
    'mystaff',
    'mylocation'
  $$,
  temporal_relationships.timeperiod('2023-03-01', 'infinity') --effective
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert('bt_tutorial.cust_bt'
,$$cust_id, cust_name, phone$$
,quote_literal(nextval('bt_tutorial.cust_id_seq'))||$$,
'mycust', '+6281197889890'$$
,temporal_relationships.timeperiod(now(), 'infinity') --effective
,temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert('bt_tutorial.product_bt'
,$$product_id, product_name,weight,price$$
,quote_literal(nextval('bt_tutorial.product_id_seq'))||$$,
'myproduct', 100,200$$
,temporal_relationships.timeperiod(now(), 'infinity') --effective
,temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert('bt_tutorial.product_bt',
  $$product_id, product_name,weight,price$$,
  quote_literal(nextval('bt_tutorial.product_id_seq'))||$$,
  'myproduct2', 200, 250 $$,
  temporal_relationships.timeperiod('2023-03-10 01:26:16.107912+07', 'infinity'), --effective
  temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert('bt_tutorial.order_bt'
,$$order_id, staff_id,cust_id,order_created_at$$
,quote_literal(nextval('bt_tutorial.order_id_seq'))||$$,
1,1,$$||quote_literal(now())
,temporal_relationships.timeperiod(now(), 'infinity') --effective
,temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert('bt_tutorial.order_line_bt',
  $$order_line_id, order_id, product_id, qty, order_line_created_at$$,
  quote_literal(nextval('bt_tutorial.order_line_id_seq')) || $$,
  1, 1, 10, $$ || quote_literal(now()),
  temporal_relationships.timeperiod(now(), 'infinity'), --effective
  temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

SELECT * FROM bitemporal_internal.ll_bitemporal_insert('bt_tutorial.order_line_bt',
  $$order_line_id,order_id, product_id,qty, order_line_created_at$$,
  quote_literal(nextval('bt_tutorial.order_line_id_seq')) || $$,
  1, 2, 15, $$ || quote_literal(now()),
  temporal_relationships.timeperiod(now(), 'infinity'), --effective
  temporal_relationships.timeperiod(now(), 'infinity') --asserted
);

-- Update
SELECT * FROM bitemporal_internal.ll_bitemporal_update(
  'bt_tutorial.staff_bt',
  'staff_location', -- fields to update'
  $$ 'newlocation' $$,  -- values to update with
  'staff_id',  -- search fields
  '1', --  search values
  temporal_relationships.timeperiod(now(), 'infinity')
);

SELECT
  o.order_id, 
  staff_name, 
  staff_location, 
  c.cust_name, 
  c.phone AS cust_phone, 
  p.product_name, 
  p.price,
  l.qty
FROM bt_tutorial.order_line_bt l
  JOIN bt_tutorial.order_bt o ON o.order_id = l.order_id
  JOIN bt_tutorial.product_bt p ON p.product_id = l.product_id
  JOIN bt_tutorial.staff_bt s ON s.staff_id = o.staff_id
  JOIN bt_tutorial.cust_bt c ON c.cust_id = o.cust_id
WHERE l.order_id=1
  AND order_line_created_at<@l.effective AND now()<@l.asserted
  AND order_created_at<@o.effective AND now()<@o.asserted
  AND order_created_at<@c.effective AND now()<@c.asserted
  AND order_created_at<@p.effective AND now()<@p.asserted
  AND order_created_at<@s.effective AND now()<@s.asserted;

-- Correction
SELECT * FROM bitemporal_internal.ll_bitemporal_correction(
  'bt_tutorial.product_bt',
  'price',
  '275',
  'product_id',
  '2',
  temporal_relationships.timeperiod('2023-03-10 01:26:16.107912+07', 'infinity')
);

---corrected price
SELECT
o.order_id, 
staff_name, 
staff_location,
c.cust_name, 
c.phone AS cust_phone, 
p.product_name, 
p.price,
l.qty
    FROM bt_tutorial.order_line_bt l
    JOIN bt_tutorial.order_bt o ON o.order_id = l.order_id
    JOIN bt_tutorial.product_bt p ON p.product_id = l.product_id
    JOIN bt_tutorial.staff_bt s ON s.staff_id = o.staff_id
    JOIN bt_tutorial.cust_bt c ON c.cust_id = o.cust_id
WHERE l.order_id=1
AND order_line_created_at<@l.effective AND now()<@l.asserted
AND order_created_at<@o.effective AND now()<@o.asserted
AND order_created_at<@c.effective AND now()<@c.asserted
AND order_created_at<@p.effective AND now()<@p.asserted
and order_created_at<@s.effective AND now()<@s.asserted;

--original price
SELECT
o.order_id, 
staff_name, 
staff_location, 
c.cust_name, 
c.phone AS cust_phone, 
p.product_name, 
p.price,
l.qty
    FROM bt_tutorial.order_line_bt l
    JOIN bt_tutorial.order_bt o ON o.order_id = l.order_id
    JOIN bt_tutorial.product_bt p ON p.product_id = l.product_id
    JOIN bt_tutorial.staff_bt s ON s.staff_id = o.staff_id
    JOIN bt_tutorial.cust_bt c ON c.cust_id = o.cust_id
WHERE l.order_id=1
AND order_line_created_at<@l.effective AND order_line_created_at<@l.asserted
AND order_created_at<@o.effective AND order_created_at<@o.asserted
AND order_created_at<@c.effective AND order_created_at<@c.asserted
AND order_created_at<@p.effective AND order_created_at<@p.asserted
AND order_created_at<@s.effective AND order_created_at<@s.asserted;

-- Inactivate
SELECT * FROM bitemporal_internal.ll_bitemporal_inactivate(
  'bt_tutorial.cust_bt',
  'cust_id',
  '1'
);

-- Delete
SELECT * FROM bitemporal_internal.ll_bitemporal_delete(
  'bt_tutorial.cust_bt',
  'cust_id',
  '1'
);

SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
  'bt_tutorial.test',
	$$
    id INT NOT NULL, 
	  name TEXT NOT NULL,
    phone TEXT
	$$,
  'id',
  'event'
);

CREATE SEQUENCE IF NOT EXISTS bt_tutorial.test_id_seq;

SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
  'bt_tutorial.test',
  $$
    id,
    name,
    phone
  $$,
  quote_literal(nextval('bt_tutorial.test_id_seq')) ||
  $$,
    'a',
    '123456'
  $$,
  now()
);