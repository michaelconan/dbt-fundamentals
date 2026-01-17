-- Fundamentals training setup script for databricks


-- create schemas in unity catalog workspace
create schema workspace.jaffle_shop; 
create schema workspace.stripe;

-- create tables
-- NOTE: databricks requires column defaults to be enabled
create table workspace.jaffle_shop.customers 
( id integer,
  first_name varchar(255),
  last_name varchar(255)
);

create table workspace.jaffle_shop.orders
( id integer,
  user_id integer,
  order_date date,
  status varchar(255),
  _etl_loaded_at timestamp
);

ALTER TABLE workspace.jaffle_shop.orders
SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE workspace.jaffle_shop.orders
ALTER COLUMN _etl_loaded_at SET DEFAULT current_timestamp();


create table workspace.stripe.payment 
( id integer,
  orderid integer,
  paymentmethod varchar(255),
  status varchar(255),
  amount integer,
  created date,
  _batched_at timestamp
);

ALTER TABLE workspace.stripe.payment
SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE workspace.stripe.payment
ALTER COLUMN _batched_at SET DEFAULT current_timestamp();


-- load data from s3 buckets, infer schema to resolve column case differences

COPY INTO workspace.jaffle_shop.customers
FROM 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true'
);

COPY INTO workspace.jaffle_shop.orders
FROM 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true'
);

COPY INTO workspace.stripe.payment
FROM 's3://dbt-tutorial-public/stripe_payments.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true'
);


-- check data to confirm load

select * from workspace.jaffle_shop.customers;

select * from workspace.jaffle_shop.orders;

select * from workspace.stripe.payment;
