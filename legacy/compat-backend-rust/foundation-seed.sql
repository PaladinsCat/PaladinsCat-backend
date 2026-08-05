CREATE TABLE regions (
  region_code text PRIMARY KEY,
  region_name text NOT NULL
);

INSERT INTO regions (region_code, region_name) VALUES
  ('NA', 'North America'),
  ('EU', 'Europe');
