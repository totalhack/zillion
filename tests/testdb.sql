DROP TABLE IF EXISTS partners;
CREATE TABLE IF NOT EXISTS partners (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS campaigns;
CREATE TABLE IF NOT EXISTS campaigns (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  category VARCHAR NOT NULL,
  partner_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS leads;
CREATE TABLE IF NOT EXISTS leads (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  campaign_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS sales;
CREATE TABLE IF NOT EXISTS sales (
  id INTEGER PRIMARY KEY,
  item VARCHAR NOT NULL,
  quantity INTEGER NOT NULL,
  revenue DECIMAL(10, 2),
  lead_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO partners (name) VALUES ('Partner A');
INSERT INTO partners (name) VALUES ('Partner B');
INSERT INTO partners (name) VALUES ('Partner C');

INSERT INTO campaigns (name, category, partner_id) VALUES ('Campaign 1A', 'fruits', 1);
INSERT INTO campaigns (name, category, partner_id) VALUES ('Campaign 2A', 'vegetables', 1);
INSERT INTO campaigns (name, category, partner_id) VALUES ('Campaign 1B', 'fruits', 2);
INSERT INTO campaigns (name, category, partner_id) VALUES ('Campaign 2B', 'vegetables', 2);
INSERT INTO campaigns (name, category, partner_id) VALUES ('Campaign 1C', 'fruits', 3);
INSERT INTO campaigns (name, category, partner_id) VALUES ('Campaign 2C', 'vegetables', 3);

INSERT INTO leads (id, name, campaign_id) VALUES (1, 'John Doe', 1);
INSERT INTO leads (id, name, campaign_id) VALUES (2, 'Jane Doe', 1);
INSERT INTO leads (id, name, campaign_id) VALUES (3, 'Jim Doe', 2);
INSERT INTO leads (id, name, campaign_id) VALUES (4, 'Jeff Doe', 2);
INSERT INTO leads (id, name, campaign_id) VALUES (5, 'Jeremy Doe', 3);
INSERT INTO leads (id, name, campaign_id) VALUES (6, 'Jessica Doe', 4);
INSERT INTO leads (id, name, campaign_id) VALUES (7, 'Jay Doe', 5);

INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('apple', 10, 10.0, 1);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('orange', 15, 7.0, 1);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('broccoli', 20, 10.0, 2);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lettuce', 30, 8.0, 3);

INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('apple', 5, 10.0, 4);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('orange', 50, 7.0, 4);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('broccoli', 25, 10.0, 4);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lettuce', 35, 8.0, 5);

INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('apple', 12, 10.0, 6);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('orange', 18, 7.0, 7);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('broccoli', 24, 10.0, 7);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lettuce', 36, 8.0, 7);
