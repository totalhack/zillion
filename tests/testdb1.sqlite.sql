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

INSERT OR IGNORE INTO partners (name) VALUES ('Partner A');
INSERT OR IGNORE INTO partners (name) VALUES ('Partner B');
INSERT OR IGNORE INTO partners (name) VALUES ('Partner C');

INSERT OR IGNORE INTO campaigns (name, category, partner_id) VALUES ('Campaign 1A', 'fruits', 1);
INSERT OR IGNORE INTO campaigns (name, category, partner_id) VALUES ('Campaign 2A', 'vegetables', 1);
INSERT OR IGNORE INTO campaigns (name, category, partner_id) VALUES ('Campaign 1B', 'fruits', 2);
INSERT OR IGNORE INTO campaigns (name, category, partner_id) VALUES ('Campaign 2B', 'vegetables', 2);
INSERT OR IGNORE INTO campaigns (name, category, partner_id) VALUES ('Campaign 1C', 'fruits', 3);
INSERT OR IGNORE INTO campaigns (name, category, partner_id) VALUES ('Campaign 2C', 'vegetables', 3);

INSERT INTO leads (id, name, campaign_id) VALUES (1, 'John Doe', 1);
INSERT INTO leads (id, name, campaign_id) VALUES (2, 'Jane Doe', 1);
INSERT INTO leads (id, name, campaign_id) VALUES (3, 'Jim Doe', 2);
INSERT INTO leads (id, name, campaign_id) VALUES (4, 'Jeff Doe', 2);
INSERT INTO leads (id, name, campaign_id) VALUES (5, 'Jeremy Doe', 3);
INSERT INTO leads (id, name, campaign_id) VALUES (6, 'Jessica Doe', 4);
INSERT INTO leads (id, name, campaign_id) VALUES (7, 'Jay Doe', 5);

INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('apple', 10, 10.0, 1);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('orange', 15, 7.0, 1);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lemon', 11, 30.0, 1);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lime', 12, 24.0, 1);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lemon', 20, 12.0, 2);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lettuce', 30, 8.0, 3);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('potato', 21, 18.0, 3);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('broccoli', 25, 17.0, 3);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('potato', 5, 11.0, 4);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('cauliflower', 50, 17.0, 4);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('broccoli', 25, 11.0, 4);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('apple', 35, 6.0, 5);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lettuce', 12, 13.0, 6);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('orange', 18, 7.5, 7);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('apple', 24, 15.0, 7);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lemon', 36, 9.0, 7);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('lime', 25, 46.0, 7);
INSERT INTO sales (item, quantity, revenue, lead_id) VALUES ('clementine', 32, 41.0, 7);
