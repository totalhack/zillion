DROP TABLE IF EXISTS leads;
CREATE TABLE IF NOT EXISTS leads (
  id INTEGER PRIMARY KEY,
  name VARCHAR DEFAULT NULL,
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

INSERT OR IGNORE INTO partners (name, created_at) VALUES ('Partner A', '2019-03-26 21:02:15');
INSERT OR IGNORE INTO partners (name, created_at) VALUES ('Partner B', '2019-03-26 21:02:15');
INSERT OR IGNORE INTO partners (name, created_at) VALUES ('Partner C', '2019-03-26 21:02:15');

INSERT OR IGNORE INTO partner_sibling (partner_id, sibling_dim) VALUES (1, "Partner A Sibling Dim");
INSERT OR IGNORE INTO partner_sibling (partner_id, sibling_dim) VALUES (2, "Partner B Sibling Dim");

INSERT OR IGNORE INTO campaigns (name, category, partner_id, created_at) VALUES ('Campaign 1A', 'fruits', 1, '2019-03-26 21:02:15');
INSERT OR IGNORE INTO campaigns (name, category, partner_id, created_at) VALUES ('Campaign 2A', 'vegetables', 1, '2019-03-26 21:02:15');
INSERT OR IGNORE INTO campaigns (name, category, partner_id, created_at) VALUES ('Campaign 1B', 'fruits', 2, '2019-03-26 21:02:15');
INSERT OR IGNORE INTO campaigns (name, category, partner_id, created_at) VALUES ('Campaign 2B', 'vegetables', 2, '2019-03-26 21:02:15');
INSERT OR IGNORE INTO campaigns (name, category, partner_id, created_at) VALUES ('Campaign 1C', 'fruits', 3, '2019-03-26 21:02:15');
INSERT OR IGNORE INTO campaigns (name, category, partner_id, created_at) VALUES ('Campaign 2C', 'vegetables', 3, '2019-03-26 21:02:15');

INSERT INTO leads (id, name, campaign_id, created_at) VALUES (1, 'John Doe', 1, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (2, 'Jane Doe', 1, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (3, 'Jim Doe', 2, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (4, 'Jeff Doe', 2, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (5, 'Jeremy Doe', 3, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (6, 'Jessica Doe', 4, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (7, 'Jay Doe', 5, '2020-04-30 23:24:11');
INSERT INTO leads (id, name, campaign_id, created_at) VALUES (8, NULL, 5, '2020-04-30 23:24:11');

INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('apple', 10, 10.0, 1, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('orange', 15, 7.0, 1, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lemon', 11, 30.0, 1, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lime', 12, 24.0, 1, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lemon', 20, 12.0, 2, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lettuce', 30, 8.0, 3, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('potato', 21, 18.0, 3, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('broccoli', 25, 17.0, 3, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('potato', 5, 11.0, 4, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('cauliflower', 50, 17.0, 4, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('broccoli', 25, 11.0, 4, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('apple', 35, 6.0, 5, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lettuce', 12, 13.0, 6, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('orange', 18, 7.5, 7, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('apple', 24, 15.0, 7, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lemon', 36, 9.0, 7, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('lime', 25, 46.0, 7, "2020-04-30 23:24:11");
INSERT INTO sales (item, quantity, revenue, lead_id, created_at) VALUES ('clementine', 32, 41.0, 7, "2020-04-30 23:24:11");
