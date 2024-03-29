DROP TABLE IF EXISTS partners;
CREATE TABLE IF NOT EXISTS partners (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS partner_sibling;
CREATE TABLE IF NOT EXISTS partner_sibling (
  partner_id INTEGER PRIMARY KEY,
  sibling_dim VARCHAR NOT NULL UNIQUE
);

DROP TABLE IF EXISTS campaigns;
CREATE TABLE IF NOT EXISTS campaigns (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  category VARCHAR NOT NULL,
  partner_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

