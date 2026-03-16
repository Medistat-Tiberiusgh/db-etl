-- Schema for the Swedish prescription statistics dataset.
--
-- This file is mounted into /docker-entrypoint-initdb.d/ and is executed
-- automatically by PostgreSQL the first time the database volume is created.
-- It runs before the ETL seed service, so all tables exist with proper types
-- and constraints when the data is inserted.
--
-- Lookup tables are loaded before prescription_data (enforced by ETL load order),
-- so foreign key constraints are safe to use here.

-- -----------------------------------------------------------------------------
-- Lookup tables
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS drugs (
    atc             TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    narcotic_class  TEXT,
    PRIMARY KEY (atc)
);

CREATE TABLE IF NOT EXISTS regions (
    id      INTEGER NOT NULL,
    name    TEXT    NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS genders (
    id      INTEGER NOT NULL,
    name    TEXT    NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS age_groups (
    id      INTEGER NOT NULL,
    name    TEXT    NOT NULL,
    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- Fact table
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS prescription_data (
    year                INTEGER NOT NULL CHECK (year >= 2006 AND year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    region              INTEGER NOT NULL,
    atc                 TEXT    NOT NULL,
    gender              INTEGER NOT NULL,
    age_group           INTEGER NOT NULL,
    num_prescriptions   INTEGER NOT NULL CHECK (num_prescriptions >= 0),
    num_patients        INTEGER NOT NULL CHECK (num_patients >= 0),
    per_1000            NUMERIC(10, 2)   NOT NULL CHECK (per_1000 >= 0),
    PRIMARY KEY (year, region, atc, gender, age_group),
    CONSTRAINT chk_prescriptions_gte_patients CHECK (num_prescriptions >= num_patients),
    FOREIGN KEY (region)    REFERENCES regions(id),
    FOREIGN KEY (atc)       REFERENCES drugs(atc),
    FOREIGN KEY (gender)    REFERENCES genders(id),
    FOREIGN KEY (age_group) REFERENCES age_groups(id)
);

-- -----------------------------------------------------------------------------
-- Indexes for prescription_data
-- -----------------------------------------------------------------------------

-- Primary lookup: all statistics for a given drug
CREATE INDEX IF NOT EXISTS idx_prescription_data_atc
    ON prescription_data (atc);

-- Regional benchmarking: drug × region × year (the "killer feature" queries)
CREATE INDEX IF NOT EXISTS idx_prescription_data_atc_region_year
    ON prescription_data (atc, region, year);

-- Demographic context: drug × gender × age_group
CREATE INDEX IF NOT EXISTS idx_prescription_data_atc_demographics
    ON prescription_data (atc, gender, age_group);

-- Trend analysis: drug × year
CREATE INDEX IF NOT EXISTS idx_prescription_data_atc_year
    ON prescription_data (atc, year);

-- -----------------------------------------------------------------------------
-- User table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
  id            SERIAL      PRIMARY KEY,
  username      TEXT        NOT NULL UNIQUE,
  password_hash TEXT        NOT NULL,
  region_id     INTEGER     NOT NULL REFERENCES regions(id) CHECK (region_id <> 0),
  gender_id     INTEGER     NOT NULL REFERENCES genders(id) CHECK (gender_id <> 3),
  age_group_id  INTEGER     NOT NULL REFERENCES age_groups(id) CHECK (age_group_id <> 99),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- UserMedication table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_medications (
  id          SERIAL      PRIMARY KEY,
  user_id     INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  atc         TEXT        NOT NULL REFERENCES drugs(atc),
  notes       TEXT,
  added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, atc)
);
