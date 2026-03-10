-- Schema for the narcotic prescription dataset.
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
    narcotic_class  TEXT    NOT NULL,
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
-- User table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
  id            SERIAL      PRIMARY KEY,
  username      TEXT        NOT NULL UNIQUE,
  password_hash TEXT        NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
