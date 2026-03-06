-- Schema for the narcotic prescription dataset.
--
-- This file is mounted into /docker-entrypoint-initdb.d/ and is executed
-- automatically by PostgreSQL the first time the database volume is created.
-- It runs before the ETL seed service, so all tables exist with proper types
-- and constraints when the data is inserted.
--
-- Note: foreign key constraints between tables are intentionally omitted.
-- The ETL loads CSV files in filesystem order (non-deterministic), so lookup
-- tables (drugs, regions, etc.) may not be populated before prescription_data
-- is loaded. PKs and NOT NULL constraints cover the important invariants.

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
    year                INTEGER NOT NULL,
    region              INTEGER NOT NULL,
    atc                 TEXT    NOT NULL,
    gender              INTEGER NOT NULL,
    age_group           INTEGER NOT NULL,
    num_prescriptions   INTEGER NOT NULL,
    num_patients        INTEGER NOT NULL,
    per_1000            FLOAT   NOT NULL,
    PRIMARY KEY (year, region, atc, gender, age_group)
);
