-- Schema for the Swedish prescription statistics dataset.
--
-- This file is mounted into /docker-entrypoint-initdb.d/ and is executed
-- automatically by PostgreSQL the first time the database volume is created.
-- It runs before the ETL seed service, so all tables exist with proper types
-- and constraints when the data is inserted.
--
-- Lookup tables are loaded before prescription_data (enforced by ETL load order),
-- so foreign key constraints are safe to use here.

-- Case-insensitive text, used for email so comparisons ignore case.
CREATE EXTENSION IF NOT EXISTS citext;

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

CREATE TABLE IF NOT EXISTS drug_info (
    atc         TEXT        PRIMARY KEY REFERENCES drugs(atc),
    data        JSONB       NOT NULL DEFAULT '{}',
    scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
-- Authentication is OAuth-only (GitHub, Google), with passkeys added after
-- signup. Every user enters through an OIDC provider that supplies a verified
-- email, so email is NOT NULL. Login methods live in auth_identities; the
-- display name is taken live from the provider profile, so it is not stored.
CREATE TABLE IF NOT EXISTS users (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  email           CITEXT      NOT NULL UNIQUE,
  email_verified  BOOLEAN     NOT NULL DEFAULT FALSE,
  region_id       INTEGER     REFERENCES regions(id) CHECK (region_id <> 0),
  gender_id       INTEGER     REFERENCES genders(id) CHECK (gender_id <> 3),
  age_group_id    INTEGER     REFERENCES age_groups(id) CHECK (age_group_id <> 99),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Auth identities (OIDC providers only)
-- -----------------------------------------------------------------------------
-- One row per external login a user has linked. provider_uid is the stable
-- subject id from that provider (GitHub id, Google sub). Linking a second
-- provider to an existing account is done by matching the provider's
-- verified-email claim against users.email.
--
-- Passkeys do NOT belong here: a WebAuthn credential is not a
-- (provider, provider_uid) pair. They get their own table (see stub below).
CREATE TABLE IF NOT EXISTS auth_identities (
  id            SERIAL      PRIMARY KEY,
  user_id       UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider      TEXT        NOT NULL,   -- 'github', 'google'
  provider_uid  TEXT        NOT NULL,   -- the provider's stable subject id
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (provider, provider_uid)
);

-- List all identities for a user (e.g. account settings page).
CREATE INDEX IF NOT EXISTS idx_auth_identities_user_id
    ON auth_identities (user_id);

-- -----------------------------------------------------------------------------
-- WebAuthn credentials (passkeys) — added later
-- -----------------------------------------------------------------------------
-- Passkeys carry more state than an OIDC identity: credential_id, public_key,
-- a sign_count replay counter updated on every login, transports, and AAGUID.
-- Model them separately when the feature lands, e.g.:
--
-- CREATE TABLE webauthn_credentials (
--   id             SERIAL      PRIMARY KEY,
--   user_id        UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
--   credential_id  BYTEA       NOT NULL UNIQUE,
--   public_key     BYTEA       NOT NULL,
--   sign_count     BIGINT      NOT NULL DEFAULT 0,
--   transports     TEXT[],
--   created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
-- );

-- -----------------------------------------------------------------------------
-- UserMedication table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_medications (
  id          SERIAL      PRIMARY KEY,
  user_id     UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  atc         TEXT        NOT NULL REFERENCES drugs(atc),
  notes       TEXT,
  added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, atc)
);
