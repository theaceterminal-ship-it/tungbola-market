-- ============================================================
-- TungbolaMarket — Supabase Schema
-- Run this in: Supabase Dashboard → SQL Editor → New query → Run
-- ============================================================

-- Players
CREATE TABLE IF NOT EXISTS players (
  phone         TEXT    PRIMARY KEY,
  id            TEXT    NOT NULL UNIQUE,
  name          TEXT    NOT NULL,
  password_hash TEXT    NOT NULL,
  created_at    BIGINT  NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
);

-- Sessions (7-day sliding TTL via expires_at)
CREATE TABLE IF NOT EXISTS sessions (
  token      TEXT        PRIMARY KEY,
  player_id  TEXT        NOT NULL,
  phone      TEXT        NOT NULL,
  name       TEXT        NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days'
);
CREATE INDEX IF NOT EXISTS idx_sessions_phone      ON sessions(phone);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

-- Browser push subscriptions
CREATE TABLE IF NOT EXISTS push_subscriptions (
  phone      TEXT        PRIMARY KEY,
  subscription JSONB     NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Operators (vendors)
CREATE TABLE IF NOT EXISTS operators (
  id               TEXT    PRIMARY KEY,
  name             TEXT    NOT NULL,
  email            TEXT    NOT NULL DEFAULT '',
  phone            TEXT    NOT NULL DEFAULT '',
  plan             TEXT    NOT NULL CHECK (plan IN ('own-sheets', 'generate')),
  api_key          TEXT    NOT NULL UNIQUE,
  telegram_chat_id TEXT,
  active           BOOLEAN NOT NULL DEFAULT true,
  created_at       BIGINT  NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
);
CREATE INDEX IF NOT EXISTS idx_operators_api_key ON operators(api_key);

-- Games
CREATE TABLE IF NOT EXISTS games (
  id              TEXT    PRIMARY KEY,
  operator_id     TEXT    REFERENCES operators(id) ON DELETE SET NULL,
  operator_name   TEXT,
  name            TEXT    NOT NULL,
  game_date       TEXT,
  game_date_raw   TEXT,
  join_time       TEXT,
  price_per_sheet INTEGER NOT NULL DEFAULT 5,
  pricing_tiers   JSONB   NOT NULL DEFAULT '[]',
  description     TEXT    NOT NULL DEFAULT '',
  prizes          JSONB   NOT NULL DEFAULT '[]',
  thumbnail       TEXT,
  status          TEXT    NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','listed','ended')),
  sheet_from      INTEGER NOT NULL DEFAULT 0,
  sheet_to        INTEGER NOT NULL DEFAULT 0,
  sheet_count     INTEGER NOT NULL DEFAULT 0,
  sold_count      INTEGER NOT NULL DEFAULT 0,
  sold_sheet_nums JSONB   NOT NULL DEFAULT '[]',
  created_at      BIGINT  NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
);
CREATE INDEX IF NOT EXISTS idx_games_status      ON games(status);
CREATE INDEX IF NOT EXISTS idx_games_operator_id ON games(operator_id);
CREATE INDEX IF NOT EXISTS idx_games_created_at  ON games(created_at DESC);

-- Shared sheet library (admin — Plan B)
CREATE TABLE IF NOT EXISTS sheets (
  id TEXT    PRIMARY KEY,
  n  INTEGER NOT NULL UNIQUE,
  f  TEXT    NOT NULL,
  u  TEXT    NOT NULL,
  s  INTEGER NOT NULL DEFAULT 0,
  ts BIGINT  NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
);
CREATE INDEX IF NOT EXISTS idx_sheets_n ON sheets(n);

-- Operator sheet libraries (Plan A)
CREATE TABLE IF NOT EXISTS operator_sheets (
  operator_id TEXT    NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
  n           INTEGER NOT NULL,
  f           TEXT    NOT NULL,
  u           TEXT    NOT NULL,
  s           INTEGER NOT NULL DEFAULT 0,
  ts          BIGINT  NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
  PRIMARY KEY (operator_id, n)
);
CREATE INDEX IF NOT EXISTS idx_op_sheets_operator ON operator_sheets(operator_id);

-- Purchases
CREATE TABLE IF NOT EXISTS purchases (
  purchase_id          TEXT        PRIMARY KEY,
  player_name          TEXT        NOT NULL,
  phone                TEXT        NOT NULL,
  game_id              TEXT        NOT NULL,
  game_name            TEXT        NOT NULL,
  quantity             INTEGER     NOT NULL,
  amount               INTEGER     NOT NULL,
  requested_sheet_nums JSONB,
  status               TEXT        NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','approved','downloaded','rejected')),
  download_token       TEXT,
  sheet_nums           JSONB,
  created_at           BIGINT      NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
  approved_at          BIGINT,
  downloaded           BOOLEAN     NOT NULL DEFAULT false,
  downloaded_at        BIGINT,
  expires_at           TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours'
);
CREATE INDEX IF NOT EXISTS idx_purchases_phone      ON purchases(phone);
CREATE INDEX IF NOT EXISTS idx_purchases_game_id    ON purchases(game_id);
CREATE INDEX IF NOT EXISTS idx_purchases_status     ON purchases(status);
CREATE INDEX IF NOT EXISTS idx_purchases_created_at ON purchases(created_at DESC);

-- Download tokens (6-hour TTL)
CREATE TABLE IF NOT EXISTS download_tokens (
  token       TEXT        PRIMARY KEY,
  sheets      JSONB       NOT NULL,
  game_name   TEXT        NOT NULL,
  purchase_id TEXT        NOT NULL,
  consumed    BOOLEAN     NOT NULL DEFAULT false,
  expires_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '6 hours'
);

-- Live game called numbers (2-hour TTL)
CREATE TABLE IF NOT EXISTS live_games (
  game_id        TEXT    PRIMARY KEY,
  called_numbers JSONB   NOT NULL DEFAULT '[]',
  last_number    INTEGER,
  last_called_at BIGINT,
  expires_at     TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '2 hours'
);

-- App config (key-value)
CREATE TABLE IF NOT EXISTS config (
  key   TEXT  PRIMARY KEY,
  value JSONB NOT NULL
);

-- Rate limits (atomic counter per action:ip:window)
CREATE TABLE IF NOT EXISTS rate_limits (
  id           TEXT   PRIMARY KEY,
  count        INTEGER NOT NULL DEFAULT 1,
  window_start BIGINT  NOT NULL
);

-- Atomic rate-limit increment (prevents race conditions on counters)
CREATE OR REPLACE FUNCTION increment_rate_limit(p_key TEXT, p_window_start BIGINT)
RETURNS INTEGER AS $$
DECLARE v_count INTEGER;
BEGIN
  INSERT INTO rate_limits (id, count, window_start)
  VALUES (p_key, 1, p_window_start)
  ON CONFLICT (id) DO UPDATE SET
    count        = CASE WHEN rate_limits.window_start = p_window_start
                        THEN rate_limits.count + 1
                        ELSE 1 END,
    window_start = p_window_start
  RETURNING count INTO v_count;
  RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Seed default config rows
INSERT INTO config (key, value) VALUES
  ('settings',   '{"operatorName":"","whatsappNumber":"","supportText":"","upiId":"","customQrUrl":null}'::jsonb),
  ('app_config', '{"upiId":"","pricePerSheet":5}'::jsonb)
ON CONFLICT (key) DO NOTHING;
