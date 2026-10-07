-- Migration 009: Add user suspension fields
-- This migration adds suspension tracking fields to the users table

BEGIN;

-- Add suspension fields
ALTER TABLE users ADD COLUMN IF NOT EXISTS suspended_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS suspended_by TEXT;

-- Add index for quick lookup of suspended users
CREATE INDEX IF NOT EXISTS idx_users_suspended_at ON users(suspended_at) WHERE suspended_at IS NOT NULL;

SELECT add_migration(9, '009_user_suspension');

COMMIT;
