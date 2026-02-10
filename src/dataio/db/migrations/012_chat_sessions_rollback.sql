-- Rollback Migration 012: Chat Sessions

DROP TRIGGER IF EXISTS trigger_update_chat_session_updated_at ON chat_messages;
DROP FUNCTION IF EXISTS update_chat_session_updated_at();
DROP TABLE IF EXISTS chat_messages;
DROP TABLE IF EXISTS chat_sessions;
