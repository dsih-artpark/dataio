-- Migration 022: Dataset Download Audit & Metrics Logging
-- Records when users request download links for datasets.

CREATE TABLE IF NOT EXISTS dataset_downloads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
    dataset_id TEXT NOT NULL,
    access_channel TEXT NOT NULL DEFAULT 'WEB', -- 'WEB', 'SDK', 'MCP'
    ip_address TEXT,
    user_agent TEXT,
    downloaded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for performance on analytics queries
CREATE INDEX IF NOT EXISTS idx_dataset_downloads_user_email ON dataset_downloads(user_email);
CREATE INDEX IF NOT EXISTS idx_dataset_downloads_dataset_id ON dataset_downloads(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_downloads_downloaded_at ON dataset_downloads(downloaded_at);
