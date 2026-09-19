CREATE TABLE IF NOT EXISTS waitlist_send_cooldown (
    subject TEXT PRIMARY KEY,
    last_sent_at TEXT NOT NULL
);
