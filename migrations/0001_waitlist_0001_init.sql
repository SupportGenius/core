CREATE TABLE IF NOT EXISTS waitlist_entries (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    email_normalized TEXT NOT NULL,
    product TEXT NOT NULL,
    status TEXT NOT NULL,
    position INTEGER,
    referral_code TEXT UNIQUE,
    referred_by TEXT,
    referrals INTEGER NOT NULL DEFAULT 0,
    answers TEXT,
    created_at TEXT NOT NULL,
    confirmed_at TEXT,
    UNIQUE(email_normalized, product)
);
