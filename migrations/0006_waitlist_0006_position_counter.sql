-- Issue #126: positions stop being `1 + MAX(position)`. The per-product
-- lock row gains a `next_position` counter that `confirm_entry`'s
-- lock-taking UPDATE increments, so allocation is a read of a row the
-- same transaction holds a write lock on rather than a MAX over rows
-- another writer could still touch. Backfill gives every existing
-- product the highest position already handed out, so the next confirm
-- continues the sequence instead of restarting it. The UNIQUE index
-- backstops the allocation: if the counter ever raced, the second
-- commit fails loudly instead of two entries silently sharing a
-- position.
ALTER TABLE waitlist_position_lock ADD COLUMN next_position INTEGER NOT NULL DEFAULT 0;
UPDATE waitlist_position_lock
SET next_position = COALESCE(
    (SELECT MAX(position) FROM waitlist_entries
     WHERE waitlist_entries.product = waitlist_position_lock.product),
    0);
CREATE UNIQUE INDEX IF NOT EXISTS waitlist_entries_product_position_key
    ON waitlist_entries (product, position);
