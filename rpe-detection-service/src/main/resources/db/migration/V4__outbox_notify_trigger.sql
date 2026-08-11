CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('outbox_ready', '1');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- FOR EACH STATEMENT: fires once per batch insert, not per row.
-- 100-row batch = 1 notification (not 100).
-- Zero column references: survives all column-level schema changes.
CREATE TRIGGER outbox_insert_notify
    AFTER INSERT ON outbox
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_outbox_insert();
