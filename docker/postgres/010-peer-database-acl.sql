-- Managed application credentials must be confined to their application DB.
-- Existing volumes require the equivalent DBA-reviewed REVOKE before cutover.
REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE template1 FROM PUBLIC;
