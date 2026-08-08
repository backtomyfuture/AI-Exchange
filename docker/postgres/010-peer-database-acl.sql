-- Managed application credentials must be confined to their application DB.
-- The polling baseline runs this only while creating its new dedicated volume.
REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE template1 FROM PUBLIC;
