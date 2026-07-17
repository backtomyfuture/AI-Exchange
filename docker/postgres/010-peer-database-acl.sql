-- Managed application credentials must be confined to their application DB.
-- Phase4-Lite runs this only while creating its new dedicated Compose volume.
REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE template1 FROM PUBLIC;
