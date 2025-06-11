CONSISTENCY ALL;
CREATE ROLE IF NOT EXISTS tfosmb WITH PASSWORD = '4bc823ffdd63997c357a0cc343ba016d' AND SUPERUSER = false;
GRANT DESCRIBE ON ALL MBEANS TO tfosmb;
GRANT SELECT ON MBEAN 'org.apache.cassandra.db:type=StorageService' TO tfosmb;
GRANT EXECUTE ON MBEAN 'org.apache.cassandra.db:type=StorageService' TO tfosmb;
GRANT EXECUTE ON MBEAN 'org.apache.cassandra.db:type=EndpointSnitchInfo' TO tfosmb;
