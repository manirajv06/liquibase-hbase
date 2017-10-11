CREATE TABLE IF NOT EXISTS TEST_HBASE.hbase_test_table11(org_id CHAR(15) not null, entity_id CHAR(15) not null, payload binary(1000), CONSTRAINT pk PRIMARY KEY (org_id, entity_id)) TTL=86400;
