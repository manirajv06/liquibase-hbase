upsert into test_hbase.person (person_id, name, marital_status, state, created_date) values (next value for test_hbase.person_seq, 'person_1', true, 'ka', now());

upsert into test_hbase.person (person_id, name) values (next value for test_hbase.person_seq, 'person_2');

