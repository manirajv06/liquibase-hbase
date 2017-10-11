create table test_hbase.person (person_id bigint not null, name varchar(255), marital_status boolean, state char(2), salary decimal(2,2), created_date date CONSTRAINT person_person_id PRIMARY KEY (person_id));

create sequence test_hbase.person_seq;
