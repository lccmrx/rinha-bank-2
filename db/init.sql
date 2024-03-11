create unlogged table clients(
	"id" int primary key not null,
	"limit" int not null default 0
);

create index idx_client on clients (id);

insert into clients ("id", "limit") values (1, 100000), (2, 80000), (3, 1000000), (4, 10000000), (5, 500000);

create unlogged table transactions1(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_1_timestamp on transactions1 (timestamp);

create unlogged table transactions2(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_2_timestamp on transactions2 (timestamp);

create unlogged table transactions3(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_3_timestamp on transactions3 (timestamp);

create unlogged table transactions4(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_4_timestamp on transactions4 (timestamp);

create unlogged table transactions5(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_5_timestamp on transactions5 (timestamp);

create or replace procedure transact(
	client_id INTEGER,
    value integer,
    type text,
    description text
)
language plpgsql
as $$
begin
	execute format('insert into %I (value, type, description) values (%L, %L, %L);', 'transactions' || client_id, value, type, description);
end; 
$$;

CREATE OR REPLACE FUNCTION get_balance(client_id integer)
RETURNS integer 
LANGUAGE plpgsql
AS $$
declare
	tablename text;
	balance integer := 0;
begin
	tablename := 'transactions' || client_id;
	
    EXECUTE format('select sum(case when type = %L then s else -s end) balance from (select type, sum(value) s from %I group by type) a;', 'c', tablename) into balance;

	balance := coalesce(balance, 0);

	return balance;
END;
$$;

CREATE TYPE transaction_type AS (
    type char, value int, description varchar(10), timestamp timestamp, balance int
);

CREATE OR REPLACE FUNCTION get_last_transactions(client_id integer)
RETURNS SETOF transaction_type
LANGUAGE plpgsql
AS $$
DECLARE
    tablename text;
    rec transaction_type; -- For fetching rows
BEGIN
    tablename := 'transactions' || client_id;

    FOR rec IN EXECUTE format('SELECT type, value, description, timestamp, %L balance FROM %I order by timestamp desc limit 10;', get_balance(client_id), tablename)
    LOOP
        RETURN NEXT rec; -- Return each row one by one
    END LOOP;

    RETURN;
END;
$$;
