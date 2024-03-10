create unlogged table clients(
	"id" int primary key not null,
	"limit" int not null default 0
);

create index idx_client on clients (id);

insert into clients ("id", "limit") values (1, 100000), (2, 80000), (3, 1000000), (4, 10000000), (5, 500000);

create unlogged table transactions1(
	id serial primary key not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_1_timestamp on transactions1 (timestamp);

create unlogged table transactions_snapshot1(
	"last_transaction_id" int not null,
	"balance" int not null
);

create unlogged table transactions2(
	id serial primary key not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_2_timestamp on transactions2 (timestamp);

create unlogged table transactions_snapshot2(
	"last_transaction_id" int not null,
	"balance" int not null
);

create unlogged table transactions3(
	id serial primary key not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_3_timestamp on transactions3 (timestamp);

create unlogged table transactions_snapshot3(
	"last_transaction_id" int not null,
	"balance" int not null
);

create unlogged table transactions4(
	id serial primary key not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_4_timestamp on transactions4 (timestamp);

create unlogged table transactions_snapshot4(
	"last_transaction_id" int not null,
	"balance" int not null
);

create unlogged table transactions5(
	id serial primary key not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_5_timestamp on transactions5 (timestamp);

create unlogged table transactions_snapshot5(
	"last_transaction_id" int not null,
	"balance" int not null
);

create or replace procedure transact(
	client_id INTEGER,
    value integer,
    type text,
    description text,
    inout balance integer default null
)
language plpgsql
as $$
begin
	select get_balance(client_id) into balance;

	if client_id = 1 and type = 'd' then 
		if -100000 > balance - value then select null into balance; rollback; return;
		end if;
	end if;
	if client_id = 2 and type = 'd' then
		if -80000 > balance - value then select null into balance; rollback; return;
		end if;
	end if;
	if client_id = 3 and type = 'd' then
		if -1000000 > balance - value then select null into balance; rollback; return;
		end if;
	end if;
	if client_id = 4 and type = 'd' then
		if -10000000 > balance - value then select null into balance; rollback; return;
		end if;
	end if;
	if client_id = 5 and type = 'd' then
		if -500000 > balance - value then select null into balance; rollback; return;
		end if;
	end if;

	execute format('insert into %I (value, type, description) values (%L, %L, %L);', 'transactions' || client_id, value, type, description);
	commit;
	select get_balance(client_id) into balance;
end; 
$$;

CREATE TYPE transaction_snapshot_type AS (
    last_id int, balance int
);

CREATE OR REPLACE FUNCTION get_balance(client_id integer)
RETURNS integer 
LANGUAGE plpgsql
AS $$
declare
	tablename text;
	tablename_snapshot text;
	last_transaction_id integer;
	balance integer := 0;
	rec transaction_snapshot_type;
	row_count integer := 0;
begin
	tablename := 'transactions' || client_id;
	tablename_snapshot := 'transactions_snapshot' || client_id;

	FOR rec in EXECUTE format('SELECT last_transaction_id, balance from %I order by last_transaction_id desc limit 1', tablename_snapshot)
	LOOP
        row_count := row_count + 1; -- Increment the row counter
    END LOOP;
	if row_count = 0 then last_transaction_id := 0; else last_transaction_id := rec.last_id; end if;

	
    EXECUTE format('select sum(case when type = %L then s else -s end) balance from (select type, sum(value) s from %I where id > %L group by type) a;', 'c', tablename, last_transaction_id) into balance;

	balance := coalesce(balance, 0);

	select balance + coalesce(rec.balance, 0) into balance;

	return balance;
END;
$$;

CREATE TYPE transaction_type AS (
    id int, type char, value int, description varchar(10), timestamp timestamp
);

CREATE OR REPLACE FUNCTION get_last_transactions(client_id integer)
RETURNS SETOF transaction_type
LANGUAGE plpgsql
AS $$
DECLARE
    tablename text;
	tablename_snapshot text;
	last_transaction_id integer;
    rec transaction_type; -- For fetching rows
    row_count integer := 0; -- Counter for the number of rows processed
BEGIN
    tablename := 'transactions' || client_id;
	tablename_snapshot := 'transactions_snapshot' || client_id;

	EXECUTE format('SELECT last_transaction_id FROM %I order by last_transaction_id desc limit 1', tablename_snapshot) into last_transaction_id;
	if last_transaction_id is null then last_transaction_id := 0; end if;

    -- Use a cursor within the EXECUTE to fetch rows one by one
    FOR rec IN EXECUTE format('SELECT id, type, value, description, timestamp FROM %I order by timestamp desc limit 10', tablename)
    LOOP
        RETURN NEXT rec; -- Return each row one by one
        row_count := row_count + 1; -- Increment the row counter
    END LOOP;

    -- Check if exactly 10 rows were processed
    IF row_count = 10 THEN
        -- Perform the snapshot operation
        -- Assuming get_balance(client_id) is defined elsewhere and returns the balance for the client
        -- Note: Adjust this part as needed to match your snapshot logic
        EXECUTE format('INSERT INTO %I (last_transaction_id, balance) VALUES (%L, %L);', tablename_snapshot, rec.id+9, get_balance(client_id));
    END IF;

    RETURN;
END;
$$;

