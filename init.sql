create unlogged table clients (
	"id" int not null,
	"limit" int not null default 0,
	"balance" int not null default 0
);

create index idx_clients on clients (id);

insert into clients ("id", "limit") values (1, 100000), (2, 80000), (3, 1000000), (4, 10000000), (5, 500000);

create unlogged table transactions (
	"client_id" int not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_clients_id on transactions (client_id);
create index idx_clients_id_timestamp on transactions (client_id, timestamp);
create index idx_timestamp on transactions (timestamp);

create or replace procedure transact(
	client_id INTEGER,
    value integer,
    type text,
    description text,
    inout rbalance integer default null
)
language plpgsql
as $$
DECLARE
	original_value int;
begin
	select value into original_value;
	if type = 'd' then value = -value; end if;

	update clients c set balance = balance + value where c.id = client_id and c.balance + value >= - c.limit returning c.balance into rbalance;

	if rbalance is null then return; end if;
	
	insert into transactions (client_id, value, type, description) values (client_id, original_value, type, description);
end; 
$$ 
