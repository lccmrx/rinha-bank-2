create unlogged table client(
	"id" int primary key not null,
	"limit" int not null default 0,
	"balance" int not null default 0
);

create index idx_client on client (id);

insert into client ("id", "limit") values (1, 100000), (2, 80000), (3, 1000000), (4, 10000000), (5, 500000);

create unlogged table transaction(
	"client_id" int not null,
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now(),
	
	constraint fk_client
	foreign key (client_id) references client(id)
);

create index idx_client_id on transaction (client_id);
create index idx_client_id_timestamp on transaction (client_id, timestamp);
create index idx_timestamp on transaction (timestamp);
