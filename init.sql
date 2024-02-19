create unlogged table client(
	"id" int primary key not null,
	"limit" int not null default 0
);

create index idx_client on client (id);

insert into client ("id", "limit") values (1, 100000), (2, 80000), (3, 1000000), (4, 10000000), (5, 500000);

create unlogged table balance1(
	balance int not null default 0,
	timestamp timestamp not null default now()
);

insert into balance1 (balance) values (0);

create index idx_balance_1_timestamp on balance1 (timestamp);

create unlogged table balance2(
	balance int not null default 0,
	timestamp timestamp not null default now()
);

insert into balance2 (balance) values (0);

create index idx_balance_2_timestamp on balance2 (timestamp);

create unlogged table balance3(
	balance int not null default 0,
	timestamp timestamp not null default now()
);

insert into balance3 (balance) values (0);

create index idx_balance_3_timestamp on balance3 (timestamp);

create unlogged table balance4(
	balance int not null default 0,
	timestamp timestamp not null default now()
);

insert into balance4 (balance) values (0);

create index idx_balance_4_timestamp on balance4 (timestamp);

create unlogged table balance5(
	balance int not null default 0,
	timestamp timestamp not null default now()
);

insert into balance5 (balance) values (0);

create index idx_balance_5_timestamp on balance5 (timestamp);

create unlogged table transaction1(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_1_timestamp on transaction1 (timestamp);

create unlogged table transaction2(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_2_timestamp on transaction2 (timestamp);

create unlogged table transaction3(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_3_timestamp on transaction3 (timestamp);

create unlogged table transaction4(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_4_timestamp on transaction4 (timestamp);

create unlogged table transaction5(
	"value" int not null,
	"type" char not null,
	"description" varchar(10) not null,
	"timestamp" timestamp not null default now()
);

create index idx_transaction_5_timestamp on transaction5 (timestamp);
