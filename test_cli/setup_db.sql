CREATE TABLE everything (
	varchar1 varchar NOT NULL,
	bigint1 int8 NULL,
	uuid1 uuid NULL,
	character1 bpchar(1) NULL,
	float41 float4 NULL,
	money1 money NULL,
	json1 json NULL,
	int4array _int4 NULL,
	interval1 interval NULL,
	CONSTRAINT everything_pk PRIMARY KEY (varchar1)
);