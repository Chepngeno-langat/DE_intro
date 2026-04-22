create database finance_db;

-- Raw layer
create table if not exists raw_fx_rates (
	id serial primary key,
	base_currency varchar(10),
	target_currency varchar(10),
	rate decimal(18, 8),
	rate_date date,
	ingested_at timestamp default now()
);

-- Transformed layer
create table if not exists mart_fx_daily_summary (
	target_currency varchar(10),
	rate_date date,
	avg_rate decimal(18, 8),
	min_rate decimal(18, 8),
	max_rate decimal(18, 8),
	rate_vs_prev decimal(18, 8),
	updated_at timestamp default now(),
	primary key (target_currency, rate_date)
);


