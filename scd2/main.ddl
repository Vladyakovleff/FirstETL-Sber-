----------------------------------------------------------------------------
-- Подготовка таблиц
create table de12.yavl_stg_transactions (			-- Создание стейджинга для транзакций
	trans_id bigint,
	trans_date timestamp(0),
	card_num varchar (20),
	oper_type varchar (20),
	amt varchar (20),
	oper_result varchar (20), 
    terminal varchar (20)
);


create table de12.yavl_stg_terminals (				-- Создание стейджинга для терминалов
	terminal_id varchar (10),
	terminal_type varchar (10),
	terminal_city varchar(20),
	terminal_address varchar(120),
	update_dt timestamp(0)
);

create table de12.yavl_stg_blacklist (				-- Создание стейджинга для черного списка
	entry_dt timestamp(0),
	passport_num varchar (20)	
);

create table de12.yavl_stg_cards (					-- Создание стейджинга для карт
	card_num varchar(20),
	account_num varchar(20),
	update_dt timestamp(0)
);

create table de12.yavl_stg_accounts (				-- Создание стейджинга для аккаунтов
	account_num varchar(20),
	valid_to date,
	client varchar(20),
	update_dt timestamp(0)
);

create table de12.yavl_stg_clients (				-- Создание стейджинга для клиентов
	client_id varchar(20),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(20),
	passport_valid_to date,
	phone varchar(20),
	update_dt timestamp(0)
);

create table de12.yavl_dwh_fact_transactions (		-- Создание целевой таблицы для транзакций
	trans_id bigint,
	trans_date timestamp(0),
	card_num varchar (20),
	oper_type varchar (20),
	amt varchar (20),
	oper_result varchar (20), 
    terminal varchar (20)
);

create table de12.yavl_dwh_fact_passport_blacklist (-- Создание целевой таблицы для черного списка
	entry_dt timestamp(0),
	passport_num varchar (20)	
);

create table de12.yavl_dwh_dim_terminals_hist (			-- Создание целевой таблицы для терминалов
	terminal_id varchar (10),
	terminal_type varchar (10),
	terminal_city varchar(20),
	terminal_address varchar(120),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1)
);

create table de12.yavl_stg_terminals_del( 			-- Создания слоя для отслеживания факта удаления записи
	terminal_id varchar (10),
	terminal_type varchar (10),
	terminal_city varchar(20),
	terminal_address varchar(120)
);

create table de12.yavl_dwh_dim_cards_hist (				-- Создание целевой таблицы для карт
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1)
);

create table de12.yavl_stg_cards_del( 				-- Создания слоя для отслеживания факта удаления записи
	card_num varchar(20),
	account_num varchar(20)
);

create table de12.yavl_dwh_dim_accounts_hist (			-- Создание целевой таблицы для аккаунтов
	account_num varchar(20),
	valid_to date,
	client varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1)
);

create table de12.yavl_stg_accounts_del( 			-- Создания слоя для отслеживания факта удаления записи
	account_num varchar(20),
	valid_to date,
	client varchar(20)
);

create table de12.yavl_dwh_dim_clients_hist (			-- Создание целевой таблицы для клиентов
	client_id varchar(20),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(20),
	passport_valid_to date,
	phone varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1)
);

create table de12.yavl_stg_clients_del( 			-- Создания слоя для отслеживания факта удаления записи
	client_id varchar(20),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(20),
	passport_valid_to date,
	phone varchar(20)
);

create table de12.yavl_rep_fraud(					-- Создание целевой таблицы для нахождения мошеннических действий
	event_dt date,
	passport varchar(20),
	fio varchar(40),
	phone varchar(20),
	event_type varchar(30),
	report_dt timestamp(0)
);

create table de12.yavl_meta(						-- Создание мета-слоя
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into de12.yavl_meta( schema_name, table_name, max_update_dt )	-- Наполнение метаслоя
values( 'DE12','yavl_first_etl', to_timestamp('1900-01-01','YYYY-MM-DD') );
