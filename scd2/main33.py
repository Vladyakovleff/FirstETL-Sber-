#!/usr/bin/python3
import psycopg2
import pandas as pd
import os

# Создание подключения к хранилищу
conn_dwh = psycopg2.connect(database = "edu",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "de12",
                        password = "sarumanthewhite",
                        port =     "5432")

# Создание подключения к источнику
# Создание подключения к источнику
conn_src = psycopg2.connect(database = "bank",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "bank_etl",
                        password = "bank_etl_password",
                        port =     "5432")

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

####################################################
##
# Очистка STAGE
cursor_dwh.execute( """ delete from de12.yavl_stg_transactions;
                        delete from de12.yavl_stg_terminals;
                        delete from de12.yavl_stg_blacklist;
                        delete from de12.yavl_stg_cards;
                        delete from de12.yavl_stg_accounts;
                        delete from de12.yavl_stg_clients;
                        delete from de12.yavl_stg_terminals_del;
                        delete from de12.yavl_stg_cards_del;
                        delete from de12.yavl_stg_accounts_del;
                        delete from de12.yavl_stg_clients_del;""" )
conn_dwh.commit()

## Импортируем transactions_03032021.txt
# Чтение из файла
df = pd.read_csv( '/home/de12/yavl/project/transactions_03032021.txt', sep=';', header=0, index_col=None )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_transactions(  trans_id, 
                                                trans_date, 
                                                amt,
                                                card_num, 
                                                oper_type, 
                                                oper_result, 
                                                terminal ) 
    VALUES( %s, %s, %s, %s, %s, %s, %s )""",
    df.values.tolist())

## Импортируем terminals_03032021.xlsx
# Чтение из файла
df = pd.read_excel( '/home/de12/yavl/project/terminals_03032021.xlsx', sheet_name='terminals', header=0, index_col=None )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_terminals( terminal_id, terminal_type, terminal_city, terminal_address, update_dt ) 
    VALUES( %s, %s, %s, %s, to_timestamp('03032021 00:00:00','DDMMYYYY HH24:MI:SS') )""",
    df.values.tolist())

##Обновление метаданных
## Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ update de12.yavl_meta
        set max_update_dt = coalesce( (select max( update_dt ) from de12.yavl_stg_terminals ), max_update_dt)
        where schema_name='DE12' and table_name = 'yavl_stg_terminals'""",
    df.values.tolist())

## Импортируем passport_blacklist_03032021.xlsx
# Чтение из файла
df = pd.read_excel( '/home/de12/yavl/project/passport_blacklist_03032021.xlsx', sheet_name='blacklist', header=0, index_col=None )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_blacklist( entry_dt, passport_num  ) 
    VALUES( to_date(%s::TEXT,'YYYY-MM-DD')  , %s )""",
    df.values.tolist())

## Загрузка и импорт из источника в таблицу de12.yavl_stg_clients
# Выполнение SQL кода в базе данных с возвратом результата
cursor_src.execute( """ SELECT 
                            client_id ,	
                            last_name ,	
                            first_name , 
                            patronymic , 
                            date_of_birth , 
                            passport_num , 
                            passport_valid_to , 
                            phone
                        FROM info.clients""" )
records = cursor_src.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_clients( client_id , last_name , first_name , patronymic , date_of_birth , passport_num , passport_valid_to , phone, update_dt ) 
    VALUES( %s, %s, %s, %s, %s, %s, %s, %s, now() )""",
    df.values.tolist())

## Загрузка и импорт из источника в таблицу de12.yavl_stg_accounts
# Выполнение SQL кода в базе данных с возвратом результата
cursor_src.execute( """ SELECT 
                            account ,
                            valid_to ,
                            client
                        FROM info.accounts""" )
records = cursor_src.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_accounts( account_num , valid_to , client, update_dt ) 
    VALUES( %s, %s, %s, now() )""",
    df.values.tolist())

## Загрузка и импорт из источника в таблицу de12.yavl_stg_cards
# Выполнение SQL кода в базе данных с возвратом результата
cursor_src.execute( """ SELECT 
                            card_num ,
                            account
                        FROM info.cards""" )
records = cursor_src.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_cards( card_num , account_num, update_dt ) 
    VALUES( %s, %s, now() )""",
    df.values.tolist())

## Формирование таблицы для вычисления удалений de12.yavl_stg_terminals_del
# Захват в стейджинг ключей из источника полным срезом для вычисления удалений
cursor_dwh.execute(
     """    insert into de12.yavl_stg_terminals_del     ( terminal_id, 
                                                          terminal_type, 
                                                          terminal_city,
                                                          terminal_address )
            select terminal_id, 
                   terminal_type, 
                   terminal_city,
                   terminal_address 
            from de12.yavl_stg_terminals""",
 df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwh_dim_terminals_hist
# Загрузка в приемник "вставок" на источнике (формат SCD2)
cursor_dwh.execute(
    """ insert into de12.yavl_dwh_dim_terminals_hist(terminal_id, 
                                                     terminal_type, 
                                                     terminal_city,
                                                     terminal_address,
                                                     effective_from,
                                                     effective_to,
                                                     deleted_flg )
            select 
	            stg.terminal_id, 
	            stg.terminal_type,
	            stg.terminal_city,
	            stg.terminal_address,
	            stg.update_dt, 
	            to_date('9999-12-31','YYYY-MM-DD'),
	            'N'
            from de12.yavl_stg_terminals stg
            left join de12.yavl_dwh_dim_terminals_hist tgt
            on stg.terminal_id = tgt.terminal_id
            where tgt.terminal_id is null""",
df.values.tolist())

# Обновление в приемнике "обновлений" на источнике (формат SCD2)
cursor_dwh.execute(
    """ update de12.yavl_dwh_dim_terminals_hist
        set 
	        effective_to = tmp.update_dt - interval '1 second'
        from (
	        select 
		        stg.terminal_id, 
		        stg.update_dt 
	        from de12.yavl_stg_terminals stg
	        inner join de12.yavl_dwh_dim_terminals_hist tgt
		        on stg.terminal_id = tgt.terminal_id
		        and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	        where   (stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )) or
	                (stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )) or
	                (stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null ))
        ) tmp
        where de12.yavl_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
          and de12.yavl_dwh_dim_terminals_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 

        insert into de12.yavl_dwh_dim_terminals_hist(   terminal_id, 
                                                        terminal_type, 
                                                        terminal_city,
                                                        terminal_address,
                                                        effective_from,
                                                        effective_to,
                                                        deleted_flg )
        select 
	        stg.terminal_id, 
	        stg.terminal_type,
	        stg.terminal_city,
	        stg.terminal_address,
	        stg.update_dt, 
	        to_date('9999-12-31','YYYY-MM-DD'),
	        'N'
        from de12.yavl_stg_terminals stg
        inner join de12.yavl_dwh_dim_terminals_hist tgt
	        on stg.terminal_id = tgt.terminal_id
	        and tgt.effective_to = update_dt - interval '1 second'
        where   (stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )) or
	            (stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )) or
	            (stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null ))""",
df.values.tolist())

# Удаление в приемнике удаленных в источнике записей (формат SCD2)
cursor_dwh.execute("""
insert into de12.yavl_dwh_dim_terminals_hist( terminal_id, 
                                              terminal_type, 
                                              terminal_city,
                                              terminal_address,
                                              effective_from,
                                              effective_to,
                                              deleted_flg )
select 
	tgt.terminal_id,
	tgt.terminal_type,
	tgt.terminal_city,
	tgt.terminal_address,
	tgt.effective_from + interval '1 day', 
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y'
from de12.yavl_dwh_dim_terminals_hist tgt
left join de12.yavl_stg_terminals_del stg
	on stg.terminal_id = tgt.terminal_id
where stg.terminal_id is null
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update de12.yavl_dwh_dim_terminals_hist
set 
	effective_to = yavl_dwh_dim_terminals_hist.effective_from - interval '1 second',
	effective_from = yavl_dwh_dim_terminals_hist.effective_from - interval '1 day'
where terminal_id in (
	select tgt.terminal_id
	from de12.yavl_dwh_dim_terminals_hist tgt
	left join de12.yavl_stg_terminals stg
		on stg.terminal_id = tgt.terminal_id
	where stg.terminal_id is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N')
  and yavl_dwh_dim_terminals_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and yavl_dwh_dim_terminals_hist.deleted_flg = 'N'
""",
df.values.tolist())

## Формирование таблицы для вычисления удалений de12.yavl_stg_cards_del
# Захват в стейджинг ключей из источника полным срезом для вычисления удалений
cursor_dwh.execute(
     """    insert into de12.yavl_stg_cards_del( card_num, 
                                                      account_num )
            select card_num, 
                   account_num
            from de12.yavl_stg_cards""",
 df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwh_dim_cards_hist
# Загрузка в приемник "вставок" на источнике (формат SCD2)
cursor_dwh.execute(
    """ insert into de12.yavl_dwh_dim_cards_hist( card_num, 
                                                  account_num,
                                                  effective_from,
                                                  effective_to,
                                                  deleted_flg )
            select 
	            stg.card_num, 
	            stg.account_num,
	            stg.update_dt, 
	            to_date('9999-12-31','YYYY-MM-DD'),
	            'N'
            from de12.yavl_stg_cards stg
            left join de12.yavl_dwh_dim_cards_hist tgt
            on stg.card_num = tgt.card_num
            where tgt.card_num is null""",
df.values.tolist())

# Обновление в приемнике "обновлений" на источнике (формат SCD2)
cursor_dwh.execute( """
update de12.yavl_dwh_dim_cards_hist
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.card_num, 
		stg.update_dt 
	from de12.yavl_stg_cards stg
	inner join de12.yavl_dwh_dim_cards_hist tgt
		on stg.card_num = tgt.card_num
		and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null )	      
) tmp
where yavl_dwh_dim_cards_hist.card_num = tmp.card_num
  and yavl_dwh_dim_cards_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 

insert into de12.yavl_dwh_dim_cards_hist( card_num, 
                                          account_num,
                                          effective_from,
                                          effective_to,
                                          deleted_flg )
select 
	stg.card_num, 
	stg.account_num,
	stg.update_dt,
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.yavl_stg_cards stg
inner join de12.yavl_dwh_dim_cards_hist tgt
	on stg.card_num = tgt.card_num
	and tgt.effective_to = update_dt - interval '1 second'
where stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null )
""", df.values.tolist())

# Удаление в приемнике удаленных в источнике записей (формат SCD2)
cursor_dwh.execute( """
insert into de12.yavl_dwh_dim_cards_hist( card_num, 
                                          account_num,
                                          effective_from,
                                          effective_to,
                                          deleted_flg )
select 
	tgt.card_num,
	tgt.account_num,
	now(),
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y'
from de12.yavl_dwh_dim_cards_hist tgt
left join de12.yavl_stg_cards stg
	on stg.card_num = tgt.card_num
where stg.card_num is null
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update de12.yavl_dwh_dim_cards_hist
set 
	effective_to = now() - interval '1 second'
where card_num in (
	select tgt.card_num
	from de12.yavl_dwh_dim_cards_hist tgt
	left join de12.yavl_stg_cards stg
		on stg.card_num = tgt.card_num
	where stg.card_num is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N')
  and yavl_dwh_dim_cards_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and yavl_dwh_dim_cards_hist.deleted_flg = 'N'
""", df.values.tolist())

## Формирование таблицы для вычисления удалений de12.yavl_stg_accounts_del
# Захват в стейджинг ключей из источника полным срезом для вычисления удалений
cursor_dwh.execute(
    """ insert into de12.yavl_stg_accounts_del( account_num,
                                                valid_to,
	                                            client )
        select account_num,
               valid_to,
	           client 
	    from de12.yavl_stg_accounts""",
 df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwh_dim_accounts_hist
# Загрузка в приемник "вставок" на источнике (формат SCD2)
cursor_dwh.execute(
    """ insert into de12.yavl_dwh_dim_accounts_hist( account_num,
                                                     valid_to,
	                                                 client,
                                                     effective_from,
                                                     effective_to,
                                                     deleted_flg )
            select 
	            stg.account_num, 
	            stg.valid_to,
	            stg.client,
	            stg.update_dt, 
	            to_date('9999-12-31','YYYY-MM-DD'),
	            'N'
            from de12.yavl_stg_accounts stg
            left join de12.yavl_dwh_dim_accounts_hist tgt
            on stg.account_num = tgt.account_num
            where tgt.account_num is null""",
df.values.tolist())

# Обновление в приемнике "обновлений" на источнике (формат SCD2)
cursor_dwh.execute( """
update de12.yavl_dwh_dim_accounts_hist
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.account_num, 
		stg.update_dt 
	from de12.yavl_stg_accounts stg
	inner join de12.yavl_dwh_dim_accounts_hist tgt
		on stg.account_num = tgt.account_num
		and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where (stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )) or
	      (stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null ))	       
) tmp
where yavl_dwh_dim_accounts_hist.account_num = tmp.account_num
  and yavl_dwh_dim_accounts_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 

insert into de12.yavl_dwh_dim_accounts_hist( account_num,
                                             valid_to,
	                                         client,
                                             effective_from,
                                             effective_to,
                                             deleted_flg )
select 
	stg.account_num, 
	stg.valid_to,
	stg.client,
	stg.update_dt,
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.yavl_stg_accounts stg
inner join de12.yavl_dwh_dim_accounts_hist tgt
	on stg.account_num = tgt.account_num
	and tgt.effective_to = update_dt - interval '1 second'
where (stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )) or
	  (stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null ))
""", df.values.tolist())

# Удаление в приемнике удаленных в источнике записей (формат SCD2)
cursor_dwh.execute( """
insert into de12.yavl_dwh_dim_accounts_hist( account_num,
                                             valid_to,
	                                         client,
                                             effective_from,
                                             effective_to,
                                             deleted_flg )
select 
	tgt.account_num,
	tgt.valid_to,
	tgt.client,
	now(),
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y'
from de12.yavl_dwh_dim_accounts_hist tgt
left join de12.yavl_stg_accounts stg
	on stg.account_num = tgt.account_num
where stg.account_num is null
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update de12.yavl_dwh_dim_accounts_hist
set 
	effective_to = now() - interval '1 second'
where account_num in (
	select tgt.account_num
	from de12.yavl_dwh_dim_accounts_hist tgt
	left join de12.yavl_stg_accounts stg
		on stg.account_num = tgt.account_num
	where stg.account_num is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N')
  and yavl_dwh_dim_accounts_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and yavl_dwh_dim_accounts_hist.deleted_flg = 'N'
""", df.values.tolist())

## Формирование таблицы для вычисления удалений de12.yavl_stg_clients_del
# Захват в стейджинг ключей из источника полным срезом для вычисления удалений
cursor_dwh.execute(
    """ insert into de12.yavl_stg_clients_del( client_id,
                                               last_name,
                                               first_name,
                                               patronymic,
                                               date_of_birth,
                                               passport_num,
                                               passport_valid_to,
                                               phone )
        select client_id,
               last_name,
               first_name,
               patronymic,
               date_of_birth,
               passport_num,
               passport_valid_to,
               phone 
        from de12.yavl_stg_clients""",
 df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwh_dim_clients_hist
# Загрузка в приемник "вставок" на источнике (формат SCD2)
cursor_dwh.execute(
    """ insert into de12.yavl_dwh_dim_clients_hist(  client_id,
                                                     last_name,
                                                     first_name,
                                                     patronymic,
                                                     date_of_birth,
                                                     passport_num,
                                                     passport_valid_to,
                                                     phone,
                                                     effective_from,
                                                     effective_to,
                                                     deleted_flg )
            select 
	            stg.client_id, 
	            stg.last_name,
	            stg.first_name,
	            stg.patronymic,
	            stg.date_of_birth,
	            stg.passport_num,
	            stg.passport_valid_to,
	            stg.phone,
	            stg.update_dt, 
	            to_date('9999-12-31','YYYY-MM-DD'),
	            'N'
            from de12.yavl_stg_clients stg
            left join de12.yavl_dwh_dim_clients_hist tgt
            on stg.client_id = tgt.client_id
            where tgt.client_id is null""",
df.values.tolist())

# Обновление в приемнике "обновлений" на источнике (формат SCD2)
cursor_dwh.execute( """
update de12.yavl_dwh_dim_clients_hist
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.client_id, 
		stg.update_dt 
	from de12.yavl_stg_clients stg
	inner join de12.yavl_dwh_dim_clients_hist tgt
		on stg.client_id = tgt.client_id
		and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where (stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )) or
	      (stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )) or
	      (stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null )) or
	      (stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null )) or
	      (stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )) or
	      (stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )) or
	      (stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null ))	      
) tmp
where yavl_dwh_dim_clients_hist.client_id = tmp.client_id
  and yavl_dwh_dim_clients_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 

insert into de12.yavl_dwh_dim_clients_hist(  client_id,
                                             last_name,
                                             first_name,
                                             patronymic,
                                             date_of_birth,
                                             passport_num,
                                             passport_valid_to,
                                             phone,
                                             effective_from,
                                             effective_to,
                                             deleted_flg )
select 
	stg.client_id, 
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.update_dt,
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.yavl_stg_clients stg
inner join de12.yavl_dwh_dim_clients_hist tgt
	on stg.client_id = tgt.client_id
	and tgt.effective_to = update_dt - interval '1 second'
where (stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )) or
	  (stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )) or
	  (stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null )) or
	  (stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null )) or
	  (stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )) or
	  (stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )) or
	  (stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null ))
""", df.values.tolist())

# Удаление в приемнике удаленных в источнике записей (формат SCD2)
cursor_dwh.execute( """
insert into de12.yavl_dwh_dim_clients_hist( client_id,
                                            last_name,
                                            first_name,
                                            patronymic,
                                            date_of_birth,
                                            passport_num,
                                            passport_valid_to,
                                            phone,
                                            effective_from,
                                            effective_to,
                                            deleted_flg )
select 
	tgt.client_id,
	tgt.last_name,
	tgt.first_name,
	tgt.patronymic,
	tgt.date_of_birth,
	tgt.passport_num,
	tgt.passport_valid_to,
	tgt.phone,	
	now(),
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y'
from de12.yavl_dwh_dim_clients_hist tgt
left join de12.yavl_stg_clients stg
	on stg.client_id = tgt.client_id
where stg.client_id is null
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update de12.yavl_dwh_dim_clients_hist
set 
	effective_to = now() - interval '1 second'
where client_id in (
	select tgt.client_id
	from de12.yavl_dwh_dim_clients_hist tgt
	left join de12.yavl_stg_clients stg
		on stg.client_id = tgt.client_id
	where stg.client_id is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N')
  and yavl_dwh_dim_clients_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and yavl_dwh_dim_clients_hist.deleted_flg = 'N'
""", df.values.tolist())

## Импорт значений из STAGE в целевую фактовую таблицу de12.yavl_dwh_fact_passport_blacklist
# Запись DataFrame в таблицу базы данных
cursor_dwh.execute(
    """INSERT INTO de12.yavl_dwh_fact_passport_blacklist  
            select  entry_dt, 
                    passport_num  
            from  de12.yavl_stg_blacklist""",
df.values.tolist())

## Импорт значений из STAGE в целевую фактовую таблицу de12.yavl_dwh_fact_transactions
# Запись DataFrame в таблицу базы данных
cursor_dwh.execute(
    """ INSERT INTO de12.yavl_dwh_fact_transactions 
            select  stg.trans_id,
            	    stg.trans_date,
	                stg.card_num,
	                stg.oper_type,
	                stg.amt,
	                stg.oper_result,
	                stg.terminal 
	        from de12.yavl_stg_transactions stg
	        left join de12.yavl_dwh_fact_transactions tgt
            on stg.trans_id = tgt.trans_id
            where tgt.trans_id is null""",
    df.values.tolist())

## Формирование таблицы de12.yavl_rep_fraud по истекшим паспортам

# Запись DataFrame в таблицу базы данных
cursor_dwh.execute(
    """ insert into de12.yavl_rep_fraud ( event_dt, 
                                          passport, 
                                          fio, 
                                          phone, 
                                          event_type, 
                                          report_dt )
            select 	distinct cast (trans_date as date), 
		                     cli.passport_num,
		                     last_name || ' ' || first_name || ' ' || patronymic, 
		                     phone, 
		                     '1',
		                     now()
            from de12.yavl_dwh_fact_transactions tra
            left join de12.yavl_dwh_dim_cards_hist car 
	            on tra.card_num = trim(car.card_num)
            left join de12.yavl_dwh_dim_accounts_hist acc 
	            on car.account_num = acc.account_num
            left join de12.yavl_dwh_dim_clients_hist cli 
	            on acc.client = cli.client_id
            left join de12.yavl_dwh_fact_passport_blacklist pss 
	            on cli.passport_num = pss.passport_num
            where trans_date > passport_valid_to or entry_dt <= trans_date
            and not exists (  select event_dt, fio
                                from de12.yavl_rep_fraud fra 
                                where fra.passport = cli.passport_num and tra.trans_date = fra.event_dt)""",
df.values.tolist())

## Формирование таблицы de12.yavl_rep_fraud по истекшим договорам
# Запись DataFrame в таблицу базы данных
cursor_dwh.execute(
    """ insert into de12.yavl_rep_fraud ( event_dt, 
                                          passport, 
                                          fio, 
                                          phone, 
                                          event_type, 
                                          report_dt )
            select distinct cast (trans_date as date), 
                            cli.passport_num,
					        last_name || ' ' || first_name || ' ' || patronymic, 
					        phone, 
					        '2',
					        now()
            from de12.yavl_dwh_fact_transactions tra
            left join de12.yavl_dwh_dim_cards_hist car 
	            on tra.card_num = trim(car.card_num)
            left join de12.yavl_dwh_dim_accounts_hist acc 
	            on car.account_num = acc.account_num
            left join de12.yavl_dwh_dim_clients_hist cli 
	            on acc.client = cli.client_id
            where trans_date > valid_to
            and not exists (  select event_dt, fio
                                from de12.yavl_rep_fraud fra 
                                where fra.passport = cli.passport_num and tra.trans_date = fra.event_dt)""",
df.values.tolist())

# Фиксируем изменения
conn_src.commit()
conn_dwh.commit()

# Закрываем соединение
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()

os.rename('/home/de12/yavl/project/transactions_03032021.txt' , '/home/de12/yavl/project/archive/transactions_03032021.txt.backup')
os.rename('/home/de12/yavl/project/terminals_03032021.xlsx', '/home/de12/yavl/project/archive/terminals_03032021.xlsx.backup')
os.rename('/home/de12/yavl/project/passport_blacklist_03032021.xlsx', '/home/de12/yavl/project/archive/passport_blacklist_03032021.xlsx.backup')
