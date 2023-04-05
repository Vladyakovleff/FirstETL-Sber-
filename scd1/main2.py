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
                        delete from de12.yavl_stg_clients;""" )
conn_dwh.commit()

## Импортируем transactions_03032021.txt
# Чтение из файла
df = pd.read_csv( '/home/de12/yavl/project/transactions_03032021.txt', sep=';', header=0, index_col=None )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_stg_transactions(  transactions_id, 
                                                transactions_date, 
                                                amount,
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
    VALUES( %s, %s, %s, %s, now() )""",
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

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwn_dim_terminals
# Выполнение SQL кода в базе данных с возвратом результата
cursor_dwh.execute( """ SELECT 
                            terminal_id, 
                            terminal_type, 
                            terminal_city, 
                            update_dt
                        FROM de12.yavl_stg_terminals""" )
records = cursor_dwh.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_dwh.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ insert into de12.yavl_dwn_dim_terminals( terminal_id, terminal_type, terminal_city, create_dt, update_dt )
        select 
	        stg.terminal_id, 
	        stg.terminal_type, 
            stg.terminal_city,
	        stg.update_dt, 
	        null 
        from de12.yavl_stg_terminals stg
        left join de12.yavl_dwn_dim_terminals tgt
        on stg.terminal_id = tgt.terminal_id
        where tgt.terminal_id is null""",
df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwn_dim_cards
# Выполнение SQL кода в базе данных с возвратом результата
cursor_dwh.execute( """ SELECT 
                            card_num,
                            account_num,
                            update_dt
                        FROM de12.yavl_stg_cards""" )
records = cursor_dwh.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_dwh.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ insert into de12.yavl_dwn_dim_cards( card_num, account_num, create_dt, update_dt )
        select 
	        stg.card_num, 
	        stg.account_num, 
	        stg.update_dt, 
	        null 
        from de12.yavl_stg_cards stg
        left join de12.yavl_dwn_dim_cards tgt
        on stg.card_num = tgt.card_num
        where tgt.card_num is null""",
df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwn_dim_accounts
# Выполнение SQL кода в базе данных с возвратом результата
cursor_dwh.execute( """ SELECT 
                            account_num,
                            valid_to,
                            client,
                            update_dt
                        FROM de12.yavl_stg_accounts""" )
records = cursor_dwh.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_dwh.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ insert into de12.yavl_dwn_dim_accounts( account_num, valid_to, client, create_dt, update_dt )
        select 
	        stg.account_num, 
	        stg.valid_to,
	        stg.client, 
	        stg.update_dt, 
	        null 
        from de12.yavl_stg_accounts stg
        left join de12.yavl_dwn_dim_accounts tgt
        on stg.account_num = tgt.account_num
        where tgt.account_num is null""",
df.values.tolist())

## Импорт значений из STAGE в целевую таблицу de12.yavl_dwn_dim_clients
# Выполнение SQL кода в базе данных с возвратом результата
cursor_dwh.execute( """ SELECT 
                            client_id,
	                        last_name,
	                        first_name,
	                        patronymic,
	                        date_of_birth,
	                        passport_num,
	                        passport_valid_to,
	                        phone,
                            update_dt
                        FROM de12.yavl_stg_clients""" )
records = cursor_dwh.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_dwh.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ insert into de12.yavl_dwn_dim_clients(  client_id,
	                                            last_name,
	                                            first_name,
	                                            patronymic,
	                                            date_of_birth,
	                                            passport_num,
	                                            passport_valid_to,
	                                            phone, 
	                                            create_dt, 
	                                            update_dt )
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
	        null 
        from de12.yavl_stg_clients stg
        left join de12.yavl_dwn_dim_clients tgt
        on stg.client_id = tgt.client_id
        where tgt.client_id is null""",
df.values.tolist())

## Импорт значений из STAGE в целевую фактовую таблицу de12.yavl_dwn_fact_passport_blacklist
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """INSERT INTO de12.yavl_dwn_fact_passport_blacklist  
            select  entry_dt, 
                    passport_num  
            from  de12.yavl_stg_blacklist""",
df.values.tolist())

## Импорт значений из STAGE в целевую фактовую таблицу de12.yavl_dwn_fact_transactions
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ INSERT INTO de12.yavl_dwn_fact_transactions 
            select  transactions_id,
            	    transactions_date,
	                card_num,
	                oper_type,
	                amount,
	                oper_result,
	                terminal 
	        from de12.yavl_stg_transactions""",
    df.values.tolist())

## Формирование таблицы de12.yavl_rep_fraud по истекшим паспортам
# Выполнение SQL кода в базе данных с возвратом результата
cursor_src.execute( """ select 	    client_id
                        from info.clients
                        where passport_valid_to < now() and passport_valid_to is not null""" )
records = cursor_src.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ insert into de12.yavl_rep_fraud (client_id, blcklst_reason, report_dt) 
        VALUES( %s, 'passport_expired', now() )""",
df.values.tolist())

## Формирование таблицы de12.yavl_rep_fraud по истекшим договорам
# Выполнение SQL кода в базе данных с возвратом результата
cursor_src.execute( """ select 	    client as client_id
                        from info.accounts
                        where valid_to < now() """ )
records = cursor_src.fetchall()
# Формирование DataFrame
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )
# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany(
    """ insert into de12.yavl_rep_fraud (client_id, blcklst_reason, report_dt) 
        VALUES( %s, 'contract_expired', now() )""",
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


































