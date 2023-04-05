select '(stg.' || c.column_name || ' <> tgt.' || c.column_name || ' or ( stg.' || c.column_name || ' is null and tgt.' || c.column_name || ' is not null ) or ( stg.' || c.column_name || ' is not null and tgt.' || c.column_name || ' is null ))'
from information_schema."columns" c 
where table_schema ='de12' and table_name = 'yavl_stg_clients'