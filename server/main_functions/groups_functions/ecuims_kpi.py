from sqlalchemy.engine import create_engine
from server.cfg import damage_odbc
import pandas as pd


def ecuims_kpi(dt_start, dt_end, groups):
    groups = "'" + "','".join(groups) + "'"
    engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    with engine.connect() as con:
        statistic = pd.read_sql_query("""select t.*, t1.[Количество открытых по группе], t2.[Количество открытых по раб. месту], (case when cast(t2.[Количество открытых по раб. месту] as decimal(10)) = 0.0  or cast(t2.[Количество открытых по раб. месту] as decimal(10)) is null then cast(t2.[Количество открытых по раб. месту] as decimal(10))
                                                                                                                    else (cast(t.[Количество открытых] as decimal(10))+cast(t.[Количество закрытых]  as decimal(10)))/(cast(t2.[Количество открытых по раб. месту] as decimal(10))) end)
                                                                                                                     as Расчёт from (
                            select aggr.*, REPLACE(REPLACE(LTRIM(rTrim(u.groups)), CHAR(13), ''), CHAR(10), '') groups, REPLACE(REPLACE(LTRIM(rTrim(u.smena)), CHAR(13), ''), CHAR(10), '') smena
                             from (
                            select sum(allinfo.[Количество открытых]) [Количество открытых], sum(allinfo.[Количество закрытых]) [Количество закрытых], uid, fio from (
                            select (select count(id) from (select * from (select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm union all select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm_m) n
                              where close_id is not null or open_id is not null) b where b.open_id = uid and convert(date,[Дата и время начала],23) = dt) [Количество открытых],
                              (select count(id) from (select * from (select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm union all select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm_m) n
                              where close_id is not null or open_id is not null) b where b.open_id = uid and convert(date,[Дата и время конца],23) = dt) [Количество закрытых],
                               * from (
                              select distinct Users.id as uid, fio, dt from Users join (select distinct convert(date,dt,23) dt from (select [Дата и время конца] as dt from esm union all select [Дата и время начала] as dt from esm) b
                              union all select distinct convert(date,dt,23) dt from (select [Дата и время конца] as dt from esm_m union all select [Дата и время начала] as dt from esm_m) b1) table1 on 1=1) bt
                              ) allinfo where allinfo.dt >='""" + dt_start + """' and allinfo.dt <='""" + dt_end + """' group by uid,fio)  aggr
                              join Users u on u.id = aggr.uid  where REPLACE(REPLACE(LTRIM(rTrim(groups)), CHAR(13), ''), CHAR(10), '') in (""" + groups + """)
                            ) t
                            join  (
                            select sum(aggr.[Количество открытых]) as [Количество открытых по группе], REPLACE(REPLACE(LTRIM(rTrim(u.groups)), CHAR(13), ''), CHAR(10), '') groups
                             from (
                            select sum(allinfo.[Количество открытых]) [Количество открытых], sum(allinfo.[Количество закрытых]) [Количество закрытых], uid, fio from (
                            select (select count(id) from (select * from (select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm union all select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm_m) n
                              where close_id is not null or open_id is not null) b where b.open_id = uid and convert(date,[Дата и время начала],23) = dt) [Количество открытых],
                              (select count(id) from (select * from (select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm union all select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm_m) n
                              where close_id is not null or open_id is not null) b where b.open_id = uid and convert(date,[Дата и время конца],23) = dt) [Количество закрытых],
                               * from (
                              select distinct Users.id as uid, fio, dt from Users join (select distinct convert(date,dt,23) dt from (select [Дата и время конца] as dt from esm union all select [Дата и время начала] as dt from esm) b
                              union all select distinct convert(date,dt,23) dt from (select [Дата и время конца] as dt from esm_m union all select [Дата и время начала] as dt from esm_m) b1) table1 on 1=1) bt
                              ) allinfo where allinfo.dt >='""" + dt_start + """' and allinfo.dt <='""" + dt_end + """' group by uid,fio)  aggr
                              join Users u on u.id = aggr.uid  where REPLACE(REPLACE(LTRIM(rTrim(groups)), CHAR(13), ''), CHAR(10), '') in (""" + groups + """)
                             group by REPLACE(REPLACE(LTRIM(rTrim(u.groups)), CHAR(13), ''), CHAR(10), '') ) t1 on t1.groups= t.groups
                             join  (
                            select sum(aggr.[Количество открытых]) as [Количество открытых по раб. месту], REPLACE(REPLACE(LTRIM(rTrim(u.smena)), CHAR(13), ''), CHAR(10), '') smena
                             from (
                            select sum(allinfo.[Количество открытых]) [Количество открытых], sum(allinfo.[Количество закрытых]) [Количество закрытых], uid, fio from (
                            select (select count(id) from (select * from (select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm union all select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm_m) n
                              where close_id is not null or open_id is not null) b where b.open_id = uid and convert(date,[Дата и время начала],23) = dt) [Количество открытых],
                              (select count(id) from (select * from (select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm union all select id, [Дата и время начала], [Дата и время конца], open_id, close_id from esm_m) n
                              where close_id is not null or open_id is not null) b where b.open_id = uid and convert(date,[Дата и время конца],23) = dt) [Количество закрытых],
                               * from (
                              select distinct Users.id as uid, fio, dt from Users join (select distinct convert(date,dt,23) dt from (select [Дата и время конца] as dt from esm union all select [Дата и время начала] as dt from esm) b
                              union all select distinct convert(date,dt,23) dt from (select [Дата и время конца] as dt from esm_m union all select [Дата и время начала] as dt from esm_m) b1) table1 on 1=1) bt
                              ) allinfo where allinfo.dt >='""" + dt_start + """' and allinfo.dt <='""" + dt_end + """' group by uid,fio)  aggr
                              join Users u on u.id = aggr.uid  where REPLACE(REPLACE(LTRIM(rTrim(groups)), CHAR(13), ''), CHAR(10), '') in (""" + groups + """)
                             group by REPLACE(REPLACE(LTRIM(rTrim(u.smena)), CHAR(13), ''), CHAR(10), '')) t2 on t2.smena= t.smena
                              order by  (case when cast(t2.[Количество открытых по раб. месту] as decimal(10)) = 0.0  or cast(t2.[Количество открытых по раб. месту] as decimal(10)) is null then cast(t2.[Количество открытых по раб. месту] as decimal(10))
                                        else (cast(t.[Количество открытых] as decimal(10))+cast(t.[Количество закрытых]  as decimal(10)))/(cast(t2.[Количество открытых по раб. месту] as decimal(10))) end)""", con)
    con.close()
    statistic = statistic.round({'Расчёт': 3})
    return statistic.to_json(orient="table")