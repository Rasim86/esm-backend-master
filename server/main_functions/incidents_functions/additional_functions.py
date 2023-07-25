# -*- coding: utf-8 -*-
from sqlalchemy.engine import create_engine
from server.cfg import damage_odbc
import urllib
from server.cfg import home_dir


def add_incident_log(data):
    query_options = f"""INSERT INTO Temp_Users (id_d, fio, email, f_m)
        VALUES ({data['id_d']},
        '{data['fio']}',
        '{data['email']}',
        {data['f_m']})"""
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
    con.close()
    return True


def add_hospis_log(data):
    query_options = f"""INSERT INTO sicks_deleter (date, idrues, block, deleter)
        VALUES ('{data['Date']}',
        '{data['rues_id']}',
        '{data['block_id']}',
        '{data['editor']}')"""
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
    con.close()
    return True


def create_rsc_alert(bs_num, time_start, id):
    time_start = time_start.split('.')
    time_start = time_start[0].replace('T', ' ')
    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    USERNAME = 'otrs2'
    PASSWORD = 'Tr1chogaster'
    HOST = '192.168.100.137'
    PORT = 1521
    SERVICE = 'm2000'
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(
        PORT) + '/?service_name=' + SERVICE
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    sql_file = open(f'{home_dir}/esm-socket-backend/server/external/create_rsc_alert.sql', encoding="utf-8")
    sql_as_string = sql_file.read().format(bs_num, time_start, id)
    with engine.connect() as con:
        try:
            con.execute(sql_as_string)
        except Exception:
            pass
    con.close()


def del_rsc_alert(id):
    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    USERNAME = 'otrs2'
    PASSWORD = 'Tr1chogaster'
    HOST = '192.168.100.137'
    PORT = 1521
    SERVICE = 'm2000'
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(
        PORT) + '/?service_name=' + SERVICE
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    sql_file = open(f'{home_dir}/esm-socket-backend/server/external/delete_rsc_alert.sql', encoding="utf-8")
    sql_as_string = sql_file.read().format(id)
    with engine.connect() as con:
        rs = con.execute(sql_as_string)
    try:
        rsc_id = rs.fetchall()[0][0]
    except Exception:
        rsc_id = None
    con.close()
    if rsc_id != None:
        with engine.connect() as con:
            try:
                con.execute("""BEGIN
                                    M_TTK.RSC_ORDER_MANAGER.DELETE_ORDER(""" + str(rsc_id) + """,1);
                                  commit;
                                  END;""")
            except Exception:
                pass
        con.close()
