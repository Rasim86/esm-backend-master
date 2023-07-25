from server.main_functions.incidents_functions.additional_functions import *
from sqlalchemy.engine import create_engine
from server.cfg import damage_odbc
import pandas as pd
import urllib, json

class CheckSickTraceback(Exception):
    pass


def sick_zues():
    response_object = {'status': 'success'}
    query_options = """SELECT * FROM sicks_nras"""
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        zues_list = pd.read_sql_query(query_options, con)
    con.close()
    zues_list = pd.pivot_table(zues_list, index='zues_name', values='zues_id')
    response_object['sick_info'] = zues_list.to_json(orient="table")
    return response_object


def sick_rues(zues_id):
    response_object = {'status': 'success'}
    query_options = f"""SELECT * FROM sicks_nras WHERE zues_id = {zues_id}"""
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        rues_list = pd.read_sql_query(query_options, con)
    con.close()
    response_object['sick_info'] = rues_list.to_json(orient="table")
    return response_object


def sick_block(rues_id, zues_id):
    response_object = {'status': 'success'}
    query_options = f"select *  from block where adm_state = 1 and rues_id in (select rues_id from sicks_nras where zues_id = {zues_id}) "
    if rues_id != None:
        query_options += f" AND rues_id = {rues_id}"
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        block_list = pd.read_sql_query(query_options, con)
    con.close()
    response_object['sick_info'] = block_list.to_json(orient="table")
    return response_object


def sick_list(data):
    response_object = {'status': 'success'}
    if data.get('zues_id') == None:
        zues_id = 999
    else:
        zues_id = data.get('zues_id')
    query_options = f"""SELECT * FROM sicks WHERE date = '{data.get('date')}' AND zues_id = {zues_id}"""
    if data.get('rues_id') != None:
        query_options += f" AND rues_id = {data.get('rues_id')}"
    if data.get('block_id') != None:
        query_options += f" AND block_id = {data.get('block_id')}"
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        sickers = pd.read_sql_query(query_options, con)
        sick_bloks = sickers['block_id'].unique().tolist()
        if len(sick_bloks) != 0:
            fio_sick = pd.read_sql_query(
                f"SELECT * FROM FIO WHERE Date = '{data.get('date')}' AND block_id IN ({','.join(str(block) for block in sick_bloks)})", con)
            fio_prolong = pd.read_sql_query(
                f"SELECT * FROM FIOprolongation WHERE Date = '{data.get('date')}' AND block_id IN ({','.join(str(block) for block in sick_bloks)})", con)
            fio_sick = fio_sick.groupby('block_id')['fio'].apply(list)
            fio_prolong = fio_prolong.groupby('block_id')['fio'].apply(list)
            fio_prolong.name = 'fio_prolong'
            sickers = sickers.merge(fio_prolong.to_frame(), left_on='block_id', right_index=True, how='left')
            sickers = sickers.merge(fio_sick.to_frame(), left_on='block_id', right_index=True, how='left')
    con.close()
    response_object['sick_list'] = sickers.to_json(orient="table")
    return response_object


def check_sick(emit_data):
    data = emit_data['incidentData']
    query_check = f"SELECT id FROM sicks WHERE date = '{data['Date']}' AND block_id = {data['block_id']}"
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        check = pd.read_sql_query(query_check, con)
        if check.shape[0] != 0:
            return False
        else:
            return True


def edit_sick(emit_data):
    data = emit_data['incidentData']
    query_check = f"SELECT id FROM sicks WHERE date = '{data['Date']}' AND block_id = {data['block_id']}"
    query_delete = f"DELETE FROM sicks WHERE date = '{data['Date']}' AND block_id = {data['block_id']}"
    query_add_report = f"""INSERT INTO sicks (Date, rues_id, zues_id, block_id, bolnich, bolnica, home, siks, Chronic, injury, Caring_for_siks, confirmed, prolongation, editor)
        VALUES ('{data['Date']}',
        {data['rues_id']},
        {data['zues_id']},
        {data['block_id']},
        {data['bolnich']},
        {data['bolnica']},
        {data['home']},
        {data['siks']},
        {data['Chronic']},
        {data['injury']},
        {data['Caring_for_siks']},
        {data['confirmed']},
        {data['prolongation']},
        '{data['editor']}')"""
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        check = pd.read_sql_query(query_check, con)
        if check.shape[0] != 0:
            add_hospis_log(data)
            con.execute(query_delete)
            emit_data['incidentType'] = 'edit'
            emit_data['removeID'] = int(check.loc[0]['id'])
        for name in data['fio']:
            query_add_fio = f"""INSERT INTO FIO (Date, rues_id, block_id, fio)
                VALUES ('{data['Date']}',
                {data['rues_id']},
                {data['block_id']},
                '{name}')"""
            con.execute(query_add_fio)
        for name in data['fio_prolong']:
            query_add_prolong_fio = f"""INSERT INTO FIOprolongation (Date, rues_id, block_id, fio)
                VALUES ('{data['Date']}',
                {data['rues_id']},
                {data['block_id']},
                '{name}')"""
            con.execute(query_add_prolong_fio)
        con.execute(query_add_report)
        rs = pd.read_sql_query("SELECT id FROM sicks WHERE id = SCOPE_IDENTITY()", con)
    con.close()
    emit_data['incidentData'] = data
    emit_data['incidentData']['id'] = int(rs.loc[0]['id'])
    return emit_data


def delete_sick(data):
    add_hospis_log(data)
    query_delete = f"DELETE FROM sicks WHERE date = '{data['Date']}' AND block_id = {data['block_id']}"
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_delete)
    con.close()