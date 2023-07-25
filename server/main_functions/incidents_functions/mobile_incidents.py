from sqlalchemy import create_engine, exc
from server.cfg import damage_odbc
import urllib, json
import pandas as pd
import datetime
import traceback
import threading
from server.main_functions.groups_functions.gou_functions import hhmm_to_dect
from server.main_functions.incidents_functions.additional_functions import *


def create_mobile_incident(data):
    try:
        if data.get('foto') == None:
            photo = ''
        else:
            photo = data['foto']
        if data['timeEnd'] != None and data['timeEnd'] != '' :
            time_end = {'value' : f"'{data['timeEnd'].replace('T', ' ')}',", 'label': 'DateEnd,'}
        else:
            time_end = {'value' : '', 'label': ''}
        problem_text = f"""{datetime.datetime.strptime(data['timeStart'], '%Y-%m-%dT%H:%M').strftime('%d.%m %H:%M')} {data['problem'].replace("'", '"')}"""
        if data['toRues'] == True:
            if datetime.datetime.now().strftime('%d.%m') == datetime.datetime.strptime(data['timeStart'], '%Y-%m-%dT%H:%M').strftime('%d.%m'):
                problem_text += f"\n{datetime.datetime.now().strftime('%H:%M')} передано в группу ЗУЭС"
            else:
                problem_text += f"\n{datetime.datetime.now().strftime('%d.%m %H:%M')} передано в группу ЗУЭС"
        main_options = f"""INSERT INTO DmgM (DateBeg, {time_end['label']} nras, Name, NBC, id_t, LTE_FDD, LTE_TDD, GSM0, Txt, TypeEq, TypeL, sec, secN, Class, ChOut, incident, groupp, editedByGou, onControl, important, photo)
         VALUES ('{data['timeStart'].replace('T', ' ')}',
                {time_end['value']}
                {data['idrues']}, 
                '{data['address']}', 
                {data['bs_num']}, 
                {data['idtechn3']}, 
                '{data['LTE_FDD']}',
                '{data['LTE_TDD']}', 
                '{data['GSM']}', 
                '{problem_text}',
                {data['type_equ_id']}, 
                {data['type_line_id']}, 
                {data['sectors'][0]}, 
                {data['sectors'][1]}, 
                {data['klass']}, 
                '{data['Y']}', 
                '{data['incident']}',
                'bs',
                {data['editedByGou']},
                {data['onControl']},
                {data['important']},
                '{photo}')"""
        params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
        ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
        engine = create_engine(ENGINE_PATH_WIN_AUTH)
        with engine.connect() as con:
            con.execute(main_options)
            rs = pd.read_sql_query("""SELECT [editedByGou], [GSM], [ID], [LTE], [LTE_FDD], [LTE_TDD],
                     [Y], [close_id], [flNot], [groupp] as [group], [idrues], [idtechn1], [idtechn2], [idtechn3], [important],
                     [incident], [onControl], [photo],
                     [type_equ_id], [type_line_id], [Время простоя с учетом ночи], [Дата и время конца] as
                     [dataEnd],[Дата и время начала] as [dataStart], [ЗУЭС] as [zues], [Класс] as [klass], [Объект] as [address],
                     [РУЭС] as [rues], [Секторы] as [sectors], [Текст] as [problem], [Технология 1] as [tech1],
                     [Технология 2] as [tech2], [Технология 3] as [tech3], [Примечание] as [editing],
                     [Тип линии] as [line_type], [Тип оборудования] as [device_type], [№ БС] as [bs_num] FROM esm_m WHERE ID = SCOPE_IDENTITY()""", con)
        con.close()
        rs = rs.to_json(orient="table")
        parsed = json.loads(rs)
        parsed['data'][0]['uslugi_all'] = 'БС ' + str(parsed['data'][0]['bs_num'])
        if data['timeEnd'] != None and data['timeEnd'] != '':
            params = urllib.parse.quote_plus(
                'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
            ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
            engine = create_engine(ENGINE_PATH_WIN_AUTH)
            with engine.connect() as con:
                if data['GSM'] == data['LTE_FDD'] == data['LTE_TDD'] == False:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({parsed['data'][0]['ID']},
                             '1',
                             '1',
                             '{data['timeEnd'].replace('T', ' ')}',
                             '0')""")
                if data['GSM'] == True:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({parsed['data'][0]['ID']},
                             '1',
                             '1',
                             '{data['timeEnd'].replace('T', ' ')}',
                             {data['sectors'][0]})""")
                if data['LTE_FDD'] == True:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({parsed['data'][0]['ID']},
                             '2',
                             '2',
                             '{data['timeEnd'].replace('T', ' ')}',
                             {data['sectors'][0]})""")
                if data['LTE_TDD'] == True:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({parsed['data'][0]['ID']},
                             '3',
                             '3',
                             '{data['timeEnd'].replace('T', ' ')}',
                             {data['sectors'][0]})""")
            con.close()
        if parsed['data'][0]['Y']:
            try:
                rsc_thread = threading.Thread(target=create_rsc_alert, args=(str(parsed['data'][0]['bs_num']), parsed['data'][0]['dataStart'], str(parsed['data'][0]['ID'])))
                rsc_thread.daemon = True
                rsc_thread.start()
            except Exception:
                print(traceback.format_exc())
        return parsed['data'][0]
    except exc.DBAPIError as e:
        if str(e).find('was deadlocked on lock ') != -1:
            return create_mobile_incident(data)
        else:
            raise Exception(e)


def edit_mobile_incident(data):
    if data['editedByGou'] == None:
        editedByGou = 0
    else:
        editedByGou = data['editedByGou']
    if data.get('foto') == None:
        photo = ''
    else:
        photo = data['foto']
    if data['timeEnd'] != None and data['timeEnd'] != '':
        time_end = f"DateEnd = '{data['timeEnd'].replace('T', ' ')}',"
    else:
        time_end = 'DateEnd = NULL,'
    main_options = f"""UPDATE DmgM
     SET DateBeg = '{data['timeStart'].replace('T', ' ')}',
        {time_end}
        nras = '{data['idrues']}',
        Name = '{data['address']}',
        NBC = '{data['bs_num']}',
        id_t = '{data['idtechn3']}',
        LTE_FDD = '{data['LTE_FDD']}',
        LTE_TDD = '{data['LTE_TDD']}',
        GSM0 = '{data['GSM']}',
        Txt = '{data['problem'].replace("'", '"')}',
        TypeEq = {data['type_equ_id']},
        TypeL = {data['type_line_id']},
        sec = '{data['sectors'][0]}',
        secN = '{data['sectors'][1]}',
        Class = '{data['klass']}',
        ChOut = '{data['Y']}',
        incident = '{data['incident']}',
        groupp = 'bs',
        editedByGou = {editedByGou},
        onControl = {data['onControl']},
        important = {data['important']},
        photo = '{photo}'
     WHERE ID = {data['ID']}"""
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(main_options)
        rv = con.execute(f"SELECT * FROM DmgSrvM WHERE ID_D = {data['ID']}").fetchall()
        if len(rv) != 0:
            con.execute(f"DELETE FROM DmgSrvM WHERE ID_D = {data['ID']}")
        if data['timeEnd'] != None and data['timeEnd'] != '':
            if len(data['serviceData']) != 0:
                additional_seconds = 0
                for service in data['serviceData']:
                    additional_seconds += 1
                    f_date = datetime.datetime.strptime(service['DateEnd'], '%Y-%m-%dT%H:%M')
                    f_date = f_date.replace(second=additional_seconds)
                    if service['dateAss'] != None and service['dateAss'] != '':
                        dateAss = {'value': f""",'{service["dateAss"].replace("T", " ")}'""", 'label': ',dateAss'}
                    else:
                        dateAss = {'value': '', 'label': ''}
                    if service['DownTime0'] != None and service['DownTime0'] != '':
                        DownTime0 = {'value': f",{hhmm_to_dect(service['DownTime0'])}", 'label': ',DownTime0'}
                    else:
                        DownTime0 = {'value': '', 'label': ''}
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors {dateAss['label']} {DownTime0['label']})
                         VALUES ({service['ID_D']},
                             {service['Srv_type_id']},
                             '{service['Srv_type_id']}',
                             '{f_date}',
                             {service['Sectors']}
                             {dateAss['value']}
                             {DownTime0['value']})""")
            else:
                if data['GSM'] == data['LTE_FDD'] == data['LTE_TDD'] == False:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({data['ID']},
                             '1',
                             '1',
                             '{data['timeEnd'].replace('T', ' ')}',
                             '0')""")
                if data['GSM'] == True:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({data['ID']},
                             '1',
                             '1',
                             '{data['timeEnd'].replace('T', ' ')}',
                             {data['sectors'][0]})""")
                if data['LTE_FDD'] == True:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({data['ID']},
                             '2',
                             '2',
                             '{data['timeEnd'].replace('T', ' ')}',
                             {data['sectors'][0]})""")
                if data['LTE_TDD'] == True:
                    con.execute(
                        f"""INSERT INTO DmgSrvM (ID_D, Srv_type_id, Srv_type, DateEnd, Sectors)
                         VALUES ({data['ID']},
                             '3',
                             '3',
                             '{data['timeEnd'].replace('T', ' ')}',
                             {data['sectors'][0]})""")

        rs = pd.read_sql_query(f"""SELECT [editedByGou], [GSM], [ID], [LTE], [LTE_FDD], [LTE_TDD],
         [Y], [close_id], [flNot], [groupp] as [group], [idrues], [idtechn1], [idtechn2], [idtechn3], [important], 
         [incident], [onControl], [photo], 
         [type_equ_id], [type_line_id], [Время простоя с учетом ночи], [Дата и время конца] as 
         [dataEnd],[Дата и время начала] as [dataStart], [ЗУЭС] as [zues], [Класс] as [klass], [Объект] as [address],
         [РУЭС] as [rues], [Секторы] as [sectors], [Текст] as [problem], [Технология 1] as [tech1],
         [Технология 2] as [tech2], [Технология 3] as [tech3], [Примечание] as [editing],
         [Тип линии] as [line_type], [Тип оборудования] as [device_type], [№ БС] as [bs_num] FROM esm_m WHERE ID = {data['ID']}""", con)
    con.close()
    rs = rs.to_json(orient="table")
    parsed = json.loads(rs)
    parsed['data'][0]['uslugi_all'] = 'БС ' + str(parsed['data'][0]['bs_num'])
    if parsed['data'][0]['Y']:
        try:
            del_rsc_alert(str(parsed['data'][0]['ID']))
            rsc_thread = threading.Thread(target=create_rsc_alert, args=(str(parsed['data'][0]['bs_num']), parsed['data'][0]['dataStart'], str(parsed['data'][0]['ID'])))
            rsc_thread.daemon = True
            rsc_thread.start()
        except Exception:
            print(traceback.format_exc())
    else:
        try:
            del_rsc_alert(str(parsed['data'][0]['ID']))
        except Exception:
            print(traceback.format_exc())
    return parsed['data'][0]


def delete_mobile_incident(data):
    query_options = "DELETE FROM DmgM WHERE ID = " + str(data['ID'])
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
    con.close()
    try:
        del_rsc_alert(str(data['ID']))
    except Exception:
        print(traceback.format_exc())