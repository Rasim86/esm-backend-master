from sqlalchemy import create_engine, exc
import sqlalchemy
from server.cfg import damage_odbc
from server.main_functions.groups_functions.gou_functions import hhmm_to_dect
import urllib, json
import pandas as pd
import datetime


def create_fixed_incident(data):
    try:
        if data['foto'] == None:
            photo = ''
        else:
            photo = data['foto']
        if data['ota'] == None:
            ota = 0
        else:
            ota = data['ota']
        if data['spd'] == None:
            spd = 0
        else:
            spd = data['spd']
        if data['sip'] == None:
            sip = 0
        else:
            sip = data['sip']
        if data['iptv'] == None:
            iptv = 0
        else:
            iptv = data['iptv']
        if data['ktv'] == None:
            ktv = 0
        else:
            ktv = data['ktv']
        problem_text = f"""{datetime.datetime.strptime(data['timeStart'], '%Y-%m-%dT%H:%M').strftime('%d.%m %H:%M')} {data['problem'].replace("'", '"')}"""
        if sum([int(spd), int(iptv), int(sip), int(ktv)], int(ota)) > 0:
            problem_text += '\nУслуги: '
            if int(ota) > 0:
                problem_text += f"{ota}ота "
            if int(spd) > 0:
                problem_text += f"{spd}шпд "
            if int(iptv) > 0:
                problem_text += f"{iptv}аб.IP-TV "
            if int(sip) > 0:
                problem_text += f"{sip}аб.sip "
            if int(ktv) > 0:
                problem_text += f"{ktv}КТВ "
        if data['toRues'] == True:
            if datetime.datetime.now().strftime('%d.%m') == datetime.datetime.strptime(data['timeStart'], '%Y-%m-%dT%H:%M').strftime('%d.%m'):
                problem_text += f"\n{datetime.datetime.now().strftime('%H:%M')} передано в группу ЗУЭС"
            else:
                problem_text += f"\n{datetime.datetime.now().strftime('%d.%m %H:%M')} передано в группу ЗУЭС"
        query_options = f"""INSERT INTO Dmg (DateBeg, nras, Name, addr, id_t, Txt, Class, IP_ADR, vip, chOut, incident, spd1, iptv1, sip1, ktv1, ota1, groupp, type, editedByGou, onControl, important, photo)
         VALUES ('{data['timeStart'].replace('T', ' ')}',
                '{data['idrues']}', 
                '{data['address']}', 
                '{data['address']}', 
                '{data['idtechn3']}', 
                '{problem_text}',
                '{data['klass']}', 
                '{data['IP']}', 
                '{data['Vip']}', 
                '{data['ch_out']}', 
                '{data['incident']}', 
                '{spd}', 
                '{iptv}', 
                '{sip}', 
                '{ktv}', 
                '{ota}', 
                '{data['group']}', 
                '{data['type']}',
                {data['editedByGou']},
                {data['onControl']},
                {data['important']},
                '{photo}')"""
        params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
        ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
        engine = create_engine(ENGINE_PATH_WIN_AUTH)
        with engine.connect() as con:
            con.execute(query_options)
            rs = pd.read_sql_query("""SELECT [ID], [Дата и время конца] as [dataEnd],[Дата и время начала] as [dataStart],
         [Технология 1] as [tech1], [Технология 2] as [tech2], [Технология 3] as [tech3], [ЗУЭС] as [zues], [РУЭС] as [rues],
         [количество услуг] as [uslugi_all], [ОТА Количество услуг] as [uslugi_OTA], [ШПД Количество услуг] as
         [uslugi_SPD], [IPTV Количество услуг] as [uslugi_IPTV], [КТВ Количество услуг] as [uslugi_KTV], [SIP Количество услуг]
         as [uslugi_SIP], [Причина повреждения] as [problem], [IP], [Адрес] as [address], [Класс] as [klass], [Vip],
         [вкл/выкл] as [flNot], [AW] as [ch_out], [incident], [Оборудование Год] as [device_year], [Оборудование Тип] as
         [device_type], [idrues], [id_model], [idtechn1], [idtechn2], [idtechn3], [ota], [spd], [iptv], [sip], [ktv],
         [editedByGou], [onControl], [important], [photo], [type], [groupp] as [group] FROM esm WHERE ID = SCOPE_IDENTITY()""", con)
            if data['timeEnd'] != None and data['timeEnd'] != '':
                services = {}
                if sum([int(spd), int(iptv), int(sip), int(ktv)], int(ota)) > 0:
                    if int(ota) > 0:
                        services[1] = ota
                    if int(spd) > 0:
                        services[2] = spd
                    if int(iptv) > 0:
                        services[3] = iptv
                    if int(sip) > 0:
                        services[5] = sip
                    if int(ktv) > 0:
                        services[4] = ktv
                else:
                    services[2] = spd
                for service in services:
                    con.execute(f"""INSERT INTO DmgSrv (ID_D, id_Srv, DateEnd, NumbSrv)
                        VALUES ({rs['ID'][0]}, {service}, '{data['timeEnd'].replace('T', ' ')}', {services[service]})""")
                rs = pd.read_sql_query(f"""SELECT [ID], [Дата и время конца] as [dataEnd],[Дата и время начала] as [dataStart],
                [Технология 1] as [tech1], [Технология 2] as [tech2], [Технология 3] as [tech3], [ЗУЭС] as [zues], [РУЭС] as [rues],
                [количество услуг] as [uslugi_all], [ОТА Количество услуг] as [uslugi_OTA], [ШПД Количество услуг] as
                [uslugi_SPD], [IPTV Количество услуг] as [uslugi_IPTV], [КТВ Количество услуг] as [uslugi_KTV], [SIP Количество услуг]
                as [uslugi_SIP], [Причина повреждения] as [problem], [IP], [Адрес] as [address], [Класс] as [klass], [Vip],
                [вкл/выкл] as [flNot], [AW] as [ch_out], [incident], [Оборудование Год] as [device_year], [Оборудование Тип] as
                [device_type], [idrues], [id_model], [idtechn1], [idtechn2], [idtechn3], [ota], [spd], [iptv], [sip], [ktv],
                [editedByGou], [onControl], [important], [photo], [type], [groupp] as [group] FROM esm WHERE ID = {rs['ID'][0]}""", con)
        con.close()
        rs['uslugi_all'] = rs['uslugi_all'].fillna(rs[['sip', 'spd', 'ktv', 'iptv', 'ota']].sum(axis=1))
        rs = rs.to_json(orient="table")
        parsed = json.loads(rs)
        return parsed['data'][0]
    except exc.DBAPIError as e:
        if str(e).find('was deadlocked on lock ') != -1:
            return create_fixed_incident(data)
        else:
            raise Exception(e)


def edit_fixed_incident(data):
    if data['editedByGou'] == None:
        editedByGou = 0
    else:
        editedByGou = data['editedByGou']
    if data['foto'] == None:
        photo = ''
    else:
        photo = data['foto']
    if data['ota'] == None:
        ota = 0
    else:
        ota = data['ota']
    if data['spd'] == None:
        spd = 0
    else:
        spd = data['spd']
    if data['sip'] == None:
        sip = 0
    else:
        sip = data['sip']
    if data['iptv'] == None:
        iptv = 0
    else:
        iptv = data['iptv']
    if data['ktv'] == None:
        ktv = 0
    else:
        ktv = data['ktv']
    query_options = f"""UPDATE Dmg 
     SET DateBeg = '{data['timeStart'].replace('T', ' ')}',
         nras = '{data['idrues']}', 
         Name = '{data['address']}', 
         addr = '{data['address']}', 
         id_t = '{data['idtechn3']}', 
         Txt = '{data['problem'].replace("'", '"')}',
         Class = '{data['klass']}', 
         IP_ADR = '{data['IP']}', 
         vip = '{data['Vip']}', 
         chOut = '{data['ch_out']}', 
         incident = '{data['incident']}', 
         spd1 = '{spd}', 
         iptv1 = '{iptv}', 
         sip1 = '{sip}', 
         ktv1 = '{ktv}', 
         ota1 = '{ota}', 
         groupp = '{data['group']}', 
         type = '{data['type']}',
         editedByGou = {editedByGou},
         onControl = {data['onControl']},
         important = {data['important']},
         photo = '{photo}'
     WHERE ID = {data['ID']}"""
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        rv = con.execute(f"SELECT * FROM DmgSrv WHERE ID_D = {data['ID']}").fetchall()
        if len(rv) != 0:
            con.execute(f"DELETE FROM DmgSrv WHERE ID_D = {data['ID']}")
        con.execute(query_options)
        rs = pd.read_sql_query(f"""SELECT [ID], [Дата и время конца] as [dataEnd],[Дата и время начала] as [dataStart],
     [Технология 1] as [tech1], [Технология 2] as [tech2], [Технология 3] as [tech3], [ЗУЭС] as [zues], [РУЭС] as [rues],
     [количество услуг] as [uslugi_all], [ОТА Количество услуг] as [uslugi_OTA], [ШПД Количество услуг] as
     [uslugi_SPD], [IPTV Количество услуг] as [uslugi_IPTV], [КТВ Количество услуг] as [uslugi_KTV], [SIP Количество услуг]
     as [uslugi_SIP], [Причина повреждения] as [problem], [IP], [Адрес] as [address], [Класс] as [klass], [Vip],
     [вкл/выкл] as [flNot], [AW] as [ch_out], [incident], [Оборудование Год] as [device_year], [Оборудование Тип] as
     [device_type], [idrues], [id_model], [idtechn1], [idtechn2], [idtechn3], [ota], [spd], [iptv], [sip], [ktv],
     [editedByGou], [onControl], [important], [photo], [type], [groupp] as [group] FROM esm WHERE ID = {data['ID']}""", con)
    con.close()
    rs['uslugi_all'] = rs['uslugi_all'].fillna(rs[['sip', 'spd', 'ktv', 'iptv', 'ota']].sum(axis=1))
    rs = rs.to_json(orient="table")
    parsed = json.loads(rs)
    return parsed['data'][0]


def close_fixed_incident(data):
    if data['editedByGou'] == None:
        editedByGou = 0
    else:
        editedByGou = data['editedByGou']
    services = {}
    if data['ota'] == None:
        ota = 0
    else:
        ota = data['ota']
    if data['spd'] == None:
        spd = 0
    else:
        spd = data['spd']
    if data['sip'] == None:
        sip = 0
    else:
        sip = data['sip']
    if data['iptv'] == None:
        iptv = 0
    else:
        iptv = data['iptv']
    if data['ktv'] == None:
        ktv = 0
    else:
        ktv = data['ktv']
    if data['id_model'] == None:
        id_model = sqlalchemy.sql.null()
    else:
        id_model = data['id_model']
    if data['device_year'] == None:
        device_year = sqlalchemy.sql.null()
    else:
        device_year = data['device_year']
    if data['foto'] == None:
        photo = ''
    else:
        photo = data['foto']
    if sum([int(spd), int(iptv), int(sip), int(ktv)], int(ota)) > 0:
        if int(ota) > 0:
            services[1] = ota
        if int(spd) > 0:
            services[2] = spd
        if int(iptv) > 0:
            services[3] = iptv
        if int(sip) > 0:
            services[5] = sip
        if int(ktv) > 0:
            services[4] = ktv
    else:
        services[2] = spd
    query_options = f"""UPDATE Dmg 
         SET DateBeg = '{data['timeStart'].replace('T', ' ')}',
             nras = '{data['idrues']}', 
             Name = '{data['address']}', 
             addr = '{data['address']}', 
             id_t = '{data['idtechn3']}', 
             Txt = '{data['problem'].replace("'", '"')}',
             Class = '{data['klass']}', 
             IP_ADR = '{data['IP']}', 
             vip = '{data['Vip']}', 
             chOut = '{data['ch_out']}', 
             incident = '{data['incident']}', 
             spd1 = '{spd}', 
             iptv1 = '{iptv}', 
             sip1 = '{sip}', 
             ktv1 = '{ktv}', 
             ota1 = '{ota}', 
             groupp = '{data['group']}', 
             type = '{data['type']}',
             editedByGou = {editedByGou},
             onControl = {data['onControl']},
             important = {data['important']},
             id_equ = {id_model},
             Year_Equ = {device_year},
             photo = '{photo}'
         WHERE ID = {data['ID']}"""
    params = urllib.parse.quote_plus(
        'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
        rv = con.execute(f"SELECT * FROM DmgSrv WHERE ID_D = {data['ID']}").fetchall()
        if len(rv) != 0:
            con.execute(f"DELETE FROM DmgSrv WHERE ID_D = {data['ID']}")
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
                con.execute(f"""INSERT INTO DmgSrv (ID_D, id_Srv, DateEnd, NumbSrv {dateAss['label']} {DownTime0['label']})
                    VALUES ({service['ID_D']},
                        {service['id_Srv']},
                        '{f_date}',
                        {service['NumbSrv']}
                        {dateAss['value']}
                        {DownTime0['value']})""")
        else:
            for service in services:
                con.execute(f"""INSERT INTO DmgSrv (ID_D, id_Srv, DateEnd, NumbSrv)
                    VALUES ({data['ID']}, {service}, '{data['timeEnd'].replace('T', ' ')}', {services[service]})""")
        rs = pd.read_sql_query(f"""SELECT [ID], [Дата и время конца] as [dataEnd],[Дата и время начала] as [dataStart],
         [Технология 1] as [tech1], [Технология 2] as [tech2], [Технология 3] as [tech3], [ЗУЭС] as [zues], [РУЭС] as [rues],
         [количество услуг] as [uslugi_all], [ОТА Количество услуг] as [uslugi_OTA], [ШПД Количество услуг] as
         [uslugi_SPD], [IPTV Количество услуг] as [uslugi_IPTV], [КТВ Количество услуг] as [uslugi_KTV], [SIP Количество услуг]
         as [uslugi_SIP], [Причина повреждения] as [problem], [IP], [Адрес] as [address], [Класс] as [klass], [Vip],
         [вкл/выкл] as [flNot], [AW] as [ch_out], [incident], [Оборудование Год] as [device_year], [Оборудование Тип] as
         [device_type], [idrues], [id_model], [idtechn1], [idtechn2], [idtechn3], [ota], [spd], [iptv], [sip], [ktv],
         [editedByGou], [onControl], [important], [photo], [type], [groupp] as [group] FROM esm WHERE ID = {data['ID']}""", con)
    con.close()
    rs['uslugi_all'] = rs['uslugi_all'].fillna(rs[['sip', 'spd', 'ktv', 'iptv', 'ota']].sum(axis=1))
    rs = rs.to_json(orient="table")
    parsed = json.loads(rs)
    return parsed['data'][0]


def delete_fixed_incident(data):
    query_options = "DELETE FROM Dmg WHERE ID = "+ str(data['ID'])
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
    con.close()