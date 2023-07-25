from sqlalchemy.engine import create_engine
import pandas as pd
from server.cfg import damage_odbc
from datetime import datetime
import urllib, json, re


def exclude_incident(data):
    fixed_options = f"""UPDATE Dmg SET flNot = '{data['flNot']}' WHERE ID = {data['ID']}"""
    fixed_select = f"""SELECT [ID], [Дата и время конца] as [dataEnd],[Дата и время начала] as [dataStart],
     [Технология 1] as [tech1], [Технология 2] as [tech2], [Технология 3] as [tech3], [ЗУЭС] as [zues], [РУЭС] as [rues],
     [количество услуг] as [uslugi_all], [ОТА Количество услуг] as [uslugi_OTA], [ШПД Количество услуг] as
     [uslugi_SPD], [IPTV Количество услуг] as [uslugi_IPTV], [КТВ Количество услуг] as [uslugi_KTV], [SIP Количество услуг]
     as [uslugi_SIP], [Причина повреждения] as [problem], [IP], [Адрес] as [address], [Класс] as [klass], [Vip],
     [вкл/выкл] as [flNot], [AW] as [ch_out], [incident], [Оборудование Год] as [device_year], [Оборудование Тип] as
     [device_type], [idrues], [id_model], [idtechn1], [idtechn2], [idtechn3], [ota], [spd], [iptv], [sip], [ktv],
     [editedByGou], [onControl], [important], [photo], [type], [groupp] as [group] FROM esm WHERE ID = {data['ID']}"""
    mobile_options = f"""UPDATE DmgM SET flNot = '{data['flNot']}' WHERE ID = {data['ID']}"""
    mobile_select = f"""SELECT [editedByGou], [GSM], [ID], [LTE], [LTE_FDD], [LTE_TDD],
         [Y], [close_id], [flNot], [groupp] as [group], [idrues], [idtechn1], [idtechn2], [idtechn3], [important], 
         [incident], [onControl], [photo], 
         [type_equ_id], [type_line_id], [Время простоя с учетом ночи], [Дата и время конца] as 
         [dataEnd],[Дата и время начала] as [dataStart], [ЗУЭС] as [zues], [Класс] as [klass], [Объект] as [address],
         [РУЭС] as [rues], [Секторы] as [sectors], [Текст] as [problem], [Технология 1] as [tech1],
         [Технология 2] as [tech2], [Технология 3] as [tech3], [Примечание] as [editing],
         [Тип линии] as [line_type], [Тип оборудования] as [device_type], [№ БС] as [bs_num] FROM esm_m WHERE ID = {data['ID']}"""
    if data['group'] == 'bs':
        query_options = mobile_options
        select_options = mobile_select
    else:
        query_options = fixed_options
        select_options = fixed_select
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
        rs = pd.read_sql_query(select_options, con)
    con.close()
    if data['group'] == 'bs':
        rs = rs.to_json(orient="table")
        parsed = json.loads(rs)
        parsed['data'][0]['uslugi_all'] = 'БС ' + str(parsed['data'][0]['bs_num'])
    else:
        rs['uslugi_all'] = rs['uslugi_all'].fillna(rs[['sip', 'spd', 'ktv', 'iptv', 'ota']].sum(axis=1))
        rs = rs.to_json(orient="table")
        parsed = json.loads(rs)
    return parsed['data'][0]


def dect_to_hhmm(normal_time):
    try:
        if normal_time != None:
            hours = int(normal_time)
            minutes = round(normal_time*60) % 60
            down_time = "%d:%02d" % (hours, minutes)
            return down_time
        else:
            return None
    except Exception:
        return  normal_time


def hhmm_to_dect(decTime):
    if decTime != None and decTime != '':
        fields = decTime.split(':')
        if len(fields) == 2:
            hours = fields[0]
            minutes = fields[1]
        else:
            hours = 0
            minutes = fields[0]
        down_time0 = float(hours) + (float(minutes) / 60.0)
        return (down_time0)
    else:
        return None


def tech_list():
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        techList = pd.read_sql_query("SELECT * FROM esm_technology", con)
    con.close()
    return techList.to_json(orient="table")


def device_list():
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        techList = pd.read_sql_query("SELECT * FROM TypeEq", con)
    con.close()
    return techList.to_json(orient="table")


def fixed_services(incident_id):
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        services = pd.read_sql_query(f"SELECT * FROM DmgSrv WHERE id_d = {incident_id}", con)
    con.close()
    services['RegTime'] = services['RegTime'].apply(dect_to_hhmm)
    services['DownTime'] = services['DownTime'].apply(dect_to_hhmm)
    services['DownTime0'] = services['DownTime0'].apply(dect_to_hhmm)
    return services.to_json(orient="table")


def mobile_services(incident_id):
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        services = pd.read_sql_query(f"SELECT * FROM DmgSrvM WHERE id_d = {incident_id}", con)
    con.close()
    services['RegTime'] = services['RegTime'].apply(dect_to_hhmm)
    services['DownTime'] = services['DownTime'].apply(dect_to_hhmm)
    services['DownTime0'] = services['DownTime0'].apply(dect_to_hhmm)
    return services.to_json(orient="table")