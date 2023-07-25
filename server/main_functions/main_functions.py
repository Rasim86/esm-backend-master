# -*- coding: utf-8 -*-
from server.cfg import damage_odbc
from sqlalchemy.engine import create_engine
from datetime import datetime
import urllib
import pandas as pd


def get_fixed_incidents(options):
    query_options = """SELECT [ID], [Дата и время конца] as [dataEnd],[Дата и время начала] as [dataStart],
     [Технология 1] as [tech1], [Технология 2] as [tech2], [Технология 3] as [tech3], [ЗУЭС] as [zues], [РУЭС] as [rues],
     [количество услуг] as [uslugi_all], [ОТА Количество услуг] as [uslugi_OTA], [ШПД Количество услуг] as
     [uslugi_SPD], [IPTV Количество услуг] as [uslugi_IPTV], [КТВ Количество услуг] as [uslugi_KTV], [SIP Количество услуг]
     as [uslugi_SIP], [Причина повреждения] as [problem], [IP], [Адрес] as [address], [Класс] as [klass], [Vip],
     [вкл/выкл] as [flNot], [AW] as [ch_out], [incident], [Оборудование Год] as [device_year], [Оборудование Тип] as
     [device_type], [idrues], [id_model], [idtechn1], [idtechn2], [idtechn3], [ota], [spd], [iptv], [sip], [ktv],
     [editedByGou], [onControl], [important], [photo], [type], [groupp] as [group], [Примечание] as [editing],
      [Длительность устранения (часы)] as [downtime], [Рег Усл час] as [regServTime], [Услуго час] as [ServTime] FROM esm"""
    if options.get('type') == 'open':
        query_options += ' WHERE [Дата и время конца] is Null'
    elif options.get('type') == 'closed':
        query_options += " WHERE [Дата и время конца] IS NOT NULL AND [editedByGou] != 1"
    elif options.get('type') == 'checked':
        interval = options.getlist('timeInterval[]')
        query_options += f" WHERE [Дата и время конца] >= '{interval[0]}' AND [Дата и время конца] <= '{interval[1]}' AND [editedByGou] = 1"
    elif options.get('type') == 'onControl':
        query_options += ' WHERE [onControl] = 1'
    if options.get('zues') != 'Все ЗУЭС':
        query_options += f" AND [ЗУЭС] = '{options.get('zues')}'"
    if options.get('rues') != 'Все РУЭС':
        query_options += f" AND [РУЭС] = '{options.get('rues')}'"
    if options.get('ecuimsGroup') is not None:
        query_options += f" AND [groupp] = '{options.get('ecuimsGroup')}'"
    if options.get('camCom') is not None:
        query_options += f" AND [type] = {str(options.get('camCom'))}"
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
                open_fixed_incidents = pd.read_sql_query(query_options, con)
    con.close()
    return open_fixed_incidents


def get_mobile_incidents(options):
    query_options = """SELECT [editedByGou], [GSM], [ID], [LTE], [LTE_FDD], [LTE_TDD], [incident], [onControl], [photo],
                 [Y], [close_id], [flNot], [groupp] as [group], [idrues], [idtechn1], [idtechn2], [idtechn3], [important], 
                 [type_equ_id], [type_line_id], [Время простоя с учетом ночи], [Дата и время конца] as 
                 [dataEnd],[Дата и время начала] as [dataStart], [ЗУЭС] as [zues], [Класс] as [klass], [Объект] as [address],
                 [РУЭС] as [rues], [Секторы] as [sectors], [Текст] as [problem], [Технология 1] as [tech1],
                 [Технология 2] as [tech2], [Технология 3] as [tech3], [Примечание] as [editing],
                 [Тип линии] as [line_type], [Тип оборудования] as [device_type], [№ БС] as [bs_num], 
                 [Время простоя] as [downtime], [Простой общ. Баз.час] as [regServTime] FROM esm_m"""
    if options.get('type') == 'open':
        query_options += ' WHERE [Дата и время конца] is Null'
    elif options.get('type') == 'closed':
        query_options += " WHERE [Дата и время конца] IS NOT NULL AND [editedByGou] != 1"
    elif options.get('type') == 'checked':
        interval = options.getlist('timeInterval[]')
        query_options += f" WHERE [Дата и время конца] >= '{interval[0]}' AND [Дата и время конца] <= '{interval[1]}' AND [editedByGou] = 1"
    elif options.get('type') == 'onControl':
        query_options += ' WHERE [onControl] = 1'
    if options.get('zues') != 'Все ЗУЭС':
        query_options += f" AND [ЗУЭС] = '{options.get('zues')}'"
    if options.get('rues') != 'Все РУЭС':
        query_options += f" AND [РУЭС] = '{options.get('rues')}'"
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
                open_mobile_incidents = pd.read_sql_query(query_options, con)
    con.close()
    return open_mobile_incidents


def get_zues_rues_list():
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
                rues_list = pd.read_sql_query("SELECT * FROM esm_nras", con)
    con.close()
    rues_list = rues_list[~rues_list["rues"].str.contains('ЗУЭС|КУЭС')]
    return rues_list