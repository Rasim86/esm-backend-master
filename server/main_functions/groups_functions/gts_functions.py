from server.cfg import damage_odbc
from sqlalchemy.engine import create_engine
import pygsheets
import pandas as pd
import urllib
from server.cfg import home_dir

CREDENTIALS_FILE = f'{home_dir}/esm-socket-backend/server/external/creds.json'
spreadsheet_bs = '1xwXt1iQr0buhIVNb4YRKqQik5kMTJush6KFTsCb85_w'
spreadsheet_ats = '1F2HsGh1PonGE4QJzUbqnmuz0nytfvVYBLZhlqq1tR4U'
google_connect = pygsheets.authorize(service_file=CREDENTIALS_FILE)


def get_ats():
    try:
        sheet = google_connect.open_by_key(spreadsheet_ats)
        worksheet = sheet.worksheet_by_title('КТЦ АТС общая')
        value = worksheet.get_as_df(has_header=False).to_json(orient="table")
    except Exception as error:
        value = 'Ошибка при обращении к Google Sheets'
        print(error)
    return value


def get_bs_list():
    try:
        sheet = google_connect.open_by_key(spreadsheet_bs)
        worksheet = sheet.worksheet_by_title('для журнала бс')
        bs = worksheet.get_as_df(has_header=True)
        sheet = google_connect.open_by_key(spreadsheet_ats)
        worksheet = sheet.worksheet_by_title('КТЦ БС общая')
        bs_ktc = worksheet.get_as_df(has_header=True)
        bs.rename(columns={'А': '№ БС'}, inplace=True)
        value = bs.merge(bs_ktc, left_on='№ БС', right_on='№ БС', how='left')
    except Exception as error:
        value = 'Ошибка при обращении к Google Sheets'
        print(error)
    return value


def get_bs(bs_list, id_bs):
    id_bs = list(map(int, id_bs))
    bs_info = bs_list.loc[bs_list['№ БС'].isin(id_bs)]
    return bs_info.to_json(orient="table")


def mobile_line():
    query_options = "SELECT * FROM Techn_MS"
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        line = pd.read_sql_query(query_options, con)
    con.close()
    line = line.to_json(orient="table")
    return line


def mobile_device():
    query_options = "SELECT * FROM EQU_MS"
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        device = pd.read_sql_query(query_options, con)
    con.close()
    device = device.to_json(orient="table")
    return device