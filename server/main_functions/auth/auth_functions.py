from flask import jsonify
from flask_jwt_extended import jwt_required, create_access_token, get_jwt_identity
from server.main_functions.auth.ldap_search import *
import os, urllib
from sqlalchemy.engine import create_engine
import pandas as pd
from server.cfg import damage_odbc


def login(data):
    username = data.json['username']
    password = data.json['password']
    if not username:
        return jsonify({"msg": "Missing username parameter"}), 400
    if not password:
        return jsonify({"msg": "Missing password parameter"}), 400
    user = ldap_user_check(username, password)
    if not user:
        return jsonify({'success': False, 'message': 'Bad username or password'}), 401
    access_token = create_access_token(identity=username)
    return jsonify({'success': True, 'token': access_token, 'cn': user[0], 'email': user[1]}), 200


def get_users_list():
    response_object = {'status': 'success'}
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        users_list = pd.read_sql_query("SELECT * FROM esm_users", con)
    con.close()
    active_users = users_list[users_list['isActive'] == 'Да']
    inactive_users = users_list[users_list['isActive'] == 'Нет']
    response_object['users'] = active_users.to_json(orient="table")
    response_object['inactive_users'] = inactive_users.to_json(orient="table")
    return response_object


def set_user_rights(uac_data):
    query_options = f"""UPDATE esm_users SET {uac_data.get('userRght')} = '{uac_data.get('userAccess')}' WHERE login = '{uac_data.get('usrLogin')}'"""
    params = urllib.parse.quote_plus('DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
    ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    with engine.connect() as con:
        con.execute(query_options)
    con.close()


def get_user_acl(data):
    try:
        params = urllib.parse.quote_plus(
            'DRIVER={' + damage_odbc + '};SERVER=192.168.100.147;DATABASE=Damage;UID=ecuims;PWD=monitoring_git2021')
        ENGINE_PATH_WIN_AUTH = "mssql+pyodbc:///?odbc_connect=%s" % params
        engine = create_engine(ENGINE_PATH_WIN_AUTH)
        with engine.connect() as con:
            user_info = pd.read_sql_query(f"SELECT * FROM esm_users WHERE login = '{data['user']}'", con)
        con.close()
        info_from_ldap = ldap_user_info(data['user'])
        if user_info.empty:
            with engine.connect() as con:
                con.execute(f"""INSERT INTO esm_users (login, username, email, otdel, grpp)
                    VALUES ('{data['user']}',
                    '{info_from_ldap['cn']}',
                    '{info_from_ldap['mail']}',
                    '{info_from_ldap['department']}',
                    '{info_from_ldap['title']}')""")
                user_info = pd.read_sql_query(f"SELECT * FROM esm_users WHERE login = '{data['user']}'", con)
            con.close()
        elif user_info.iloc[0]['username'] != info_from_ldap['cn'] or user_info.iloc[0]['otdel'] != info_from_ldap[
            'department'] or user_info.iloc[0]['grpp'] != info_from_ldap['title']:
            with engine.connect() as con:
                con.execute(f"""UPDATE esm_users SET
                    username = '{info_from_ldap['cn']}',
                    otdel = '{info_from_ldap['department']}',
                    grpp = '{info_from_ldap['title']}'
                    WHERE login = '{data['user']}'""")
                user_info = pd.read_sql_query(f"SELECT * FROM esm_users WHERE login = '{data['user']}'", con)
            con.close()
        return user_info.to_json(orient="table")
    except Exception as Error:
        print(Error)
