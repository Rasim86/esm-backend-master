from sqlalchemy.engine import create_engine
import pandas as pd
from datetime import datetime
import urllib, json, re


def get_camera_info(ip):
    query_options = """SELECT to_char(M_TTK.aod_rsc.get_addressf(ro.OBJ_ADR)) ADR, ro.OBJ_TELZONE  FROM
                            (SELECT rvl_res, rvl_prop_id FROM M_TTK.rm_res_prop_value WHERE rvl_value='""" + ip + """') rvl
                            JOIN (SELECT EQU_ID, EQU_OBJ  FROM M_TTK.rm_equipment) re on rvl.RVL_RES = re.EQU_ID
                            JOIN (SELECT OBJ_ID, OBJ_ADR, OBJ_TELZONE FROM M_TTK.RM_OBJECT) ro ON ro.obj_id=re.EQU_OBJ"""
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
    with engine.connect() as con:
        camera_info = pd.read_sql_query(query_options, con)
    con.close()
    return camera_info


def get_git_info(ip):
    services = pd.DataFrame()
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
    all_ip = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", ip)
    if len(all_ip) > 0:
        for ip in all_ip:
            ip = ip.strip()
            with engine.connect() as con:
                git_info = pd.read_sql_query("""SELECT to_char(sum(case when TK_TYPE in (2, 82, 42, 142, 282, 302,374,375, 462) then 1 else 0 end)) spd,
                                        to_char(sum(case when TK_TYPE= 122 then 1 else 0 end)) iptv,
                                        to_char(sum(case when TK_TYPE= 202 then 1 else 0 end)) sip,
                                        to_char(sum(case when TK_TYPE in (522,242) then 1 else 0 end)) ktv,
                                        to_char(max(M_TTK.aod_rsc.get_addressf(b.OBJ_ADR))) ADR,
                                        min(b.OBJ_TELZONE) rues
                                        FROM
                                        (SELECT tkd.tkd_tk, tk.tk_type, min(prt.prt_name),ro.OBJ_ADR,ro.OBJ_TELZONE FROM
                                       (SELECT rvl_res, rvl_prop_id FROM M_TTK.rm_res_prop_value WHERE rvl_value='""" + ip + """') rvl
                                       JOIN (SELECT EQU_ID, EQU_OBJ  FROM M_TTK.rm_equipment) re on rvl.RVL_RES = re.EQU_ID
                                       JOIN (SELECT OBJ_ID, OBJ_ADR, OBJ_TELZONE FROM M_TTK.RM_OBJECT) ro ON ro.obj_id=re.EQU_OBJ
                                       JOIN (SELECT rpr_id  FROM M_TTK.rm_res_property WHERE RPR_STRCOD = 'IP_ADRESS') rpr ON rvl.rvl_prop_id = rpr.rpr_id
                                       JOIN (SELECT un_id, un_equip FROM M_TTK.rm_equip_unit) un ON rvl.rvl_res=un.un_equip
                                       JOIN (SELECT prt_id, prt_unit,prt_name FROM M_TTK.rm_equip_port  ) prt ON prt.prt_unit=un.un_id
                                       JOIN (SELECT* from M_TTK.rm_tk_data WHERE tkd_res_class = 2) tkd ON prt.prt_id=tkd.tkd_resource
                                       LEFT JOIN (SELECT tk_id, TK_TYPE, TK_STATUS_ID from M_TTK.rm_tk WHERE TK_STATUS_ID =1 AND tk_type IN (2, 82, 42, 142, 282, 302,374,375, 462,122,522,242,202) )
                                       tk ON tkd.tkd_tk = tk.tk_id
                                       GROUP BY  tkd.tkd_tk, tk.tk_type, ro.OBJ_ADR, ro.OBJ_TELZONE)b """, con)
                con.close()
                services = pd.concat([services, git_info], sort=False, axis=0)
        services = services.dropna(how='all')
        try:
            services['total_spd']= services['spd'].astype(int).sum(axis=0)
        except Exception:
            services['total_spd'] = 0
        try:
            services['total_iptv'] = services['iptv'].astype(int).sum(axis=0)
        except Exception:
            services['total_iptv'] = 0
        try:
            services['total_ktv'] = services['ktv'].astype(int).sum(axis=0)
        except Exception:
            services['total_ktv'] = 0
        try:
            services['total_sip'] = services['sip'].astype(int).sum(axis=0)
        except Exception:
            services['total_sip'] = 0
    else:
        services['rues'] = None
        services['adr'] = None
        services['total_spd'] = 0
        services['total_iptv'] = 0
        services['total_ktv'] = 0
        services['total_sip'] = 0
    return services