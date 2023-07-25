from flask import Flask, render_template, request
from server.main_functions.main_functions import *
from server.main_functions.auth.auth_functions import *
from server.main_functions.groups_functions.git_functions import *
from server.main_functions.groups_functions.gts_functions import *
from server.main_functions.groups_functions.gou_functions import *
from server.main_functions.groups_functions.sick_report import *
from server.main_functions.groups_functions.ecuims_kpi import *
from server.main_functions.groups_functions.operational_report_functions import *
from server.main_functions.incidents_functions.fixed_incidents import *
from server.main_functions.incidents_functions.mobile_incidents import *
from server.main_functions.incidents_functions.additional_functions import *
from server.external.messaging.tgm_sender import *
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required
import requests
import pandas as pd
import traceback

from flask_socketio import SocketIO, emit, join_room, leave_room, rooms

app = Flask(__name__,
            static_folder="./dist/static",
            template_folder="./dist")
cors = CORS(app, resources={r"/api/*": {"orgins": "*"}})
app.config['JWT_SECRET_KEY'] = 'Super_Secret_JWT_KEY'
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False
jwt = JWTManager(app)
socketio = SocketIO(app, cors_allowed_origins="*")

backend_version = '1.2.2'
ats_list = get_ats()
bs_list = get_bs_list()


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    if app.debug:
        return requests.get('http://localhost:8080/{}'.format(path)).text
    return render_template("index.html")


# Router functions
@app.route('/api/login', methods=['POST'])
def login_start():
    return login(request)


@app.route('/api/add_user', methods=['GET'])
def add_user():
    return get_user_acl(request.args)


@app.route('/api/verify-token', methods=['POST'])
@jwt_required()
def verify_token():
    return jsonify({'success': True, 'access': get_user_acl(request.get_json())}), 200


@app.route('/api/update_ats', methods=['GET'])
def update_ats():
    global ats_list
    ats_list = get_ats()
    return ats_list


@app.route('/api/update_bs', methods=['GET'])
def update_bs():
    global bs_list
    bs_list = get_bs_list()
    return str(bs_list.empty)


@app.route('/api/get_incidents', methods=['GET'])
def get_incidents():
    fixed_incidents = pd.DataFrame()
    mobile_incidents = pd.DataFrame()
    if request.args.get('fix') == 'true':
        fixed_incidents = get_fixed_incidents(request.args)
        fixed_incidents['uslugi_all'] = fixed_incidents['uslugi_all'].fillna(
            fixed_incidents[['sip', 'spd', 'ktv', 'iptv', 'ota']].sum(axis=1))
    if request.args.get('mob') == 'true':
        mobile_incidents = get_mobile_incidents(request.args)
        mobile_incidents['uslugi_all'] = 'БС ' + mobile_incidents['bs_num'].astype(str)
    incidents = pd.concat([fixed_incidents, mobile_incidents], sort=False, axis=0)
    incidents = incidents.to_json(orient="table")
    response_object = {'status': 'success', 'incidents': incidents}
    return jsonify(response_object)


@app.route('/api/get_tech_list', methods=['GET'])
def get_tech_list():
    response_object = {'status': 'success', 'tech_list': tech_list()}
    return jsonify(response_object)


@app.route('/api/get_device_list', methods=['GET'])
def get_device_list():
    response_object = {'status': 'success', 'device_list': device_list()}
    return jsonify(response_object)


@app.route('/api/get_fixed_services', methods=['GET'])
def get_fixed_services():
    response_object = {'status': 'success', 'fixed_services': fixed_services(request.args.get('id'))}
    return jsonify(response_object)


@app.route('/api/get_mobile_services', methods=['GET'])
def get_mobile_services():
    response_object = {'status': 'success', 'mobile_services': mobile_services(request.args.get('id'))}
    return jsonify(response_object)


@app.route('/api/get_mobile_device', methods=['GET'])
def get_mobile_device():
    response_object = {'status': 'success', 'mobile_device': mobile_device()}
    return jsonify(response_object)


@app.route('/api/get_mobile_line', methods=['GET'])
def get_mobile_line():
    response_object = {'status': 'success', 'mobile_line': mobile_line()}
    return jsonify(response_object)


@app.route('/api/get_zues', methods=['GET'])
def get_zues():
    zues_info = get_zues_rues_list()
    zues_info = zues_info.to_json(orient="table")
    response_object = {'status': 'success', 'zuesInfo': zues_info}
    return jsonify(response_object)


@app.route('/api/get_ats', methods=['GET'])
def get_ats_list():
    response_object = {'status': 'success', 'ats_list': ats_list}
    return jsonify(response_object)


@app.route('/api/get_bs', methods=['GET'])
def get_bs_info():
    response_object = {'status': 'success', 'bsInfo': get_bs(bs_list, request.args.getlist('bs[]'))}
    return jsonify(response_object)


@app.route('/api/get_camera_information', methods=['GET'])
def get_cam():
    camera_info = get_camera_info(request.args.get('ip'))
    camera_info = camera_info.to_json(orient="table")
    response_object = {'status': 'success', 'cameraInfo': camera_info}
    return jsonify(response_object)


@app.route('/api/get_git_services', methods=['GET'])
def get_git_services():
    services = get_git_info(request.args.get('ip'))
    services = services.to_json(orient="table")
    response_object = {'status': 'success', 'services': services}
    return jsonify(response_object)


@app.route('/api/get_users', methods=['GET'])
def get_user():
    return jsonify(get_users_list())


@app.route('/api/change_user', methods=['POST'])
def change_user():
    set_user_rights(request.get_json())
    response_object = {'status': 'success'}
    return jsonify(response_object)


@app.route('/api/get_sick_zues', methods=['GET'])
def get_sick_zues():
    return jsonify(sick_zues())


@app.route('/api/get_sick_rues', methods=['GET'])
def get_sick_rues():
    return jsonify(sick_rues(request.args.get('zues_id')))


@app.route('/api/get_sick_block')
def get_sick_block():
    return jsonify(sick_block(request.args.get('rues_id'), request.args.get('zues_id')))


@app.route('/api/get_sick_list')
def get_sick_list():
    return jsonify(sick_list(request.args))


@app.route('/api/get_address_list')
def get_address_list():
    response_object = {'status': 'success', 'incident': request.args.get('incident'),
                       'coords': get_coords(request.args.get('address')), 'id': request.args.get('id')}
    return jsonify(response_object)


@app.route('/api/get_kpi')
def get_statistic():
    post_data = request.args
    response_object = {'status': 'success'}
    response_object['statistic'] = ecuims_kpi(post_data['start'], post_data['end'], request.args.getlist('groups[]'))
    return response_object


# SocketIO Functions
@socketio.on('connect')
def client_connect():
    emit('on_connect', True)


@socketio.on('ping')
def ping_connect(*esm_version):
    esm = 0
    if len(esm_version) != 0:
        esm = esm_version[0]
    pong_info = {'rooms': rooms(), 'versionControl': esm == backend_version}
    emit('pong', pong_info)


@socketio.on('disconnect')
def client_disconnect():
    pass


@socketio.on('join')
def on_join(data):
    join_room(data['room'])
    emit('getUpdate', f"{data['username']} enter the {data['room']}", to=data['room'])


@socketio.on('leave')
def on_leave(data):
    leave_room(data['room'])
    emit('getUpdate', f"{data['username']} has left the {data['room']}", to=data['room'])


@socketio.on('hospisUpdate')
def on_join(data):
    emit_data = data['data']
    try:
        if data['data']['incidentType'] == 'new':
            if check_sick(emit_data):
                emit_data = edit_sick(emit_data)
            else:
                raise CheckSickTraceback
        elif data['data']['incidentType'] == 'edit':
            emit_data = edit_sick(emit_data)
        elif data['data']['incidentType'] == 'delete':
            delete_sick(data['data']['incidentData'])
        emit('sickUpdate', emit_data, to='hospis')
        emit('sickUpdate', {'incidentType': 'backlog', 'type': data['data']['incidentType'], 'data': 'success'})
    except Exception:
        emit('sickUpdate',
             {'incidentType': 'backlog', 'type': data['data']['incidentType'], 'data': str(traceback.format_exc())})


@socketio.on('updateIncident')
def on_join(data):
    log_data = {'email': data['data']['incidentData']['email'], 'fio': data['data']['incidentData']['user'],
                'toRues': data['data']['incidentData'].get('toRues')}
    if data['data']['incidentData']['group'] != 'bs':
        log_data['f_m'] = 0
    else:
        log_data['f_m'] = 1
    transfer_group = data['data']['incidentData'].get('transfer')
    old_group = None
    old_data = None
    room = data['data']['incidentData']['group']
    emit_data = data['data']
    try:
        if (data['data']['incidentType'] != 'new' and data['data']['incidentType'] != 'delete'
                and data['data']['incidentData'].get('timeEnd') != None
                and data['data']['incidentData'].get('timeEnd') != ''):
            data['data']['incidentType'] = 'close'
        if transfer_group != None and data['data']['incidentData']['group'] != transfer_group:
            old_group = data['data']['incidentData']['group']
            old_data = data['data']['incidentData']
            data['data']['incidentData']['group'] = transfer_group
            room = transfer_group
        if data['data']['incidentType'] == 'close':
            if data['data']['incidentData']['group'] != 'bs':
                emit_data['incidentData'] = close_fixed_incident(data['data']['incidentData'])
            else:
                emit_data['incidentData'] = edit_mobile_incident(data['data']['incidentData'])
        if data['data']['incidentType'] == 'new':
            if data['data']['incidentData']['group'] != 'bs':
                emit_data['incidentData'] = create_fixed_incident(data['data']['incidentData'])
                if emit_data['incidentData']['dataEnd'] != None:
                    emit_data['incidentType'] = 'close'
            else:
                emit_data['incidentData'] = create_mobile_incident(data['data']['incidentData'])
                if emit_data['incidentData']['dataEnd'] != None:
                    emit_data['incidentType'] = 'close'
        if data['data']['incidentType'] == 'edit':
            if data['data']['incidentData']['group'] != 'bs':
                emit_data['incidentData'] = edit_fixed_incident(data['data']['incidentData'])
                if emit_data['incidentData']['dataEnd'] != None:
                    emit_data['incidentType'] = 'close'
            else:
                emit_data['incidentData'] = edit_mobile_incident(data['data']['incidentData'])
                if emit_data['incidentData']['dataEnd'] != None:
                    emit_data['incidentType'] = 'close'
        if data['data']['incidentType'] == 'delete':
            if data['data']['incidentData']['group'] != 'bs':
                delete_fixed_incident(data['data']['incidentData'])
            else:
                delete_mobile_incident(data['data']['incidentData'])
        if data['data']['incidentType'] != 'delete':
            if data['data']['incidentData']['group'] == 'bs':
                cam_type = 0
            else:
                cam_type = data['data']['incidentData']['type']
            emit('getUpdate', {'incidentType': 'backlog', 'type': data['data']['incidentType'],
                               'group': data['data']['incidentData']['group'], 'cams': cam_type, 'data': 'success'})
        emit('getUpdate', emit_data, to=room)
        if old_group != None:
            emit('getUpdate', {'incidentType': 'delete', 'incidentData': old_data}, to=old_group)
        emit('getUpdate', emit_data, to='digest')
        emit('getUpdate', emit_data, to='operational_report')
        if room != 'gou':
            emit('getUpdate', emit_data, to='gou')
        log_data['id_d'] = emit_data['incidentData']['ID']
        add_incident_log(log_data)
        try:
            send_incident_message(emit_data, log_data)
        except Exception:
            print(traceback.format_exc())
    except Exception:
        if data['data']['incidentType'] != 'delete':
            if data['data']['incidentData']['group'] == 'bs':
                cam_type = 0
            else:
                cam_type = data['data']['incidentData']['type']
            emit('getUpdate', {'incidentType': 'backlog', 'type': data['data']['incidentType'],
                               'group': data['data']['incidentData']['group'], 'cams': cam_type,
                                'data': str(traceback.format_exc())})


@socketio.on('excludeIncident')
def on_join(data):
    room = data['group']
    if data.get('dataEnd') != None and data.get('dataEnd') != '':
        emit_data = {'incidentType': 'close', 'incidentData': exclude_incident(data)}
    else:
        emit_data = {'incidentType': 'edit', 'incidentData': exclude_incident(data)}
    emit('getUpdate', emit_data, to=room)
    if room != 'gou':
        emit('getUpdate', emit_data, to='gou')


if __name__ == "__main__":
    socketio.run(app, host='192.168.255.251', debug=True, port=8000)
