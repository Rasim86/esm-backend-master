import telebot
import datetime

bot = telebot.TeleBot('1787007031:AAGLGdKZopipvl7D-nMC0O4ufMDJ-VeemiY')

alert_group = '-660517301'
ecuims_id = '-1001323822032'
channel_list = {
            "Буинский ЗУЭС": "-1001179205532",
            "Наб.Челнинский ЗУЭС": "-1001479419528",
            "Альметьевский ЗУЭС": "-1001309925195",
            "Чистопольский ЗУЭС": "-1001358253321",
            "КУЭС": "-1001123680348",
            "Управление": "",
            }


def send_incident_message(data, log_data):
    time_start = data['incidentData']['dataStart'].split('.')
    timestart = datetime.datetime.strptime(time_start[0], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y %H:%M')
    incident_header = ''
    incident_bottom = ''
    date_end = ''
    if data['incidentData']['group'] == 'bs':
        bs_insert = f"\n<b>БС №</b> {data['incidentData']['bs_num']}"
    else:
        bs_insert = ''
    if data['incidentType'] == 'new':
        incident_bottom = 'Создан: '
        if data['incidentData']['dataEnd']!= None:
            time_end = data['incidentData']['dataEnd'].split('.')
            timeend = datetime.datetime.strptime(time_end[0], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y %H:%M')
            date_end = f"<b>\nДата конца: </b>{timeend}"
        if data['incidentData']['incident'] == True:
            incident_header = f"Создан новый инцидент №{data['incidentData']['ID']}"
        else:
            incident_header = f"Создано новое повреждение №{data['incidentData']['ID']}"
    elif data['incidentType'] == 'edit':
        incident_bottom = 'Изменен: '
        if data['incidentData']['dataEnd']!= None:
            time_end = data['incidentData']['dataEnd'].split('.')
            timeend = datetime.datetime.strptime(time_end[0], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y %H:%M')
            date_end = f"\n<b>Дата конца: </b>{timeend}"
        if data['incidentData']['incident'] == True:
            incident_header = f"Изменен инцидент №{data['incidentData']['ID']}"
        else:
            incident_header = f"Изменено повреждение №{data['incidentData']['ID']}"
    elif  data['incidentType'] == 'close':
        incident_bottom = 'Закрыт: '
        if data['incidentData']['dataEnd']!= None:
            time_end = data['incidentData']['dataEnd'].split('.')
            timeend = datetime.datetime.strptime(time_end[0], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y %H:%M')
            date_end = f"\n<b>Дата конца: </b>{timeend}"
        if data['incidentData']['incident'] == True:
            incident_header = f"Закрыт инцидент №{data['incidentData']['ID']}"
        else:
            incident_header = f"Закрыто повреждение №{data['incidentData']['ID']}"
    elif data['incidentType'] == 'delete':
        incident_bottom = 'Удален: '
        if data['incidentData']['dataEnd']!= None:
            time_end = data['incidentData']['dataEnd'].split('.')
            timeend = datetime.datetime.strptime(time_end[0], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y %H:%M')
            date_end = f"\n<b>Дата конца: </b>{timeend}"
        if data['incidentData']['incident'] == True:
            incident_header = f"Удален инцидент №{data['incidentData']['ID']}"
        else:
            incident_header = f"Удалено повреждение №{data['incidentData']['ID']}"
    incident_text = '<b>' + incident_header + '</b>' \
                    + '\n<b>ЗУЭС: </b>' + data['incidentData']['zues'] \
                    + '\n<b>РУЭС: </b>' + data['incidentData']['rues'] \
                    + '\n<b>Адрес: </b>' + data['incidentData']['address'] \
                    + '\n<b>Дата начала: </b>' + timestart \
                    + date_end \
                    + '\n<b>Проблема: </b>' + data['incidentData']['problem'] \
                    + bs_insert
    if len(incident_text) > 4096:
        long_text = incident_text + '\n<b>' + incident_bottom + '</b>' + log_data['fio']
        for x in range(0, len(long_text), 4096):
            bot.send_message(chat_id=ecuims_id, text=long_text[x:x + 4096], parse_mode='HTML')
    else:
        bot.send_message(chat_id=ecuims_id, text=incident_text + '\n<b>' + incident_bottom + '</b>' + log_data['fio'], parse_mode='HTML')
    if data['incidentType'] == 'new' and data['incidentData']['zues'] != 'Управление' and log_data['toRues'] == True:
        bot.send_message(chat_id=channel_list[data['incidentData']['zues']], text=incident_text, parse_mode='HTML')
