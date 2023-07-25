from clickhouse_driver import Client
import traceback
import requests

api_key = '88beef7d-9929-4a3b-b085-fa201e0dff20'


def add_to_DB(lat: str, long: str, adr: str,): #Добавление в базу данных
    sql = 'INSERT INTO qlik.m2000adres_coor select max(id)+1 , \'' + adr + '\',' + lat + ',' + long + ' from qlik.m2000adres_coor'
    client = Client(host='192.168.114.84', password='123456', database='qlik')
    client.execute(sql)
    client.disconnect()


def geocoding(adr: str, apikey: str): #Геокодирование
    url = 'https://geocode-maps.yandex.ru/1.x?apikey='+apikey+'&geocode='+adr+'&results=1&rspn=1&ll=50.750888,55.34769&spn=7.06,2.74' #URL на API поиск по РТ ограничил
    r = requests.get(url).text
    adr_api = r.partition('<AddressLine>')[2].partition('</')[0]
    lat = r.partition('<pos>')[2].partition('</')[0].partition(' ')[2].strip()
    long = r.partition('<pos>')[2].partition('</')[0].partition(' ')[0].strip()
    if adr_api.lower().find('татарстан') != -1: # 2ая часть проверки на татарстан
        add_to_DB(lat=lat, long=long, adr=adr)
        return  long, lat
    else:
        long = '49.106414'
        lat = '55.796129'
        add_to_DB(lat=lat, long=long, adr=adr)
        return long, lat


def get_coords(address):
    f_address = address.strip().strip('\n').strip('\t')
    if f_address != 'Нет данных в m2000' and f_address != '':
        client = Client(host='192.168.114.84', password='123456', database='qlik')
        sql = f"SELECT * FROM qlik.m2000adres_coor where address= '{f_address}'"
        response = client.execute(sql)
        client.disconnect()
        if len(response) != 0:
            return [response[0][3], response[0][2]]
        else:
            try:
                return geocoding(f_address, api_key)
            except Exception:
                print(traceback.format_exc())

