import pymysql.cursors


def mysql_connect():
    return pymysql.connect(host='...',
                           user='...',
                           password='...',
                           db='db',
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)


def get_race_id_(event_name_, date_, series_id_):
    mysql_db = mysql_connect()
    sql_select = "SELECT * FROM db.a WHERE event_name = '" + event_name_ + "' AND date = '" + date_ + "' AND series = '" + series_id_ + "'"
    with mysql_db.cursor() as cursor:
        cursor.execute(sql_select)
        for line in cursor:
            race_id = str(line['race_id'])
    return race_id


def total_drivers(event_name_, date_, series_id_):
    race_id = get_race_id_(event_name_, date_, series_id_)
    mysql_db = mysql_connect()
    sql_select = "SELECT * FROM db.b WHERE event_id = '" + race_id + "'"
    with mysql_db.cursor() as cursor:
        total_drivers = []
        sql__ = sql_select + " AND session_type = 'RACE'"
        cursor.execute(sql__)
        for line in cursor:
            if line.get('driver_id') not in total_drivers:
                total_drivers.append(line.get('driver_id'))
    return total_drivers


def data_driver(event_name_, date_, series_id_):
    mysql_db = mysql_connect()
    total = total_drivers(event_name_, date_, series_id_)
    race_id = get_race_id_(event_name_, date_, series_id_)
    ar = {}
    drivers = []
    driv = {}
    for id in total:
        sql_select = "SELECT * FROM db.c WHERE driver_id = '" + str(id) + "' AND event_id = '" + str(race_id) + "'"
        with mysql_db.cursor() as cursor:
            sql__ = sql_select + " AND session_type = 'a'"
            cursor.execute(sql__)
            for line in cursor:
                pos_num = line.get('pos')
                if pos_num != None:
                    driv["driver_id"] = line.get('driver_id')
                    driv["pos"] = line.get('pos')
                    driv["race"] = int(race_id)
                    driv1 = driv.copy()
                    drivers.append(driv1)
    ar["a"] = drivers
    ar["b"] = "a"
    return ar


def pred_data(name, event_name_, date_, series_id_):
    if '_p1' in name or '_top3' in name or '_top5' in name or '_top10' in name:
        pred_data_ = data_driver(event_name_, date_, series_id_)
    return pred_data_


pred = pred_data('p1', 'event', '2017-20-05', '3')
print(pred)
