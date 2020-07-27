from pymongo import MongoClient
import csv
import re
from datetime import datetime

client = MongoClient('mongodb://localhost:27017/')

def read_data(file):
    """Читает csv и добавляет в DB"""
    list_reader = []
    with open(file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for id, item in enumerate(reader):
            id_item = {'_id':id} #преобразуем в числовой id
            item.update(id_item)
            item['Цена'] = int(item['Цена'])
            str_time = f'{item.get("Дата")}.2020'#преобразование даны
            dt = datetime.strptime(str_time, "%d.%m.%Y")
            item["Дата"] = dt
            list_reader.append(item)

    data_show.insert_many(list_reader).inserted_ids
    result = list(concerts_db.data_show.find())
    print(result)

    # удалить коллекцию
    # concerts_db.drop_collection(data_show)
    return result

def find_cheapest(db, update=False):
    """Сортировка по возрастанию цен"""
    list_sort = []
    for item in db.find().sort('Цена', 1):
        print(item)
        list_sort.append(item)
        if update: #если True заменяет коллекцию на отсортированную
            concerts_db.drop_collection(data_show)
            data_show.insert_many(list_sort).inserted_ids

def find_by_name(name, db):
    """Находит билеты по исполнителю по возрастанию цен"""
    data_artists = []
    pattern = re.compile(name, re.IGNORECASE)
    for item in db.find({'Исполнитель':pattern}).sort('Цена', 1):
        print(item)
        data_artists.append(item)
    return data_artists

def find_data_show(data, db):
    """Поиск концертов по дате"""
    first_day, last_day = data.split('-')
    dt_1 = datetime.strptime(first_day, "%d.%m.%Y")
    dt_2 = datetime.strptime(last_day, "%d.%m.%Y")
    for item in db.find({'Дата': {"$gte":dt_1, '$lte':dt_2}}).sort('Дата', 1):
        print(item)

if __name__ == '__main__':
    concerts_db = client['concerts']
    data_show = concerts_db['data_show']

    # read_data('artists.csv')
    # find_cheapest(data_show, update=False)
    # find_by_name('А', data_show)
    find_data_show('01.01.2020-28.03.2020', data_show)


