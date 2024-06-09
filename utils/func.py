import requests
import time
import concurrent.futures
import pandas as pd
import numpy as np
from tqdm import tqdm
from decouple import Config, RepositoryEnv


# функция для получения токена из файла .env, с вариантами получения одного токена или списка токенов
def get_tokens(single_token=True):
    # прописываем путь так как он не стандартный для decouple
    config = Config(RepositoryEnv('config/.env'))

    # получаем строку токенов из переменной
    tokens_str = config('ACCESS_TOKENS', default='').strip()

    # преобразовываем в список
    access_tokens = tokens_str.split(',')

    if single_token:
        # возвращаем один токен
        return access_tokens[0] if access_tokens else None
    else:
        # возвращаем все доступные
        return access_tokens
    
# Функция для получения участников группы с учетом пагинации
def get_all_members(group_id, token, version):
    # список для хранения
    all_members = []
    # стартовая страница с подмножеством участников
    offset = 0
    # максимальное количество записей за один запрос
    count = 1000  
    total_count = None

    while True:
        # URL и параметры запроса
        url = 'https://api.vk.com/method/groups.getMembers'
        params = {
            'access_token': token,
            'v': version,
            'group_id': group_id,
            'fields': 'id,first_name,last_name,bdate,last_seen,contacts,city',
            'count': count,
            'offset': offset
        }
        # запрос
        response = requests.get(url, params=params)
        # проверка успешности ответа
        if response.status_code == 200:
            # преобразование в json
            data = response.json()
            # проверка наличия response в ответе, если его нет надо обрабатывать как ошибку
            if 'response' in data:
                # получаем общее количество участников на первой итерации и создаем прогресс бар
                if total_count is None:
                    total_count = data['response']['count']
                    pbar = tqdm(total=total_count, desc="Загрузка участников группы")
                # получаем список участников добавляем в итоговый список и передаем в прогресс бар для отслеживания
                members = data['response']['items']
                all_members.extend(members)
                pbar.update(len(members))
                # если кол-во участников меньше чем заданное на одну итерацию, можно завершать цикл
                if len(members) < count:
                    pbar.close()
                    break  # Выходим из цикла, если больше нет данных
                offset += count
            # обработка ошибок
            else:
                print(f"Ошибка в ответе: {data}")
                pbar.close()
                break
        # обработка ошибок    
        else:
            print(f"Ошибка: {response.status_code}")
            pbar.close()
            break

    return all_members

# Функция для получения количества друзей пользователя с использованием execute
def get_friends_count(user_ids, token, version):
    #(user_ids - список пользователей, token - токен vk, version - версия api vk)
    # формируем код для выполнения запроса к vk с помощью execute. Получаем количество друзей для каждого пользователя из переданного списка
    code = 'return [' + ','.join([f'API.friends.get({{"user_id": {user_id}}}).count' for user_id in user_ids]) + '];'
    # формирование ссылки и добаление параметров для запроса
    url = f"https://api.vk.com/method/execute"
    params = {
        'code': code,
        'access_token': token,
        'v': version
    }
    # выполнение get-запроса к API
    try:
        response = requests.get(url, params=params).json()
        # обработка успешного ответа 
        if 'response' in response:
            # запрос возвращает словарь, параметр count в нем это общее кол-во друзей пользователя, zip для объединения
            return {user_id: count for user_id, count in zip(user_ids, response['response'])}
        else:
            # если в ответе нет ключа response, то параметра count тоже не будет, значит возвращаем None
            return {user_id: None for user_id in user_ids}
    # обрабатываем ошибку запроса к API    
    except requests.RequestException as e:
        # выводим ошибку в терминал 
        print(f"Ошибка при обработке запроса для пользователей {user_ids}: {e}")
        # обработка ошибки
        return {user_id: None for user_id in user_ids}
    
# функция для обработки группы пользователей
def process_users(user_ids, token, version, request_per_second=3):
    #(user_ids - список пользователей, token - токен vk, version - версия api vk, request_per_second - количество запросов в секунду(ограничение от vk))
    
    results = {}
    # максимально возможный размер группы для метода execute = 25
    batch_size = 25
    # цикл для обработки пользователей группами с заданным ранее размером, tqdm - прогресс бар
    for i in tqdm(range(0, len(user_ids), batch_size), desc="Processing users"):
        # извлекаем по 25 пользователей из списка пользователей
        batch_ids = user_ids[i:i + batch_size]
        # вызываем функцию get_friends_count для каждой группы пользователей
        batch_results = get_friends_count(batch_ids, token, version)
        # добавляем результат в словаь
        results.update(batch_results)
        # регулируем когличество запросов в секунду чтобы не получить блокировку 
        time.sleep(1 / request_per_second)
    
    return results

# функция для параллельного выполнения запросов
def process_user_chunks(user_ids, tokens, version):
    #(user_ids - список пользователей, tokens - список токенов vk, version - версия api vk)
    # получаем размеры частей, общее число пользователей делем на количество потоков, чтобы разделить их равномерно
    num_threads = len(tokens)
    chunk_size = len(user_ids) // num_threads
    # делим всех пользователей на части заданного размера
    user_chunks = [user_ids[i:i + chunk_size] for i in range(0, len(user_ids), chunk_size)]
    
    results = {}
    # инициализируем многопоточную обработку с чилом потоков num_threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # запускаем функцию process_users для каждой части пользователей в отдельном потоке, через tokens[i % num_threads] циклически назначаем токены
        future_to_chunk = {executor.submit(process_users, chunk, tokens[i % num_threads], version): chunk for i, chunk in enumerate(user_chunks)}
        # собираем задачи по мере завершения и добавляем в общий словарь
        for future in concurrent.futures.as_completed(future_to_chunk):
            result = future.result()
            results.update(result)
        
    return results    

# функция для создания базы данных и таблиц в clickhouse
def database_create(client, database_name = 'testcase_database'):
     
    try:
        # создание базы данных, если она еще не создана
        client.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
    except Exception as e:
        print(f'База данных {database_name} уже существует. Измените имя создаваемой базы данных')

    # использование созданной базы данных
    client.execute(f'USE {database_name}')

    # код sql для создания таблиц в базе данных
    try:
        client.execute('''
            CREATE TABLE IF NOT EXISTS Users
            (
                UserID Int32,
                FirstNameID Int32,
                LastNameID Int32,
                BirthDate DateTime,
                LastSeenTime DateTime,
                Contacts FixedString(255),
                CityID Int32,
                FriendsCount Nullable(Int32)
            ) ENGINE = MergeTree()
            ORDER BY UserID
        ''')
    
        client.execute('''
            CREATE TABLE IF NOT EXISTS FirstNames
            (
                FirstNameID Int32,
                FirstName FixedString(55)
            ) ENGINE = MergeTree()
            ORDER BY FirstNameID
        ''')
    
        client.execute('''
            CREATE TABLE IF NOT EXISTS LastNames
            (
                LastNameID Int32,
                LastName FixedString(55)
            ) ENGINE = MergeTree()
            ORDER BY LastNameID
        ''')
        
        client.execute('''
            CREATE TABLE IF NOT EXISTS Cities
            (
                CityID Int32,
                City FixedString(55)
            ) ENGINE = MergeTree()
            ORDER BY CityID
        ''')

        print("База данных и таблицы успешно созданы.")
    # обработка ошибок 
    except Exception as e:
        print(f'Ошибка при создании таблиц: {e}')

# функция для создания отдельных таблиц из скачанной с vk базы
def uniques(df, value):
    # сортировка значений
    df = df.sort_values(by=value)
    
    # замена пустых строк на NaN и удаление NaN
    df[value] = df[value].replace('', np.nan)
    unique_values = df[value].dropna().unique()
    
    # создание датафрейма с уникальными значениями и их id
    uniques_df = pd.DataFrame({
        'id': range(len(unique_values)),
        value: unique_values
    })
    
    # преобразуем в список кортежей
    unique_records = list(uniques_df.itertuples(index=False, name=None))
    
    return uniques_df, unique_records
 
def load_to_database(df, client):
    
    # получение уникальных значений и создание словарей для будущих таблиц
    first_names_df, first_name_records = uniques(df, 'first_name')
    last_names_df, last_name_records = uniques(df, 'last_name')
    cities_df, city_records = uniques(df, 'city.title')
    # создаем словари 
    first_name_dict = pd.Series(first_names_df.id.values, index=first_names_df.first_name).to_dict()
    last_name_dict = pd.Series(last_names_df.id.values, index=last_names_df.last_name).to_dict()
    city_dict = pd.Series(cities_df.id.values, index=cities_df['city.title']).to_dict()

    # вставляем id в датафрейм, чтобы в перспективе сделать таблицу Users
    df['FirstNameID'] = df['first_name'].map(first_name_dict).fillna(-1).astype(int)
    df['LastNameID'] = df['last_name'].map(last_name_dict).fillna(-1).astype(int)
    df['CityID'] = df['city.title'].map(city_dict).fillna(-1).astype(int)
    # обработка данных о дате рождения, приведение в соотвествие формату и обработка выбросов
    df['bdate'] = pd.to_datetime(df['bdate'], format='%d.%m.%Y', errors='coerce')
    df['bdate'] = df['bdate'].fillna(pd.Timestamp('1970-01-01'))
    df.loc[df['bdate'] < pd.Timestamp('1900-01-01'), 'bdate'] = pd.Timestamp('1970-01-01')
    # обработка данных о дате последнего визита, приведение в соотвествие формату и обработка выбросов
    df['last_seen.time'] = pd.to_datetime(df['last_seen.time'], unit='s', errors='coerce')
    df['last_seen.time'] = df['last_seen.time'].fillna(pd.Timestamp('1970-01-01'))
    df.loc[df['last_seen.time'] < pd.Timestamp('1900-01-01'), 'last_seen.time'] = pd.Timestamp('1970-01-01')
    # приведение к типу данных
    df['home_phone'] = df['home_phone'].astype(str)
    # очитка и приведение типа данных
    df['friends_count'] = df['friends_count'].fillna(0).astype('Int32')
    # выбор столбцов для таблицы Users
    users = df[['id', 'FirstNameID', 'LastNameID', 'bdate', 'last_seen.time', 'home_phone', 'CityID', 'friends_count']]
    # переименование столбцов
    users = users.rename(columns={
        'id': 'UserID',
        'home_phone': 'Contacts',
        'last_seen.time': 'LastSeenTime',
        'bdate': 'BirthDate'
    })
    # преобразование в список кортежей для вставки в таблицу clickhouse
    user_records = list(users.itertuples(index=False, name=None))

    # вставка данных в таблицы FirstNames, LastNames и Cities
    client.execute('INSERT INTO FirstNames (FirstNameID, FirstName) VALUES', first_name_records)
    print('Таблица FirstNames записана')
    client.execute('INSERT INTO LastNames (LastNameID, LastName) VALUES', last_name_records)
    print('Таблица LastNames записана')
    client.execute('INSERT INTO Cities (CityID, City) VALUES', city_records)
    print('Таблица Cities записана')

    # вставка данных в таблицу Users
    client.execute(f'INSERT INTO Users (UserID, FirstNameID, LastNameID, BirthDate, LastSeenTime, Contacts, CityID, FriendsCount) VALUES', user_records, types_check=True)
    print('Таблица Users записана')

    print("Данные успешно загружены в таблицы ClickHouse.")