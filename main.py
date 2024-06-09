import time
import pandas as pd
import subprocess
from clickhouse_driver import Client
from utils.func import get_tokens, get_all_members, process_user_chunks, database_create, load_to_database
# основная функция для запуска
def main():
    # получение одного токена (для groups.GetMembers нужен только один токен)
    TOKEN = get_tokens(single_token=True)
    # получение остальных доступных токенов (friends.get будет очень долго работать на одном токене)
    TOKENS = get_tokens(single_token=False)
    # текущая актуальная версия API
    VERSION = '5.199'
    # группа к которой будем делать запрос
    GROUP_ID = 'vk_fishing'
    # потенциальное название для базы данных в clickhouse
    DATABASE_NAME = 'testcase_database'

    # получение всех участников группы и данных доступных через GetMembers
    all_members = get_all_members(GROUP_ID, TOKEN, VERSION)

    # преобразование полученных данных в DataFrame
    df = pd.json_normalize(all_members)
    
    # получаем список юзеров для получения количества друзей. 
    # Если is_closed=True количество друзей закрыто и запрос ничего не вернет, нет смысла запрашивать
    USER_IDS = df[df['is_closed'] == False]['id'].to_list()

    # получаем количество друзей для участников группы
    results = process_user_chunks(USER_IDS, TOKENS, VERSION)

    # сохраняем результат в датафрейм и сливаем с ранее полученным
    result_df = pd.DataFrame.from_dict(results, orient='index', columns=['friends_count'])
    result_df.reset_index(inplace=True)
    result_df.rename(columns={'index': 'id'}, inplace=True)
    df = df.merge(result_df, on='id', how='left')

    # сохранение файла 
    df.to_csv('data/data.csv', index=False)
    print("Данные сохранены в 'data.csv'")

    # создание и запуск контейнера в docker
    subprocess.run(['docker', 'run', '-d', '-p', '9000:9000', '--ulimit', 'nofile=262144:262144', '--name', 'clickhouse-server', 'yandex/clickhouse-server'])

    # даем время запустится 
    time.sleep(10)

    # инициализация клиента для запросов к clickhouse
    client = Client(host='localhost', port=9000, user='default', password='', database='default')

    # даем время запустится 
    time.sleep(10)

    # создаем базу данных в clickhouse
    database_create(client, DATABASE_NAME)

    # загружаем данные в базу
    load_to_database(df, client)

    # останавливаем контейнер 
    subprocess.run(['docker', 'stop', 'clickhouse-server'])

    print('Загрузка данных и сохранение их в датасет и базу данных выполнено!')

if __name__ == "__main__":
    main()







