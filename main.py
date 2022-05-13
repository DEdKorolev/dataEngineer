# Скрипт для автоматизации ETL процесса, который ежедневно получает выгрузку,
# данных в формате excel, загружает ее в хранилище данных согласно структуре
# хранилища и ежедневно строит отчет.
import os
import sys

import easygui as eg
import time
import datetime
import glob
import cx_Oracle
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import Integer, Date, Float, String, create_engine


# Подключение к БД
# Окно авторизации
while True:
    print("Авторизация:")
    try:
        fieldValues = eg.multpasswordbox(msg='Введите данные',
                                         title='Авторизация',
                                         fields=['IP',
                                                'Port',
                                                'Имя БД',
                                                'Логин',
                                                'Пароль'])
        user = fieldValues[3]
        password = fieldValues[4]
        dsn = f'{fieldValues[0]}:{fieldValues[1]}/{fieldValues[2]}'

        # Коннекты к БД
        con = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        engine = create_engine(f'oracle://{user}:{password}@{dsn}')
        break # Если закконнектится

    # Если коннект неуспешен
    except cx_Oracle.DatabaseError:
        print("Неверные авторизационные данные")
        result = eg.ynbox(msg='Неверные авторизационные данные,'
                              'попробовать снова?',
                 title='Отказ в подключении к БД',
                 choices=['Да', 'Нет'])

        #Если решили попробовать ввести данные снова
        if result == True:
            continue
        # Если отказались повторно вводить данные
        sys.exit()

    # Если отказались от ввода данных изначально
    except TypeError:
        sys.exit()

print(f'\t\t\t\t\t\tОК')

# Директория с файлами и скриптами (по дефолту папка с проектом)
directory = ''

# Создание директории для входящих excel-файлов
if not os.path.isdir(f'{directory}files_in'):
            os.mkdir(f'{directory}files_in')

# Создание директории для отчетов в формате excel
if not os.path.isdir(f'{directory}reports'):
            os.mkdir(f'{directory}reports')


def open_file(sql_script):
    """Для открытия файлов со скриптами
    и последовательного считывания скриптов из них"""

    with open(f'{directory}sql_scripts\\{sql_script}.sql') as file_object:
        full_sql = file_object.read()
        sql_commands = full_sql.split('//')
        with con.cursor() as cursor:
            for sql_command in sql_commands:
                # Отлавливает ошибку при удалении несуществующей таблицы
                try:
                    cursor.execute(sql_command)
                    con.commit()
                except cx_Oracle.DatabaseError:
                    continue
        print(f'Скрипт {sql_script} отработал успешно')

# Запуск скрипта DDL
print('Запуск скрипта DDL')
eg.msgbox('Запуск скрипта DDL')
open_file('DDL')

class EventHandler(FileSystemEventHandler):
    """Для отслеживания появления новых файлов"""

    def on_created(self, event):
        """Вызывается на событие создания файла или директории"""

        print(event.event_type, event.src_path)
        # Поиск последнего файла в директории
        time.sleep(5)
        print('Поиск последнего добавленного файла:')
        file = glob.glob(f'{directory}files_in\\*.xlsx')[-1]
        print(f"\t\t\t\t\t\tОК")
        time.sleep(5)
        # Перевод excel в df
        print('Перевод excel в df:')
        data = pd.read_excel(file)
        print(f'\t\t\t\t\t\tОК')

        # Загрузка df в БД
        data.to_sql(name='src_increment',
                    con=engine,
                    if_exists='replace',
                    index=False,
                    dtype={"trans_id": Integer(),
                           "date": Date(),
                           "card": String(length=255),
                           "account": String(length=255),
                           "account_valid_to": Date(),
                           "client": String(length=255),
                           "last_name": String(length=255),
                           "first_name": String(length=255),
                           "patronymic": String(length=255),
                           "date_of_birth": Date(),
                           "passport": Integer,
                           "passport_valid_to": Date(),
                           "phone": String(length=255),
                           "oper_type": String(length=255),
                           "amount": Float(),
                           "oper_result": String(length=255),
                           "terminal": String(length=255),
                           "terminal_type": String(length=255),
                           "city": String(length=255),
                           "address": String(length=255)})
        time.sleep(5)
        # Запуск скрипта ETL
        print('Запуск скрипта ETL')
        open_file('ETL')
        time.sleep(5)
        # Запуск скрипта Report
        print('Запуск скрипта Report')
        open_file('Report')

        # Экспорт отчета в Excel
        print('Экспорт отчета в Excel')
        now = datetime.datetime.now()
        df = pd.read_sql('SELECT * FROM report', engine)
        df.to_excel(f'{directory}reports\\report_'
                    f'{now.date()}_'
                    f'{now.hour}-'
                    f'{now.minute}-'
                    f'{now.second}.xlsx',
                    index=False
                    )
        print(f'\t\t\t\t\t\tОК')

if __name__ == "__main__":

    path = f'{directory}files_in' # отслеживаемая директория
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()